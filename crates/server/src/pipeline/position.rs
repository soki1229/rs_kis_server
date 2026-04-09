use crate::monitoring::alert::AlertRouter;
use crate::pipeline::TickData;
use crate::position::{calculate_trailing_stop, evaluate_exit, ExitDecision, PositionState};
use crate::regime::RegimeReceiver;
use crate::state::MarketLiveState;
use crate::types::{FillInfo, OrderRequest, Position, Side};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::{Row, SqlitePool};
use std::collections::{HashMap, HashSet};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub fn market_close_utc(
    hour: u32,
    minute: u32,
    tz: chrono_tz::Tz,
) -> chrono::DateTime<chrono::Utc> {
    use chrono::TimeZone;
    let now_local = chrono::Utc::now().with_timezone(&tz);
    let today_close = tz
        .from_local_datetime(
            &now_local
                .date_naive()
                .and_hms_opt(hour, minute, 0)
                .expect("valid time"),
        )
        .earliest()
        .expect("valid local datetime")
        .with_timezone(&chrono::Utc);
    if today_close > chrono::Utc::now() {
        today_close
    } else {
        today_close + chrono::Duration::days(1)
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_position_task(
    mut fill_rx: mpsc::Receiver<FillInfo>,
    mut tick_pos_rx: mpsc::Receiver<TickData>,
    mut eod_rx: tokio::sync::mpsc::Receiver<()>,
    live_state_tx: watch::Sender<MarketLiveState>,
    force_order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: RegimeReceiver,
    db_pool: SqlitePool,
    alert: AlertRouter,
    summary_alert: AlertRouter,
    token: CancellationToken,
    eod_fallback_utc: chrono::DateTime<chrono::Utc>,
    market_name: String,
    pos_cfg: crate::config::PositionConfig,
    notion: Option<std::sync::Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    tunable_tx: Option<std::sync::Arc<tokio::sync::watch::Sender<crate::config::TunableConfig>>>,
    signal_cfg: crate::config::SignalConfig,
    dry_run: bool,
) {
    let mut pos_states: HashMap<String, (PositionState, u64, Option<String>)> = HashMap::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();

    // Reload open positions from DB on startup
    match sqlx::query(
        "SELECT p.symbol, p.qty, p.entry_price, p.stop_price, p.trailing_stop_price, \
         p.atr_at_entry, p.profit_target_1, p.profit_target_2, p.partial_exit_done, \
         p.regime_at_entry, p.exchange_code, d.name FROM positions p \
         LEFT JOIN daily_ohlc d ON p.symbol = d.symbol AND d.date = '0000-00-00'",
    )
    .fetch_all(&db_pool)
    .await
    {
        Ok(rows) => {
            for row in &rows {
                let symbol: String = row.get("symbol");
                let name: Option<String> = row.try_get("name").ok();
                let qty_str: String = row.get("qty");
                let qty: u64 = qty_str.parse().unwrap_or(0);
                if qty == 0 {
                    continue;
                }

                let entry_price: Decimal = row
                    .get::<String, _>("entry_price")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let stop_price: Decimal = row
                    .get::<String, _>("stop_price")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let atr_at_entry: Decimal = row
                    .get::<String, _>("atr_at_entry")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let pt1: Decimal = row
                    .get::<String, _>("profit_target_1")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let pt2: Decimal = row
                    .get::<String, _>("profit_target_2")
                    .parse()
                    .unwrap_or(Decimal::ZERO);
                let trailing_stop_price: Option<Decimal> = row
                    .try_get::<Option<String>, _>("trailing_stop_price")
                    .ok()
                    .flatten()
                    .and_then(|s| s.parse().ok());
                let partial_exit_done: bool = row.get::<i32, _>("partial_exit_done") != 0;
                let regime_str: String = row.get("regime_at_entry");
                let regime = match regime_str.as_str() {
                    "Volatile" => crate::types::MarketRegime::Volatile,
                    "Quiet" => crate::types::MarketRegime::Quiet,
                    _ => crate::types::MarketRegime::Trending,
                };
                let exchange_code: Option<String> = row.try_get("exchange_code").ok().flatten();

                let state = PositionState {
                    entry_price,
                    stop_price,
                    atr_at_entry,
                    profit_target_1: pt1,
                    profit_target_2: pt2,
                    trailing_stop_price,
                    partial_exit_done,
                    regime,
                    profit_target_1_atr: pos_cfg.profit_target_1_atr,
                    profit_target_2_atr: pos_cfg.profit_target_2_atr,
                    trailing_atr_trending: pos_cfg.trailing_atr_trending,
                    trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
                    exchange_code,
                };
                pos_states.insert(symbol, (state, qty, name));
            }
        }
        Err(e) => {
            alert.critical(format!("PositionTask: DB load failed: {e}"));
        }
    }

    update_live_state(&pos_states, &live_state_tx, Decimal::ZERO, &last_prices);

    let mut pending_exits: HashSet<String> = HashSet::new();
    let mut daily_pnl_r: Decimal = Decimal::ZERO;
    let mut consecutive_losses: u32 = 0;
    let mut eod_fallback_fired = false;

    let now_std = std::time::SystemTime::now();
    let deadline_std = std::time::SystemTime::UNIX_EPOCH
        + std::time::Duration::from_secs(eod_fallback_utc.timestamp().max(0) as u64);
    let fallback_instant = tokio::time::Instant::now()
        + deadline_std
            .duration_since(now_std)
            .unwrap_or(std::time::Duration::ZERO);

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                for (sym, (state, qty, _)) in pos_states.drain() {
                    let _ = force_order_tx.send(OrderRequest { symbol: sym.clone(), side: Side::Sell, qty, price: None, atr: None, exchange_code: state.exchange_code.clone() }).await;
                }
                return;
            }
            fill = fill_rx.recv() => {
                if let Some(f) = fill { handle_fill(f, &mut pos_states, &regime_rx, &db_pool, &live_state_tx, &alert, daily_pnl_r, &pos_cfg, &last_prices).await; }
            }
            tick = tick_pos_rx.recv() => {
                if let Some(t) = tick {
                    last_prices.insert(t.symbol.clone(), t.price);
                    handle_tick(t, &mut pos_states, &force_order_tx, &db_pool, &live_state_tx, &alert, &mut daily_pnl_r, &pos_cfg, &mut consecutive_losses, &mut pending_exits, &last_prices).await;
                }
            }
            eod_msg = eod_rx.recv() => {
                if eod_msg.is_some() {
                    handle_eod(&mut pos_states, &force_order_tx, &db_pool, &live_state_tx, &alert, &summary_alert, daily_pnl_r, consecutive_losses, &token, &market_name, notion.clone(), tunable_tx.clone(), signal_cfg.clone(), pos_cfg.clone(), dry_run, &mut pending_exits).await;
                    daily_pnl_r = Decimal::ZERO; consecutive_losses = 0; eod_fallback_fired = false;
                }
            }
            _ = tokio::time::sleep_until(fallback_instant), if !eod_fallback_fired => {
                eod_fallback_fired = true;
                handle_eod(&mut pos_states, &force_order_tx, &db_pool, &live_state_tx, &alert, &summary_alert, daily_pnl_r, consecutive_losses, &token, &market_name, notion.clone(), tunable_tx.clone(), signal_cfg.clone(), pos_cfg.clone(), dry_run, &mut pending_exits).await;
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_fill(
    fill: FillInfo,
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    regime_rx: &RegimeReceiver,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    alert: &AlertRouter,
    daily_pnl_r: Decimal,
    pos_cfg: &crate::config::PositionConfig,
    last_prices: &HashMap<String, Decimal>,
) {
    let row = sqlx::query("SELECT o.atr, d.name FROM orders o LEFT JOIN daily_ohlc d ON o.symbol = d.symbol AND d.date = '0000-00-00' WHERE o.id = ?")
        .bind(&fill.order_id).fetch_optional(db_pool).await.ok().flatten();
    let atr_str: Option<String> = row.as_ref().and_then(|r| r.try_get("atr").ok());
    let name: Option<String> = row.as_ref().and_then(|r| r.try_get("name").ok());
    let atr = match atr_str.and_then(|s| s.parse::<Decimal>().ok()) {
        Some(a) if a > Decimal::ZERO => a,
        _ => return,
    };

    if pos_states.contains_key(&fill.symbol) {
        return;
    }

    let entry = fill.filled_price;
    let stop = entry - pos_cfg.stop_atr_multiplier * atr;
    let pt1 = entry + pos_cfg.profit_target_1_atr * atr;
    let pt2 = entry + pos_cfg.profit_target_2_atr * atr;
    let regime = regime_rx.borrow().clone();

    let state = PositionState {
        entry_price: entry,
        stop_price: stop,
        atr_at_entry: atr,
        profit_target_1: pt1,
        profit_target_2: pt2,
        trailing_stop_price: None,
        partial_exit_done: false,
        regime,
        profit_target_1_atr: pos_cfg.profit_target_1_atr,
        profit_target_2_atr: pos_cfg.profit_target_2_atr,
        trailing_atr_trending: pos_cfg.trailing_atr_trending,
        trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
        exchange_code: fill.exchange_code.clone(),
    };

    let _ = sqlx::query("INSERT OR REPLACE INTO positions (id, order_id, symbol, qty, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, partial_exit_done, regime_at_entry, entered_at, updated_at, exchange_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)")
        .bind(uuid::Uuid::new_v4().to_string()).bind(&fill.order_id).bind(&fill.symbol).bind(fill.filled_qty.to_string())
        .bind(entry.to_string()).bind(stop.to_string()).bind(atr.to_string()).bind(pt1.to_string()).bind(pt2.to_string())
        .bind(format!("{:?}", state.regime)).bind(chrono::Utc::now().to_rfc3339()).bind(chrono::Utc::now().to_rfc3339()).bind(&state.exchange_code)
        .execute(db_pool).await;

    pos_states.insert(fill.symbol.clone(), (state, fill.filled_qty, name));
    let display_name = match pos_states
        .get(&fill.symbol)
        .and_then(|(_, _, n)| n.as_ref())
    {
        Some(n) => format!("{}({})", n, fill.symbol),
        None => fill.symbol.clone(),
    };
    alert.info(format!(
        "PositionTask: entered {} {}@{} stop={:.2}",
        display_name, fill.filled_qty, entry, stop
    ));
    update_live_state(pos_states, live_state_tx, daily_pnl_r, last_prices);
}

#[allow(clippy::too_many_arguments)]
async fn handle_tick(
    tick: TickData,
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    force_order_tx: &mpsc::Sender<OrderRequest>,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    alert: &AlertRouter,
    daily_pnl_r: &mut Decimal,
    _pos_cfg: &crate::config::PositionConfig,
    consecutive_losses: &mut u32,
    pending_exits: &mut HashSet<String>,
    last_prices: &HashMap<String, Decimal>,
) {
    let sym = tick.symbol.clone();
    let price = tick.price;
    if pending_exits.contains(&sym) {
        return;
    }

    let decision = if let Some((state, _, _)) = pos_states.get(&sym) {
        evaluate_exit(state, price)
    } else {
        return;
    };

    match decision {
        ExitDecision::StopLoss => {
            let Some((entry_price, qty, exchange_code, name)) = pos_states
                .get(&sym)
                .map(|(s, q, n)| (s.entry_price, *q, s.exchange_code.clone(), n.clone()))
            else {
                return;
            };
            let pnl = (price - entry_price) * Decimal::from(qty);
            *daily_pnl_r += pnl;
            if pnl < Decimal::ZERO {
                *consecutive_losses += 1;
            } else {
                *consecutive_losses = 0;
            }
            persist_session_stats(db_pool, *daily_pnl_r, *consecutive_losses).await;

            if force_order_tx
                .send(OrderRequest {
                    symbol: sym.clone(),
                    side: Side::Sell,
                    qty,
                    price: None,
                    atr: None,
                    exchange_code,
                })
                .await
                .is_ok()
            {
                pending_exits.insert(sym.clone());
                pos_states.remove(&sym);
                let _ = sqlx::query("DELETE FROM positions WHERE symbol = ?")
                    .bind(&sym)
                    .execute(db_pool)
                    .await;
                let display_name = match name {
                    Some(n) => format!("{}({})", n, sym),
                    None => sym.clone(),
                };
                let pnl_pct = if entry_price.is_zero() {
                    0.0
                } else {
                    ((price - entry_price) / entry_price * Decimal::from(100))
                        .to_f64()
                        .unwrap_or(0.0)
                };
                let msg = format!(
                    "📉 [손절청산] {} 손절가 도달로 전량 매도 (@{}, 수익률: {:.2}%)",
                    display_name, price, pnl_pct
                );
                tracing::warn!("{}", msg);
                alert.warn(msg);
            }
        }
        ExitDecision::PartialExit { pct } => {
            if let Some((state, qty, name)) = pos_states.get_mut(&sym) {
                let sell_qty = ((Decimal::from(*qty) * pct).ceil().to_u64().unwrap_or(1))
                    .max(1)
                    .min(*qty);
                let pnl = (price - state.entry_price) * Decimal::from(sell_qty);
                *daily_pnl_r += pnl;
                if force_order_tx
                    .send(OrderRequest {
                        symbol: sym.clone(),
                        side: Side::Sell,
                        qty: sell_qty,
                        price: None,
                        atr: None,
                        exchange_code: state.exchange_code.clone(),
                    })
                    .await
                    .is_ok()
                {
                    *qty -= sell_qty;
                    state.partial_exit_done = true;
                    let ts = calculate_trailing_stop(
                        price,
                        state.atr_at_entry,
                        &state.regime,
                        state.trailing_atr_trending,
                        state.trailing_atr_volatile,
                    );
                    state.trailing_stop_price = ts.map(|t| t.max(state.stop_price));
                    let _ = sqlx::query("UPDATE positions SET qty = ?, partial_exit_done = 1, trailing_stop_price = ?, updated_at = ? WHERE symbol = ?")
                        .bind(qty.to_string()).bind(state.trailing_stop_price.map(|t| t.to_string())).bind(chrono::Utc::now().to_rfc3339()).bind(&sym).execute(db_pool).await;
                    let ts_display = state
                        .trailing_stop_price
                        .map(|t| format!("{}", t))
                        .unwrap_or_else(|| "없음".to_string());
                    let pnl_pct = if state.entry_price.is_zero() {
                        0.0
                    } else {
                        ((price - state.entry_price) / state.entry_price * Decimal::from(100))
                            .to_f64()
                            .unwrap_or(0.0)
                    };
                    let display_name = match name {
                        Some(n) => format!("{}({})", n, sym),
                        None => sym.clone(),
                    };
                    let msg = format!(
                        "🌓 [부분익절] {} {}주 매도 완료 (@{}, 수익률: {:.2}%, 추적손절가: {})",
                        display_name, sell_qty, price, pnl_pct, ts_display
                    );
                    tracing::info!("{}", msg);
                    alert.info(msg);
                }
            }
        }
        ExitDecision::FullExit | ExitDecision::TrailingStop => {
            let Some((entry_price, qty, exchange_code, name)) = pos_states
                .get(&sym)
                .map(|(s, q, n)| (s.entry_price, *q, s.exchange_code.clone(), n.clone()))
            else {
                return;
            };
            let pnl = (price - entry_price) * Decimal::from(qty);
            *daily_pnl_r += pnl;
            if pnl < Decimal::ZERO {
                *consecutive_losses += 1;
            } else {
                *consecutive_losses = 0;
            }
            if force_order_tx
                .send(OrderRequest {
                    symbol: sym.clone(),
                    side: Side::Sell,
                    qty,
                    price: None,
                    atr: None,
                    exchange_code,
                })
                .await
                .is_ok()
            {
                pending_exits.insert(sym.clone());
                pos_states.remove(&sym);
                let _ = sqlx::query("DELETE FROM positions WHERE symbol = ?")
                    .bind(&sym)
                    .execute(db_pool)
                    .await;
                persist_session_stats(db_pool, *daily_pnl_r, *consecutive_losses).await;
                let pnl_pct = if entry_price.is_zero() {
                    0.0
                } else {
                    ((price - entry_price) / entry_price * Decimal::from(100))
                        .to_f64()
                        .unwrap_or(0.0)
                };
                let display_name = match name {
                    Some(n) => format!("{}({})", n, sym),
                    None => sym.clone(),
                };
                let label = match decision {
                    ExitDecision::FullExit => "🌕 [전량익절]",
                    _ => "📈 [추적익절]",
                };
                let msg = format!(
                    "{} {} 전량 매도 완료 (@{}, 수익률: {:.2}%)",
                    label, display_name, price, pnl_pct
                );
                tracing::info!("{}", msg);
                alert.info(msg);
            }
        }
        ExitDecision::Hold => {
            if let Some((state, _, _)) = pos_states.get_mut(&sym) {
                if state.partial_exit_done {
                    if let Some(new_ts) = calculate_trailing_stop(
                        price,
                        state.atr_at_entry,
                        &state.regime,
                        state.trailing_atr_trending,
                        state.trailing_atr_volatile,
                    ) {
                        let candidate = new_ts.max(state.stop_price);
                        if state.trailing_stop_price.is_none_or(|old| candidate > old) {
                            state.trailing_stop_price = Some(candidate);
                        }
                    }
                }
            }
        }
    }
    update_live_state(pos_states, live_state_tx, *daily_pnl_r, last_prices);
}

async fn persist_session_stats(db_pool: &SqlitePool, daily_pnl: Decimal, consecutive_losses: u32) {
    let date = chrono::Local::now().format("%Y-%m-%d").to_string();
    let _ = sqlx::query("INSERT INTO session_stats (date, pnl, consecutive_losses, trade_count, max_drawdown, profile_active, updated_at) VALUES (?, ?, ?, 0, '0', 'default', ?) ON CONFLICT(date) DO UPDATE SET pnl = excluded.pnl, consecutive_losses = excluded.consecutive_losses, updated_at = excluded.updated_at")
        .bind(&date).bind(daily_pnl.to_string()).bind(consecutive_losses as i64).bind(chrono::Utc::now().to_rfc3339()).execute(db_pool).await;
}

#[allow(clippy::too_many_arguments)]
async fn handle_eod(
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    force_order_tx: &mpsc::Sender<OrderRequest>,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    alert: &AlertRouter,
    summary_alert: &AlertRouter,
    daily_pnl_r: Decimal,
    consecutive_losses: u32,
    _token: &CancellationToken,
    market_name: &str,
    notion: Option<std::sync::Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    tunable_tx: Option<std::sync::Arc<tokio::sync::watch::Sender<crate::config::TunableConfig>>>,
    signal_cfg: crate::config::SignalConfig,
    position_cfg: crate::config::PositionConfig,
    dry_run: bool,
    pending_exits: &mut HashSet<String>,
) {
    for (sym, (state, qty, _)) in pos_states.iter() {
        if pending_exits.contains(sym) {
            continue;
        }
        let _ = force_order_tx
            .send(OrderRequest {
                symbol: sym.clone(),
                side: Side::Sell,
                qty: *qty,
                price: None,
                atr: None,
                exchange_code: state.exchange_code.clone(),
            })
            .await;
        let _ = sqlx::query("DELETE FROM positions WHERE symbol = ?")
            .bind(sym)
            .execute(db_pool)
            .await;
    }
    pos_states.clear();
    update_live_state(pos_states, live_state_tx, Decimal::ZERO, &HashMap::new());
    persist_session_stats(db_pool, daily_pnl_r, consecutive_losses).await;

    let db_for_review = db_pool.clone();
    let alert_for_review = alert.clone();
    let summary_for_review = summary_alert.clone();
    let api_key = std::env::var("ANTHROPIC_API_KEY").ok();
    let model = std::env::var("LLM_REVIEW_MODEL")
        .unwrap_or_else(|_| "claude-3-5-haiku-20241022".to_string());
    let market_for_review = market_name.to_string();
    tokio::spawn(async move {
        if let Ok(report) = crate::pipeline::review::run_daily_review(
            &market_for_review,
            &db_for_review,
            &alert_for_review,
            &summary_for_review,
            api_key.as_deref(),
            &model,
            dry_run,
        )
        .await
        {
            let _ = crate::pipeline::review::run_post_review(
                &market_for_review,
                &report,
                &db_for_review,
                &alert_for_review,
                &summary_for_review,
                notion.as_ref(),
                tunable_tx.as_ref(),
                &signal_cfg,
                &position_cfg,
                dry_run,
            )
            .await;
        }
    });
}

fn update_live_state(
    pos_states: &HashMap<String, (PositionState, u64, Option<String>)>,
    live_state_tx: &watch::Sender<MarketLiveState>,
    daily_pnl_r: Decimal,
    last_prices: &HashMap<String, Decimal>,
) {
    let positions = pos_states
        .iter()
        .map(|(sym, (state, qty, name))| {
            let current = last_prices.get(sym).copied().unwrap_or(state.entry_price);
            let unrealized = (current - state.entry_price) * Decimal::from(*qty);
            let pnl_pct = if state.entry_price.is_zero() {
                0.0
            } else {
                ((current - state.entry_price) / state.entry_price * Decimal::from(100))
                    .to_f64()
                    .unwrap_or(0.0)
            };
            Position {
                symbol: sym.clone(),
                name: name.clone(),
                qty: *qty as i64,
                avg_price: state.entry_price,
                current_price: current,
                unrealized_pnl: unrealized,
                pnl_pct,
                stop_price: state.stop_price,
                trailing_stop: state.trailing_stop_price,
                profit_target_1: state.profit_target_1,
                profit_target_2: state.profit_target_2,
                regime: format!("{:?}", state.regime),
            }
        })
        .collect();
    let _ = live_state_tx.send(MarketLiveState {
        positions,
        daily_pnl_r: daily_pnl_r.to_f64().unwrap_or(0.0),
        regime: String::new(),
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::alert::AlertRouter;
    use crate::pipeline::TickData;
    use crate::regime::regime_channel;
    use crate::state::MarketLiveState;
    use crate::types::MarketRegime;
    use crate::types::{FillInfo, OrderRequest};
    use rust_decimal_macros::dec;
    use tokio::sync::{mpsc, watch};
    use tokio_util::sync::CancellationToken;

    async fn make_pool() -> sqlx::SqlitePool {
        let pool = crate::db::connect(":memory:").await.unwrap();
        sqlx::query("DROP TABLE IF EXISTS orders")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS daily_ohlc")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DROP TABLE IF EXISTS positions")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("CREATE TABLE orders (id TEXT PRIMARY KEY, symbol TEXT, side TEXT, state TEXT, order_type TEXT, qty TEXT, atr TEXT, submitted_at TEXT, updated_at TEXT)")
            .execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE daily_ohlc (symbol TEXT, name TEXT, date TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, PRIMARY KEY(symbol, date))")
            .execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE positions (id TEXT PRIMARY KEY, order_id TEXT, symbol TEXT, qty TEXT, entry_price TEXT, stop_price TEXT, atr_at_entry TEXT, profit_target_1 TEXT, profit_target_2 TEXT, partial_exit_done INTEGER, regime_at_entry TEXT, entered_at TEXT, updated_at TEXT, exchange_code TEXT)")
            .execute(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn fill_creates_position_in_db() {
        let pool = make_pool().await;
        sqlx::query("INSERT INTO orders (id, symbol, side, state, order_type, qty, atr, submitted_at, updated_at) VALUES ('ord-1', 'NVDA', 'buy', 'FullyFilled', 'marketable_limit', '1', '2.5', '2026-03-23T00:00:00Z', '2026-03-23T00:00:00Z')").execute(&pool).await.unwrap();
        sqlx::query("INSERT INTO daily_ohlc (symbol, name, date, open, high, low, close, volume) VALUES ('NVDA', 'NVIDIA', '0000-00-00', '0', '0', '0', '0', '0')").execute(&pool).await.unwrap();
        let (fill_tx, fill_rx) = mpsc::channel(8);
        let (_tick_tx, tick_pos_rx) = mpsc::channel::<TickData>(8);
        let (_eod_tx, eod_rx) = tokio::sync::mpsc::channel::<()>(4);
        let (live_state_tx, live_state_rx) = watch::channel(MarketLiveState::default());
        let (force_order_tx, _force_order_rx) = mpsc::channel::<OrderRequest>(8);
        let (_, regime_rx) = regime_channel(MarketRegime::Trending);
        let token = CancellationToken::new();
        let pool_clone = pool.clone();
        tokio::spawn(async move {
            run_position_task(
                fill_rx,
                tick_pos_rx,
                eod_rx,
                live_state_tx,
                force_order_tx,
                regime_rx,
                pool_clone,
                AlertRouter::new(16),
                AlertRouter::new(16),
                token,
                chrono::Utc::now() + chrono::Duration::hours(8),
                "TEST".to_string(),
                crate::config::PositionConfig::default(),
                None,
                None,
                crate::config::SignalConfig::default(),
                false,
            )
            .await;
        });
        fill_tx
            .send(FillInfo {
                order_id: "ord-1".into(),
                symbol: "NVDA".into(),
                filled_qty: 1,
                filled_price: dec!(130.00),
                exchange_code: None,
            })
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM positions WHERE symbol = 'NVDA'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(live_state_rx.borrow().positions.len(), 1);
    }
}
