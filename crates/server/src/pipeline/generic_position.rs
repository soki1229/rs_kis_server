//! Generic position task using MarketAdapter trait.
//!
//! This module provides a unified position management pipeline that works
//! with any market through the MarketAdapter abstraction.

use crate::config::PositionConfig;
use crate::market::MarketAdapter;
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
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Generic position task that works with any MarketAdapter.
#[allow(clippy::too_many_arguments)]
pub async fn run_generic_position_task<M: MarketAdapter>(
    adapter: Arc<M>,
    fill_rx: mpsc::Receiver<FillInfo>,
    tick_pos_rx: mpsc::Receiver<TickData>,
    eod_rx: mpsc::Receiver<()>,
    live_state_tx: watch::Sender<MarketLiveState>,
    force_order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: RegimeReceiver,
    db_pool: SqlitePool,
    alert: AlertRouter,
    summary_alert: AlertRouter,
    token: CancellationToken,
    eod_fallback: chrono::DateTime<chrono::Utc>,
    pos_cfg: PositionConfig,
    notion: Option<Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    tunable_tx: Option<watch::Sender<crate::config::TunableConfig>>,
    signal_cfg: crate::config::SignalConfig,
    dry_run: bool,
) {
    let market_id = adapter.market_id();
    let market_name = adapter.name();

    tracing::info!(
        market_id = ?market_id,
        market = %market_name,
        task = "position",
        "starting generic position task"
    );

    let mut fill_rx = fill_rx;
    let mut tick_pos_rx = tick_pos_rx;
    let mut eod_rx = eod_rx;

    let mut pos_states: HashMap<String, (PositionState, u64, Option<String>)> = HashMap::new();
    let mut daily_pnl_r = Decimal::ZERO;
    let mut consecutive_losses: u32 = 0;
    let mut pending_exits: HashSet<String> = HashSet::new();
    let mut last_prices: HashMap<String, Decimal> = HashMap::new();
    let mut eod_fallback_fired = false;
    let fallback_instant = tokio::time::Instant::now()
        + std::time::Duration::from_secs(
            (eod_fallback - chrono::Utc::now()).num_seconds().max(0) as u64
        );

    // Recover positions from DB
    if let Ok(rows) = sqlx::query("SELECT symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, trailing_stop_price, partial_exit_done, regime_at_entry, qty, exchange_code FROM positions")
        .fetch_all(&db_pool)
        .await
    {
        for row in rows {
            let symbol: String = row.get("symbol");
            let entry_price: String = row.get("entry_price");
            let stop_price: String = row.get("stop_price");
            let atr_at_entry: String = row.get("atr_at_entry");
            let pt1: String = row.get("profit_target_1");
            let pt2: String = row.get("profit_target_2");
            let ts: Option<String> = row.try_get("trailing_stop_price").ok().flatten();
            let partial: i32 = row.get("partial_exit_done");
            let regime_str: String = row.get("regime_at_entry");
            let qty: String = row.get("qty");
            let exchange_code: Option<String> = row.try_get("exchange_code").ok().flatten();

            let regime = match regime_str.as_str() {
                "Volatile" => crate::types::MarketRegime::Volatile,
                "Quiet" => crate::types::MarketRegime::Quiet,
                _ => crate::types::MarketRegime::Trending,
            };

            let state = PositionState {
                entry_price: entry_price.parse().unwrap_or(Decimal::ZERO),
                stop_price: stop_price.parse().unwrap_or(Decimal::ZERO),
                atr_at_entry: atr_at_entry.parse().unwrap_or(Decimal::ONE),
                profit_target_1: pt1.parse().unwrap_or(Decimal::ZERO),
                profit_target_2: pt2.parse().unwrap_or(Decimal::ZERO),
                trailing_stop_price: ts.and_then(|s| s.parse().ok()),
                partial_exit_done: partial != 0,
                regime,
                profit_target_1_atr: pos_cfg.profit_target_1_atr,
                profit_target_2_atr: pos_cfg.profit_target_2_atr,
                trailing_atr_trending: pos_cfg.trailing_atr_trending,
                trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
                exchange_code,
            };
            pos_states.insert(symbol, (state, qty.parse().unwrap_or(0), None));
        }
        if !pos_states.is_empty() {
            tracing::info!(market = %market_name, task = "position", count = pos_states.len(), "recovered positions from DB");
        }
    }

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                let pos_count = pos_states.len();
                for (sym, (state, qty, _)) in pos_states.drain() {
                    tracing::info!(market = %market_name, task = "position", symbol = %sym, qty, "closing position on shutdown");
                    let _ = force_order_tx.send(OrderRequest {
                        symbol: sym,
                        side: Side::Sell,
                        qty,
                        price: None,
                        atr: None,
                        exchange_code: state.exchange_code,
                        strength: None,
                    }).await;
                }
                tracing::info!(market = %market_name, task = "position", closed_positions = pos_count, "shutdown complete");
                return;
            }
            fill = fill_rx.recv() => {
                if let Some(f) = fill {
                    handle_fill(
                        adapter.as_ref(),
                        f,
                        &mut pos_states,
                        &regime_rx,
                        &db_pool,
                        &live_state_tx,
                        &alert,
                        daily_pnl_r,
                        &pos_cfg,
                        &last_prices,
                    ).await;
                }
            }
            tick = tick_pos_rx.recv() => {
                if let Some(t) = tick {
                    last_prices.insert(t.symbol.clone(), t.price);
                    handle_tick(
                        t,
                        &mut pos_states,
                        &force_order_tx,
                        &db_pool,
                        &live_state_tx,
                        &alert,
                        &mut daily_pnl_r,
                        &pos_cfg,
                        &mut consecutive_losses,
                        &mut pending_exits,
                        &last_prices,
                    ).await;
                }
            }
            eod_msg = eod_rx.recv() => {
                if eod_msg.is_some() {
                    handle_eod(
                        &mut pos_states,
                        &force_order_tx,
                        &db_pool,
                        &live_state_tx,
                        &alert,
                        &summary_alert,
                        daily_pnl_r,
                        consecutive_losses,
                        &token,
                        market_name,
                        notion.clone(),
                        tunable_tx.clone(),
                        signal_cfg.clone(),
                        pos_cfg.clone(),
                        dry_run,
                        &mut pending_exits,
                    ).await;
                    daily_pnl_r = Decimal::ZERO;
                    consecutive_losses = 0;
                    eod_fallback_fired = false;
                }
            }
            _ = tokio::time::sleep_until(fallback_instant), if !eod_fallback_fired => {
                eod_fallback_fired = true;
                handle_eod(
                    &mut pos_states,
                    &force_order_tx,
                    &db_pool,
                    &live_state_tx,
                    &alert,
                    &summary_alert,
                    daily_pnl_r,
                    consecutive_losses,
                    &token,
                    market_name,
                    notion.clone(),
                    tunable_tx.clone(),
                    signal_cfg.clone(),
                    pos_cfg.clone(),
                    dry_run,
                    &mut pending_exits,
                ).await;
            }
        }
    }
}

/// Handle a fill event, creating a new position.
#[allow(clippy::too_many_arguments)]
async fn handle_fill<M: MarketAdapter>(
    adapter: &M,
    fill: FillInfo,
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    regime_rx: &RegimeReceiver,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    alert: &AlertRouter,
    daily_pnl_r: Decimal,
    pos_cfg: &PositionConfig,
    last_prices: &HashMap<String, Decimal>,
) {
    let market_name = adapter.name();

    let row = sqlx::query(
        "SELECT o.atr, d.name FROM orders o LEFT JOIN daily_ohlc d ON o.symbol = d.symbol AND d.date = '0000-00-00' WHERE o.id = ?"
    )
    .bind(&fill.order_id)
    .fetch_optional(db_pool)
    .await
    .ok()
    .flatten();

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

    // Calculate profit targets with FX spread compensation from adapter
    let fx_compensation = entry * adapter.fx_spread_pct();
    let pt1 = entry + pos_cfg.profit_target_1_atr * atr + fx_compensation;
    let pt2 = entry + pos_cfg.profit_target_2_atr * atr + fx_compensation;

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

    let _ = sqlx::query(
        "INSERT OR REPLACE INTO positions (id, order_id, symbol, qty, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, partial_exit_done, regime_at_entry, entered_at, updated_at, exchange_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?, ?)"
    )
    .bind(uuid::Uuid::new_v4().to_string())
    .bind(&fill.order_id)
    .bind(&fill.symbol)
    .bind(fill.filled_qty.to_string())
    .bind(entry.to_string())
    .bind(stop.to_string())
    .bind(atr.to_string())
    .bind(pt1.to_string())
    .bind(pt2.to_string())
    .bind(format!("{:?}", state.regime))
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&state.exchange_code)
    .execute(db_pool)
    .await;

    pos_states.insert(fill.symbol.clone(), (state, fill.filled_qty, name.clone()));

    let display_name = match &name {
        Some(n) => format!("{}({})", n, fill.symbol),
        None => fill.symbol.clone(),
    };

    if fx_compensation > Decimal::ZERO {
        alert.info(format!(
            "GenericPositionTask[{}]: entered {} {}@{} stop={:.2} (FX adjusted: pt1={:.2}, pt2={:.2})",
            market_name, display_name, fill.filled_qty, entry, stop, pt1, pt2
        ));
    } else {
        alert.info(format!(
            "GenericPositionTask[{}]: entered {} {}@{} stop={:.2}",
            market_name, display_name, fill.filled_qty, entry, stop
        ));
    }

    update_live_state(pos_states, live_state_tx, daily_pnl_r, last_prices);
}

/// Handle a tick event, evaluating exits.
#[allow(clippy::too_many_arguments)]
async fn handle_tick(
    tick: TickData,
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    force_order_tx: &mpsc::Sender<OrderRequest>,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    alert: &AlertRouter,
    daily_pnl_r: &mut Decimal,
    _pos_cfg: &PositionConfig,
    consecutive_losses: &mut u32,
    pending_exits: &mut HashSet<String>,
    last_prices: &HashMap<String, Decimal>,
) {
    let sym = tick.symbol.clone();
    let price = tick.price;

    if pending_exits.contains(&sym) {
        return;
    }

    // Get immutable snapshot first to avoid mutable borrow issues
    let (
        decision,
        entry_price,
        qty_snapshot,
        exchange_code,
        name_display,
        atr,
        regime,
        trailing_cfg,
    ) = {
        let (state, qty, name) = match pos_states.get(&sym) {
            Some(v) => v,
            None => return,
        };
        let decision = evaluate_exit(state, price);
        let display = match name {
            Some(n) => format!("{}({})", n, sym),
            None => sym.clone(),
        };
        (
            decision,
            state.entry_price,
            *qty,
            state.exchange_code.clone(),
            display,
            state.atr_at_entry,
            state.regime.clone(),
            (
                state.trailing_atr_trending,
                state.trailing_atr_volatile,
                state.stop_price,
                state.partial_exit_done,
            ),
        )
    };

    match decision {
        ExitDecision::StopLoss | ExitDecision::TrailingStop => {
            let pnl = (price - entry_price) * Decimal::from(qty_snapshot);
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
                    qty: qty_snapshot,
                    price: None,
                    atr: None,
                    exchange_code,
                    strength: None,
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

                let label = if matches!(decision, ExitDecision::StopLoss) {
                    "🔴 [손절]"
                } else {
                    "🟡 [추적손절]"
                };
                alert.info(format!(
                    "{} {} @{:.2} pnl={:.2}",
                    label, name_display, price, pnl
                ));
            }
        }
        ExitDecision::PartialExit { pct } => {
            let (trailing_atr_trending, trailing_atr_volatile, stop_price, partial_exit_done) =
                trailing_cfg;
            if !partial_exit_done {
                let sell_qty = (Decimal::from(qty_snapshot) * pct)
                    .round()
                    .to_u64()
                    .unwrap_or(1)
                    .min(qty_snapshot);
                let pnl = (price - entry_price) * Decimal::from(sell_qty);
                *daily_pnl_r += pnl;

                if force_order_tx
                    .send(OrderRequest {
                        symbol: sym.clone(),
                        side: Side::Sell,
                        qty: sell_qty,
                        price: None,
                        atr: None,
                        exchange_code,
                        strength: None,
                    })
                    .await
                    .is_ok()
                {
                    // Update state
                    if let Some((state, qty, _)) = pos_states.get_mut(&sym) {
                        *qty -= sell_qty;
                        state.partial_exit_done = true;
                        let ts = calculate_trailing_stop(
                            price,
                            atr,
                            &regime,
                            trailing_atr_trending,
                            trailing_atr_volatile,
                        );
                        state.trailing_stop_price = ts.map(|t| t.max(stop_price));

                        let _ = sqlx::query(
                            "UPDATE positions SET qty = ?, partial_exit_done = 1, trailing_stop_price = ?, updated_at = ? WHERE symbol = ?"
                        )
                        .bind(qty.to_string())
                        .bind(state.trailing_stop_price.map(|t| t.to_string()))
                        .bind(chrono::Utc::now().to_rfc3339())
                        .bind(&sym)
                        .execute(db_pool)
                        .await;
                    }

                    alert.info(format!(
                        "🟢 [부분익절] {} {} @{:.2} pnl={:.2}",
                        name_display, sell_qty, price, pnl
                    ));
                }
            }
        }
        ExitDecision::FullExit => {
            let pnl = (price - entry_price) * Decimal::from(qty_snapshot);
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
                    qty: qty_snapshot,
                    price: None,
                    atr: None,
                    exchange_code,
                    strength: None,
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

                alert.info(format!(
                    "🌕 [전량익절] {} @{:.2} pnl={:.2}",
                    name_display, price, pnl
                ));
            }
        }
        ExitDecision::Hold => {
            let (trailing_atr_trending, trailing_atr_volatile, _, partial_exit_done) = trailing_cfg;
            if partial_exit_done {
                if let Some(new_ts) = calculate_trailing_stop(
                    price,
                    atr,
                    &regime,
                    trailing_atr_trending,
                    trailing_atr_volatile,
                ) {
                    if let Some((state, _, _)) = pos_states.get_mut(&sym) {
                        if let Some(current_ts) = state.trailing_stop_price {
                            if new_ts > current_ts {
                                state.trailing_stop_price = Some(new_ts);
                                let _ = sqlx::query(
                                    "UPDATE positions SET trailing_stop_price = ?, updated_at = ? WHERE symbol = ?"
                                )
                                .bind(new_ts.to_string())
                                .bind(chrono::Utc::now().to_rfc3339())
                                .bind(&sym)
                                .execute(db_pool)
                                .await;
                            }
                        }
                    }
                }
            }
        }
    }

    update_live_state(pos_states, live_state_tx, *daily_pnl_r, last_prices);
}

/// Handle end-of-day processing.
#[allow(clippy::too_many_arguments)]
async fn handle_eod(
    pos_states: &mut HashMap<String, (PositionState, u64, Option<String>)>,
    force_order_tx: &mpsc::Sender<OrderRequest>,
    db_pool: &SqlitePool,
    live_state_tx: &watch::Sender<MarketLiveState>,
    _alert: &AlertRouter,
    _summary_alert: &AlertRouter,
    daily_pnl_r: Decimal,
    consecutive_losses: u32,
    _token: &CancellationToken,
    _market_name: &str,
    _notion: Option<Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    _tunable_tx: Option<watch::Sender<crate::config::TunableConfig>>,
    _signal_cfg: crate::config::SignalConfig,
    _pos_cfg: PositionConfig,
    _dry_run: bool,
    pending_exits: &mut HashSet<String>,
) {
    // Close all positions at EOD
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
                strength: None,
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
}

fn update_live_state(
    pos_states: &HashMap<String, (PositionState, u64, Option<String>)>,
    live_state_tx: &watch::Sender<MarketLiveState>,
    daily_pnl_r: Decimal,
    last_prices: &HashMap<String, Decimal>,
) {
    let positions: Vec<Position> = pos_states
        .iter()
        .map(|(sym, (state, qty, name))| {
            let current_price = last_prices.get(sym).copied().unwrap_or(state.entry_price);
            let unrealized = (current_price - state.entry_price) * Decimal::from(*qty);
            let pnl_pct = if state.entry_price.is_zero() {
                0.0
            } else {
                ((current_price - state.entry_price) / state.entry_price * Decimal::from(100))
                    .to_f64()
                    .unwrap_or(0.0)
            };
            Position {
                symbol: sym.clone(),
                name: name.clone(),
                qty: *qty as i64,
                avg_price: state.entry_price,
                current_price,
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

async fn persist_session_stats(
    db_pool: &SqlitePool,
    daily_pnl_r: Decimal,
    consecutive_losses: u32,
) {
    let _ = sqlx::query(
        "INSERT OR REPLACE INTO session_stats (id, daily_pnl_r, consecutive_losses, updated_at) VALUES (1, ?, ?, ?)"
    )
    .bind(daily_pnl_r.to_string())
    .bind(consecutive_losses as i32)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(db_pool)
    .await;
}
