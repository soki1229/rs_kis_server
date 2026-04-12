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
pub async fn run_generic_position_task(
    adapter: Arc<dyn MarketAdapter>,
    fill_rx: mpsc::Receiver<FillInfo>,
    tick_pos_rx: mpsc::Receiver<TickData>,
    eod_rx: mpsc::Receiver<()>,
    live_state_tx: watch::Sender<MarketLiveState>,
    force_order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: RegimeReceiver,
    db_pool: SqlitePool,
    _alert: AlertRouter,
    _summary_alert: AlertRouter,
    token: CancellationToken,
    eod_fallback: chrono::DateTime<chrono::Utc>,
    pos_cfg: PositionConfig,
    _notion: Option<Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    _tunable_tx: Option<watch::Sender<crate::config::TunableConfig>>,
    _signal_cfg: crate::config::SignalConfig,
    _dry_run: bool,
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

    let mut pos_states: HashMap<String, (PositionState, u64)> = HashMap::new();
    let daily_pnl_r = 0.0;
    let _pending_exits: HashSet<String> = HashSet::new();
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
            pos_states.insert(symbol, (state, qty.parse().unwrap_or(0)));
        }
    }

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep_until(fallback_instant), if !eod_fallback_fired => {
                tracing::warn!(market = %market_name, "EOD fallback fired — forcing exit for all positions");
                eod_fallback_fired = true;
                for (symbol, (state, qty)) in &pos_states {
                    let _ = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None,
                    }).await;
                }
            }
            Some(_) = eod_rx.recv() => {
                tracing::info!(market = %market_name, "EOD trigger received — closing all positions");
                for (symbol, (state, qty)) in &pos_states {
                    let _ = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None,
                    }).await;
                }
            }
            Some(fill) = fill_rx.recv() => {
                if !pos_states.contains_key(&fill.symbol) {
                    let regime = regime_rx.borrow().clone();
                    let current_price = fill.filled_price;
                    let atr = Decimal::ONE; // Placeholder
                    let stop_price = current_price - atr * pos_cfg.stop_atr_multiplier;
                    let pt1 = current_price + atr * pos_cfg.profit_target_1_atr;
                    let pt2 = current_price + atr * pos_cfg.profit_target_2_atr;

                    let state = PositionState {
                        entry_price: current_price,
                        stop_price,
                        atr_at_entry: atr,
                        profit_target_1: pt1,
                        profit_target_2: pt2,
                        trailing_stop_price: None,
                        partial_exit_done: false,
                        regime: regime.clone(),
                        profit_target_1_atr: pos_cfg.profit_target_1_atr,
                        profit_target_2_atr: pos_cfg.profit_target_2_atr,
                        trailing_atr_trending: pos_cfg.trailing_atr_trending,
                        trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
                        exchange_code: fill.exchange_code.clone(),
                    };
                    pos_states.insert(fill.symbol.clone(), (state, fill.filled_qty));
                    sqlx::query("INSERT OR REPLACE INTO positions (symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, regime_at_entry, qty, exchange_code) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        .bind(&fill.symbol).bind(current_price.to_string()).bind(stop_price.to_string()).bind(atr.to_string()).bind(pt1.to_string()).bind(pt2.to_string()).bind(format!("{:?}", regime)).bind(fill.filled_qty.to_string()).bind(&fill.exchange_code)
                        .execute(&db_pool).await.ok();
                } else {
                    pos_states.remove(&fill.symbol);
                    sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                }
            }
            Some(tick) = tick_pos_rx.recv() => {
                if let Some((state, qty)) = pos_states.get_mut(&tick.symbol) {
                    last_prices.insert(tick.symbol.clone(), tick.price);
                    let decision = evaluate_exit(state, tick.price);
                    match decision {
                        ExitDecision::Hold => {
                            if let Some(new_ts) = calculate_trailing_stop(tick.price, state.atr_at_entry, &state.regime, state.trailing_atr_trending, state.trailing_atr_volatile) {
                                let update = match state.trailing_stop_price {
                                    Some(old) if new_ts > old => true,
                                    None => true,
                                    _ => false,
                                };
                                if update {
                                    state.trailing_stop_price = Some(new_ts);
                                    sqlx::query("UPDATE positions SET trailing_stop_price = ? WHERE symbol = ?").bind(new_ts.to_string()).bind(&tick.symbol).execute(&db_pool).await.ok();
                                }
                            }
                        }
                        ExitDecision::PartialExit { .. } => {
                            if !state.partial_exit_done {
                                let exit_qty = *qty / 2;
                                if exit_qty > 0 {
                                    let _ = force_order_tx.send(OrderRequest {
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: exit_qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None,
                                    }).await;
                                }
                                state.partial_exit_done = true;
                                sqlx::query("UPDATE positions SET partial_exit_done = 1 WHERE symbol = ?").bind(&tick.symbol).execute(&db_pool).await.ok();
                            }
                        }
                        ExitDecision::StopLoss | ExitDecision::FullExit | ExitDecision::TrailingStop => {
                            let _ = force_order_tx.send(OrderRequest {
                                symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None,
                            }).await;
                            tracing::info!(symbol = %tick.symbol, ?decision, "Exit triggered");
                        }
                    }
                }
            }
        }

        let mut positions = Vec::new();
        for (symbol, (state, qty)) in &pos_states {
            let current_price = last_prices
                .get(symbol)
                .cloned()
                .unwrap_or(state.entry_price);
            positions.push(Position {
                symbol: symbol.clone(),
                name: None,
                qty: *qty as i64,
                avg_price: state.entry_price,
                current_price,
                pnl_pct: ((current_price - state.entry_price)
                    / state.entry_price.max(Decimal::ONE))
                .to_f64()
                .unwrap_or(0.0),
                unrealized_pnl: (current_price - state.entry_price) * Decimal::from(*qty),
                stop_price: state.stop_price,
                trailing_stop: state.trailing_stop_price,
                profit_target_1: state.profit_target_1,
                profit_target_2: state.profit_target_2,
                regime: format!("{:?}", state.regime),
            });
        }
        live_state_tx
            .send(MarketLiveState {
                positions,
                daily_pnl_r,
                regime: format!("{:?}", *regime_rx.borrow()),
            })
            .ok();
    }
}
