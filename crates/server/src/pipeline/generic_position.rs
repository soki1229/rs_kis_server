//! Generic position task using MarketAdapter trait.
//!
//! This module provides a unified position management pipeline that works
//! with any market through the MarketAdapter abstraction.

use crate::config::PositionConfig;
use crate::market::MarketAdapter;
use crate::monitoring::alert::AlertRouter;
use crate::pipeline::TickData;
use crate::regime::RegimeReceiver;
use crate::state::MarketLiveState;
use crate::types::{FillInfo, MarketRegime, OrderRequest, Position, Side};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

pub struct PositionState {
    pub entry_price: Decimal,
    pub stop_price: Decimal,
    pub atr_at_entry: Decimal,
    pub profit_target_1: Decimal,
    pub profit_target_2: Decimal,
    pub trailing_stop_price: Option<Decimal>,
    pub partial_exit_done: bool,
    pub regime: MarketRegime,
    #[allow(dead_code)]
    pub profit_target_1_atr: Decimal,
    #[allow(dead_code)]
    pub profit_target_2_atr: Decimal,
    pub trailing_atr_trending: Decimal,
    pub trailing_atr_volatile: Decimal,
    /// KIS 시장분류코드: "J"=KOSPI, "Q"=KOSDAQ. US는 None.
    pub exchange_code: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExitDecision {
    /// 손절가 도달 → 시장가 전량 청산
    StopLoss,
    /// 1차 목표가 도달 → 50% 부분 익절. pct = 0.5
    PartialExit { pct: Decimal },
    /// 2차 목표가 도달 (1차 익절 후) → 잔여 전량 청산
    FullExit,
    /// Trailing stop 도달 → 잔여 전량 청산
    TrailingStop,
    /// 아무 조건도 해당 없음 → 보유 유지
    Hold,
}

/// 스펙 Section 7 기반 익절/손절 판정. 순수 함수.
/// 우선순위: StopLoss > FullExit > TrailingStop > PartialExit > Hold
pub fn evaluate_exit(pos: &PositionState, current_price: Decimal) -> ExitDecision {
    // 손절가 도달
    if current_price <= pos.stop_price {
        return ExitDecision::StopLoss;
    }

    if pos.partial_exit_done {
        // 2차 목표가 도달
        if current_price >= pos.profit_target_2 {
            return ExitDecision::FullExit;
        }
        // Trailing stop 도달 (1차 익절 후에만 적용)
        if let Some(ts) = pos.trailing_stop_price {
            if current_price <= ts {
                return ExitDecision::TrailingStop;
            }
        }
    } else {
        // 1차 목표가 도달
        if current_price >= pos.profit_target_1 {
            return ExitDecision::PartialExit {
                pct: Decimal::new(5, 1),
            }; // 0.5
        }
    }

    ExitDecision::Hold
}

/// 레짐별 trailing stop 가격 계산. 스펙 Section 7 기반.
/// Quiet 레짐은 trailing stop 없음 → None 반환.
pub fn calculate_trailing_stop(
    high_price: Decimal,
    atr: Decimal,
    regime: &MarketRegime,
    trending_multiplier: Decimal,
    volatile_multiplier: Decimal,
) -> Option<Decimal> {
    let multiplier = match regime {
        MarketRegime::Trending => trending_multiplier,
        MarketRegime::Volatile => volatile_multiplier,
        MarketRegime::Quiet => return None,
    };
    Some(high_price - atr * multiplier)
}

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
    token: CancellationToken,
    eod_fallback: chrono::DateTime<chrono::Utc>,
    pos_cfg: PositionConfig,
    summary_alert: AlertRouter,
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

    // DB가 비어 있으면 balance() API로 실제 포지션 동기화 (재시작 후 복구)
    if pos_states.is_empty() {
        if let Ok(balance) = adapter.balance().await {
            for api_pos in balance.positions {
                use rust_decimal_macros::dec;
                let qty = match api_pos.qty.to_u64() {
                    Some(q) if q > 0 => q,
                    _ => continue,
                };
                let entry = api_pos.avg_price;
                // ATR을 진입가의 2%로 추정
                let atr = if entry > Decimal::ZERO {
                    entry * dec!(0.02)
                } else {
                    Decimal::ONE
                };
                let stop = entry - pos_cfg.stop_atr_multiplier * atr;
                let pt1 = entry + pos_cfg.profit_target_1_atr * atr;
                let pt2 = entry + pos_cfg.profit_target_2_atr * atr;
                let exchange_code = Some("NASD".to_string());

                let state = PositionState {
                    entry_price: entry,
                    stop_price: stop,
                    atr_at_entry: atr,
                    profit_target_1: pt1,
                    profit_target_2: pt2,
                    trailing_stop_price: None,
                    partial_exit_done: false,
                    regime: MarketRegime::Trending,
                    profit_target_1_atr: pos_cfg.profit_target_1_atr,
                    profit_target_2_atr: pos_cfg.profit_target_2_atr,
                    trailing_atr_trending: pos_cfg.trailing_atr_trending,
                    trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
                    exchange_code: exchange_code.clone(),
                };

                tracing::info!(
                    market = %market_name,
                    symbol = %api_pos.symbol,
                    qty,
                    entry = %entry,
                    "잔고 API로 포지션 복구 (DB 없음)"
                );

                let now = chrono::Utc::now().to_rfc3339();
                let recovery_order_id = uuid::Uuid::new_v4().to_string();
                let pos_id = uuid::Uuid::new_v4().to_string();
                let symbol = api_pos.symbol.clone();

                // positions.order_id FK 충족을 위해 단일 트랜잭션으로 orders + positions 동시 삽입
                let result = async {
                    let mut tx = db_pool.begin().await?;
                    sqlx::query("INSERT OR IGNORE INTO orders (id, symbol, side, qty, price, atr, state, updated_at) VALUES (?, ?, 'buy', ?, NULL, NULL, 'Filled', ?)")
                        .bind(&recovery_order_id)
                        .bind(&symbol)
                        .bind(qty.to_string())
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query("INSERT OR REPLACE INTO positions (id, order_id, symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, regime_at_entry, qty, exchange_code, entered_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        .bind(&pos_id)
                        .bind(&recovery_order_id)
                        .bind(&symbol)
                        .bind(entry.to_string())
                        .bind(stop.to_string())
                        .bind(atr.to_string())
                        .bind(pt1.to_string())
                        .bind(pt2.to_string())
                        .bind("Trending")
                        .bind(qty.to_string())
                        .bind(exchange_code)
                        .bind(&now)
                        .bind(&now)
                        .execute(&mut *tx)
                        .await?;
                    tx.commit().await?;
                    Ok::<_, sqlx::Error>(())
                }.await;
                if let Err(e) = result {
                    tracing::error!(symbol = %api_pos.symbol, error = %e, "포지션 복구 DB 저장 실패");
                }

                pos_states.insert(api_pos.symbol, (state, qty));
            }
        }
    }

    // 초기 포지션 상태를 live_state_tx에 발행 (상태 조회 명령에서 포지션이 보이도록)
    publish_live_state(&live_state_tx, &pos_states, &last_prices, &regime_rx);

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = tokio::time::sleep_until(fallback_instant), if !eod_fallback_fired => {
                tracing::warn!(market = %market_name, "EOD fallback fired — forcing exit for all positions");
                eod_fallback_fired = true;
                for (symbol, (state, qty)) in &pos_states {
                    let _ = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                    }).await;
                }
            }
            Some(_) = eod_rx.recv() => {
                tracing::info!(market = %market_name, "EOD trigger received — closing all positions");
                for (symbol, (state, qty)) in &pos_states {
                    let _ = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                    }).await;
                }
            }
            Some(fill) = fill_rx.recv() => {
                if !pos_states.contains_key(&fill.symbol) {
                    let regime = regime_rx.borrow().clone();
                    let current_price = fill.filled_price;
                    // ATR is required for calculating stop loss and profit targets.
                    // If missing (e.g. manual fill or error), default to Decimal::ONE to avoid panic,
                    // although strategy should have provided it during Signal stage.
                    let atr = fill.atr.unwrap_or(Decimal::ONE);
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
                    let now = chrono::Utc::now().to_rfc3339();
                    sqlx::query("INSERT OR REPLACE INTO positions (id, order_id, symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, regime_at_entry, qty, exchange_code, entered_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        .bind(uuid::Uuid::new_v4().to_string())
                        .bind(&fill.order_id)
                        .bind(&fill.symbol)
                        .bind(current_price.to_string())
                        .bind(stop_price.to_string())
                        .bind(atr.to_string())
                        .bind(pt1.to_string())
                        .bind(pt2.to_string())
                        .bind(format!("{:?}", regime))
                        .bind(fill.filled_qty.to_string())
                        .bind(&fill.exchange_code)
                        .bind(&now)
                        .bind(&now)
                        .execute(&db_pool).await.ok();
                    summary_alert.info(format!(
                        "📥 진입 [{market_name}] {} × {}주 @ {}\n스탑: {} | 목표1: {} | 목표2: {}",
                        fill.symbol, fill.filled_qty, current_price, stop_price, pt1, pt2
                    ));
                } else {
                    pos_states.remove(&fill.symbol);
                    sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                    summary_alert.info(format!(
                        "📤 청산 [{market_name}] {} 포지션 종료",
                        fill.symbol
                    ));
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
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: exit_qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                                    }).await;
                                }
                                state.partial_exit_done = true;
                                sqlx::query("UPDATE positions SET partial_exit_done = 1 WHERE symbol = ?").bind(&tick.symbol).execute(&db_pool).await.ok();
                                let pnl_pct = ((tick.price - state.entry_price) / state.entry_price.max(Decimal::ONE) * Decimal::from(100)).to_f64().unwrap_or(0.0);
                                summary_alert.info(format!(
                                    "🎯 1차익절 [{market_name}] {} × {}주 @ {} ({:+.2}%)\n잔여 {}주 보유 중",
                                    tick.symbol, exit_qty, tick.price, pnl_pct, *qty - exit_qty
                                ));
                            }
                        }
                        ExitDecision::StopLoss | ExitDecision::FullExit | ExitDecision::TrailingStop => {
                            let _ = force_order_tx.send(OrderRequest {
                                symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                            }).await;
                            tracing::info!(symbol = %tick.symbol, ?decision, "Exit triggered");
                            let (icon, label) = match decision {
                                ExitDecision::StopLoss => ("🔴", "손절"),
                                ExitDecision::TrailingStop => ("🟠", "트레일링 스탑"),
                                _ => ("🟢", "목표가 도달"),
                            };
                            let pnl_pct = ((tick.price - state.entry_price) / state.entry_price.max(Decimal::ONE) * Decimal::from(100)).to_f64().unwrap_or(0.0);
                            summary_alert.info(format!(
                                "{icon} {label} [{market_name}] {} × {}주 @ {}\n진입: {} → 현재: {} ({:+.2}%)",
                                tick.symbol, *qty, tick.price, state.entry_price, tick.price, pnl_pct
                            ));
                        }
                    }
                }
            }
        }

        publish_live_state(&live_state_tx, &pos_states, &last_prices, &regime_rx);
    }
}

fn publish_live_state(
    live_state_tx: &watch::Sender<MarketLiveState>,
    pos_states: &HashMap<String, (PositionState, u64)>,
    last_prices: &HashMap<String, Decimal>,
    regime_rx: &RegimeReceiver,
) {
    let positions = pos_states
        .iter()
        .map(|(symbol, (state, qty))| {
            let current_price = last_prices
                .get(symbol)
                .cloned()
                .unwrap_or(state.entry_price);
            Position {
                symbol: symbol.clone(),
                name: None,
                qty: *qty as i64,
                avg_price: state.entry_price,
                current_price,
                pnl_pct: ((current_price - state.entry_price)
                    / state.entry_price.max(Decimal::ONE))
                .to_f64()
                .unwrap_or(0.0)
                    * 100.0,
                unrealized_pnl: (current_price - state.entry_price) * Decimal::from(*qty),
                stop_price: state.stop_price,
                trailing_stop: state.trailing_stop_price,
                profit_target_1: state.profit_target_1,
                profit_target_2: state.profit_target_2,
                regime: format!("{:?}", state.regime),
            }
        })
        .collect();
    live_state_tx
        .send(MarketLiveState {
            positions,
            daily_pnl_r: 0.0,
            regime: format!("{:?}", *regime_rx.borrow()),
        })
        .ok();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::MarketRegime;
    use rust_decimal_macros::dec;

    fn make_position(entry: f64, atr: f64) -> PositionState {
        let entry = Decimal::try_from(entry).unwrap();
        let atr = Decimal::try_from(atr).unwrap();
        PositionState {
            entry_price: entry,
            stop_price: entry - atr * dec!(2.0),
            atr_at_entry: atr,
            profit_target_1: entry + atr * dec!(2.0),
            profit_target_2: entry + atr * dec!(4.0),
            trailing_stop_price: None,
            partial_exit_done: false,
            regime: MarketRegime::Trending,
            profit_target_1_atr: dec!(2.0),
            profit_target_2_atr: dec!(4.0),
            trailing_atr_trending: dec!(2.0),
            trailing_atr_volatile: dec!(1.0),
            exchange_code: None,
        }
    }

    #[test]
    fn no_exit_when_in_normal_range() {
        let pos = make_position(100.0, 5.0);
        let decision = evaluate_exit(&pos, dec!(102));
        assert!(matches!(decision, ExitDecision::Hold));
    }

    #[test]
    fn stop_hit_triggers_full_exit() {
        let pos = make_position(100.0, 5.0);
        let decision = evaluate_exit(&pos, dec!(90));
        assert!(matches!(decision, ExitDecision::StopLoss));
    }

    #[test]
    fn first_target_hit_triggers_partial() {
        let pos = make_position(100.0, 5.0);
        let decision = evaluate_exit(&pos, dec!(110));
        assert!(matches!(decision, ExitDecision::PartialExit { .. }));
    }

    #[test]
    fn second_target_hit_triggers_full_exit() {
        let mut pos = make_position(100.0, 5.0);
        pos.partial_exit_done = true;
        let decision = evaluate_exit(&pos, dec!(120));
        assert!(matches!(decision, ExitDecision::FullExit));
    }

    #[test]
    fn trailing_stop_hit_after_partial_exit() {
        let mut pos = make_position(100.0, 5.0);
        pos.partial_exit_done = true;
        pos.trailing_stop_price = Some(dec!(105));
        let decision = evaluate_exit(&pos, dec!(104));
        assert!(matches!(decision, ExitDecision::TrailingStop));
    }

    #[test]
    fn trailing_stop_price_for_trending_regime() {
        let stop = calculate_trailing_stop(
            dec!(120),
            dec!(5),
            &MarketRegime::Trending,
            dec!(2.0),
            dec!(1.0),
        );
        assert_eq!(stop, Some(dec!(110)));
    }

    #[test]
    fn trailing_stop_price_for_volatile_regime() {
        let stop = calculate_trailing_stop(
            dec!(120),
            dec!(5),
            &MarketRegime::Volatile,
            dec!(2.0),
            dec!(1.0),
        );
        assert_eq!(stop, Some(dec!(115)));
    }

    #[test]
    fn trailing_stop_returns_none_for_quiet_regime() {
        let stop = calculate_trailing_stop(
            dec!(120),
            dec!(5),
            &MarketRegime::Quiet,
            dec!(2.0),
            dec!(1.0),
        );
        assert_eq!(stop, None);
    }
}
