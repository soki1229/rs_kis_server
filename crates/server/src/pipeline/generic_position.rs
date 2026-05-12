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
use tokio::sync::{mpsc, watch, Notify};
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
    /// exit 주문 전송 후 true → 다음 tick에 중복 청산 주문 방지
    pub exit_pending: bool,
    /// exit_pending 설정 시각 — 90초 초과 시 자동 리셋 (Cancelled 감지 대체)
    pub exit_pending_since: Option<std::time::Instant>,
    /// exit 90초 타임아웃 횟수 — 5회 이상이면 포지션 강제 제거 (잔고없음 무한루프 방지)
    pub exit_timeout_count: u32,
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
    refresh_notify: std::sync::Arc<Notify>,
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
    let mut symbol_names: HashMap<String, String> = HashMap::new();
    let mut available_cash: Option<Decimal> = None;
    let mut eod_fallback_fired = false;
    // 이번 세션에서 fatal 오류로 강제 제거된 심볼 — balance_sync 재복구 방지
    let mut fatal_removed: std::collections::HashSet<String> = std::collections::HashSet::new();
    // 실현 손익 누적 (DB에서 로드, fill 시 증분 갱신)
    let (mut realized_today, mut realized_month, mut realized_total) =
        query_pnl_summary(&db_pool, market_name).await;
    let mut initial_equity = query_initial_equity(&db_pool).await;

    // daily_ohlc 테이블에서 종목명 로드
    if let Ok(rows) =
        sqlx::query("SELECT symbol, name FROM daily_ohlc WHERE name IS NOT NULL GROUP BY symbol")
            .fetch_all(&db_pool)
            .await
    {
        for row in rows {
            let sym: String = row.get("symbol");
            let name: String = row.get("name");
            symbol_names.insert(sym, name);
        }
    }

    // 5분마다 잔고 API 동기화 타이머
    let mut balance_sync_interval = tokio::time::interval(std::time::Duration::from_secs(5 * 60));
    balance_sync_interval.tick().await; // 첫 틱 즉시 발생 — 건너뜀
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
                exit_pending: false,
                exit_pending_since: None,
                exit_timeout_count: 0,
            };
            pos_states.insert(symbol, (state, qty.parse().unwrap_or(0)));
        }
    }

    // balance() API로 실제 포지션 동기화: DB에 없는 orphaned 포지션도 복구
    if let Ok(balance) = adapter.balance().await.map_err(|e| {
        tracing::error!(market = %market_name, "초기 balance API 실패 — 포지션 복구 불가: {e}");
        e
    }) {
        available_cash = Some(balance.available_cash);
        // balance API에서 종목명 보강 (US 등 daily_ohlc에 없는 경우)
        for api_pos in &balance.positions {
            if let Some(name) = &api_pos.name {
                if !name.is_empty() {
                    symbol_names
                        .entry(api_pos.symbol.clone())
                        .or_insert_with(|| name.clone());
                }
            }
        }
        {
            let api_positions = balance.positions;
            // 초기 balance sync: DB에는 있지만 API에 없는 포지션 정리
            // (재시작 시 이전 세션에서 청산된 포지션이 DB에 남아있으면 exit_pending 루프 발생)
            let api_qty_map: HashMap<String, u64> = api_positions
                .iter()
                .filter_map(|p| {
                    p.qty
                        .to_u64()
                        .filter(|&q| q > 0)
                        .map(|q| (p.symbol.clone(), q))
                })
                .collect();

            let stale_symbols: Vec<String> = pos_states
                .iter()
                .filter(|(s, _)| !api_qty_map.contains_key(*s))
                .map(|(s, _)| s.clone())
                .collect();

            for sym in stale_symbols {
                tracing::warn!(symbol = %sym, "초기 balance sync: API에 없음 → 포지션 삭제 (stale)");
                pos_states.remove(&sym);
                sqlx::query("DELETE FROM positions WHERE symbol = ?")
                    .bind(&sym)
                    .execute(&db_pool)
                    .await
                    .ok();
            }

            // 초기 qty 불일치 수정
            for (sym, api_qty) in &api_qty_map {
                if let Some((_, pos_qty)) = pos_states.get_mut(sym) {
                    if *pos_qty != *api_qty {
                        tracing::warn!(symbol = %sym, pos_qty = %pos_qty, api_qty = %api_qty, "초기 balance sync: qty 불일치 → API 기준으로 수정");
                        *pos_qty = *api_qty;
                        sqlx::query("UPDATE positions SET qty = ? WHERE symbol = ?")
                            .bind(api_qty.to_string())
                            .bind(sym)
                            .execute(&db_pool)
                            .await
                            .ok();
                    }
                }
            }

            for api_pos in api_positions {
                // DB에 이미 있는 포지션은 스킵 (위에서 qty 동기화 완료)
                if pos_states.contains_key(&api_pos.symbol) {
                    continue;
                }
                // 이번 세션에서 fatal 오류로 강제 제거된 심볼은 재복구 차단
                if fatal_removed.contains(&api_pos.symbol) {
                    tracing::warn!(symbol = %api_pos.symbol, "fatal_removed 심볼 — orphaned recovery 스킵");
                    continue;
                }
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
                let exchange_code = if matches!(
                    market_id,
                    crate::market::MarketId::Kr | crate::market::MarketId::KrVts
                ) {
                    None
                } else {
                    Some("NASD".to_string())
                };

                let state = PositionState {
                    entry_price: entry,
                    stop_price: stop,
                    atr_at_entry: atr,
                    profit_target_1: pt1,
                    profit_target_2: pt2,
                    trailing_stop_price: None,
                    partial_exit_done: false,
                    exit_pending: false,
                    exit_pending_since: None,
                    exit_timeout_count: 0,
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
                    "잔고 API로 포지션 복구 (orphaned)"
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

    // 복구된 포지션의 현재가를 API로 조회해 last_prices 초기화
    for symbol in pos_states.keys() {
        match adapter.current_price(symbol).await {
            Ok(price) => {
                last_prices.insert(symbol.clone(), price);
            }
            Err(e) => {
                tracing::warn!(symbol = %symbol, error = %e, "현재가 조회 실패 — 진입가로 대체");
            }
        }
    }

    // 초기 포지션 상태를 live_state_tx에 발행 (상태 조회 명령에서 포지션이 보이도록)
    publish_live_state(
        &live_state_tx,
        &pos_states,
        &last_prices,
        &regime_rx,
        &symbol_names,
        available_cash,
        realized_today,
        realized_month,
        realized_total,
        initial_equity,
    );

    loop {
        tokio::select! {
            _ = token.cancelled() => break,

            // /status 즉시 갱신 요청 (Telegram에서 notify)
            _ = refresh_notify.notified() => {
                if let Ok(balance) = adapter.balance().await {
                    available_cash = Some(balance.available_cash);
                    for api_pos in &balance.positions {
                        if let Some(name) = &api_pos.name {
                            if !name.is_empty() {
                                symbol_names.entry(api_pos.symbol.clone()).or_insert_with(|| name.clone());
                            }
                        }
                    }
                }
                (realized_today, realized_month, realized_total) = query_pnl_summary(&db_pool, market_name).await;
                initial_equity = query_initial_equity(&db_pool).await;
                publish_live_state(&live_state_tx, &pos_states, &last_prices, &regime_rx, &symbol_names, available_cash, realized_today, realized_month, realized_total, initial_equity);
            }

            // 5분마다 잔고 API 동기화: qty 불일치 수정, available_cash 갱신, 종목명 보강
            _ = balance_sync_interval.tick() => {
                // 30s 타임아웃: balance API hang 시 position task 블록 방지 (eod_fallback 미발화 원인)
                match tokio::time::timeout(std::time::Duration::from_secs(30), adapter.balance()).await.unwrap_or_else(|_| {
                    tracing::warn!(market = %market_name, "balance sync 타임아웃 (30s) — 스킵");
                    Err(crate::error::BotError::ApiError { msg: "balance sync timeout".into() })
                }) {
                    Ok(balance) => {
                        available_cash = Some(balance.available_cash);
                        // 종목명 보강 (balance API에서 name 제공되는 경우)
                        for api_pos in &balance.positions {
                            if let Some(name) = &api_pos.name {
                                if !name.is_empty() {
                                    symbol_names.entry(api_pos.symbol.clone()).or_insert_with(|| name.clone());
                                }
                            }
                        }
                        // 실제 잔고와 내부 pos_states qty 비교 → 불일치 수정
                        let api_qty_map: HashMap<String, u64> = balance.positions
                            .iter()
                            .filter_map(|p| p.qty.to_u64().filter(|&q| q > 0).map(|q| (p.symbol.clone(), q)))
                            .collect();
                        // API에 없는 종목은 청산된 것으로 처리 (단, exit_pending 중인 포지션은 보호)
                        let stale_symbols: Vec<String> = pos_states.iter()
                            .filter(|(s, (state, _))| !api_qty_map.contains_key(*s) && !state.exit_pending)
                            .map(|(s, _)| s.clone())
                            .collect();
                        for sym in stale_symbols {
                            tracing::warn!(symbol = %sym, "balance sync: API에 없음 → 포지션 삭제");
                            // 포지션 제거 전 실현 손익 기록 (last_price 기준 추정)
                            if let Some((state, qty)) = pos_states.get(&sym) {
                                let exit_price = last_prices.get(&sym).copied().unwrap_or(state.entry_price);
                                let pnl = (exit_price - state.entry_price) * Decimal::from(*qty);
                                let pnl_pct = if state.entry_price > Decimal::ZERO {
                                    ((exit_price - state.entry_price) / state.entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                                } else { 0.0 };
                                let now_rfc = chrono::Utc::now().to_rfc3339();
                                sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                    .bind(uuid::Uuid::new_v4().to_string())
                                    .bind(&sym)
                                    .bind(market_name)
                                    .bind(qty.to_string())
                                    .bind(state.entry_price.to_string())
                                    .bind(exit_price.to_string())
                                    .bind(pnl.to_string())
                                    .bind(pnl_pct)
                                    .bind(&now_rfc)
                                    .execute(&db_pool).await.ok();
                            }
                            pos_states.remove(&sym);
                            sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&sym).execute(&db_pool).await.ok();
                        }
                        // qty 불일치 수정
                        for (sym, api_qty) in &api_qty_map {
                            if let Some((_, pos_qty)) = pos_states.get_mut(sym) {
                                if *pos_qty != *api_qty {
                                    tracing::warn!(symbol = %sym, pos_qty = %pos_qty, api_qty = %api_qty, "balance sync: qty 불일치 → API 기준으로 수정");
                                    *pos_qty = *api_qty;
                                    sqlx::query("UPDATE positions SET qty = ? WHERE symbol = ?").bind(api_qty.to_string()).bind(sym).execute(&db_pool).await.ok();
                                }
                            }
                        }
                        // 5분마다 PnL 집계 DB에서 재동기화
                        (realized_today, realized_month, realized_total) =
                            query_pnl_summary(&db_pool, market_name).await;
                        initial_equity = query_initial_equity(&db_pool).await;
                        publish_live_state(&live_state_tx, &pos_states, &last_prices, &regime_rx, &symbol_names, available_cash, realized_today, realized_month, realized_total, initial_equity);
                    }
                    Err(e) => tracing::warn!(market = %market_name, error = %e, "balance sync 실패"),
                }
            }

            _ = tokio::time::sleep_until(fallback_instant), if !eod_fallback_fired => {
                tracing::warn!(market = %market_name, "EOD fallback fired — forcing exit for all positions");
                eod_fallback_fired = true;
                for (symbol, (state, qty)) in &pos_states {
                    // VTS 시장가(price=None) 미체결 방지: last_prices 기준 지정가로 전송
                    let price = last_prices.get(symbol).copied();
                    if let Err(e) = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                    }).await {
                        tracing::error!(symbol = %symbol, error = %e, "EOD fallback: Failed to send force order for full exit");
                    }
                }
            }
            Some(_) = eod_rx.recv() => {
                tracing::info!(market = %market_name, "EOD trigger received — closing all positions");
                for (symbol, (state, qty)) in &pos_states {
                    let price = last_prices.get(symbol).copied();
                    if let Err(e) = force_order_tx.send(OrderRequest {
                        symbol: symbol.clone(), side: Side::Sell, qty: *qty, price, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                    }).await {
                        tracing::error!(symbol = %symbol, error = %e, "EOD trigger: Failed to send force order for full exit");
                    }
                }
            }
            Some(fill) = fill_rx.recv() => {
                if fill.fatal {
                    // 재시도 불가 치명적 오류 (e.g., 브로커 잔고 없음) → 포지션 강제 제거 + 재복구 차단
                    tracing::error!(symbol = %fill.symbol, "치명적 매도 실패 — 포지션 강제 제거 (브로커 잔고 없음 등)");
                    summary_alert.info(format!("⚠️ [{market_name}] {} 매도 불가 (브로커 잔고 없음) → 포지션 제거", fill.symbol));
                    pos_states.remove(&fill.symbol);
                    fatal_removed.insert(fill.symbol.clone());
                    sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                    continue;
                }
                if fill.filled_qty == 0 {
                    if let Some((state, _)) = pos_states.get_mut(&fill.symbol) {
                        state.exit_pending = false;
                        state.exit_pending_since = None;
                        state.exit_timeout_count = 0;
                        tracing::info!(symbol = %fill.symbol, "Order failure/cancel received, resetting exit_pending");
                    }
                    continue;
                }
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
                        exit_pending: false,
                        exit_pending_since: None,
                exit_timeout_count: 0,
                        regime: regime.clone(),
                        profit_target_1_atr: pos_cfg.profit_target_1_atr,
                        profit_target_2_atr: pos_cfg.profit_target_2_atr,
                        trailing_atr_trending: pos_cfg.trailing_atr_trending,
                        trailing_atr_volatile: pos_cfg.trailing_atr_volatile,
                        exchange_code: fill.exchange_code.clone(),
                    };
                    pos_states.insert(fill.symbol.clone(), (state, fill.filled_qty));
                    // 종목명 갱신 (daily_ohlc에 있으면 사용)
                    if let Ok(row) = sqlx::query("SELECT name FROM daily_ohlc WHERE symbol = ? AND name IS NOT NULL ORDER BY date DESC LIMIT 1")
                        .bind(&fill.symbol)
                        .fetch_one(&db_pool)
                        .await
                    {
                        if let Ok(name) = row.try_get::<String, _>("name") {
                            symbol_names.insert(fill.symbol.clone(), name);
                        }
                    }
                    let now = chrono::Utc::now().to_rfc3339();
                    if let Err(e) = sqlx::query("INSERT OR REPLACE INTO positions (id, order_id, symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, regime_at_entry, qty, exchange_code, entered_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
                        .execute(&db_pool).await {
                        tracing::error!(symbol = %fill.symbol, "포지션 DB 저장 실패 — 재시작 후 복구 불가: {e}");
                        summary_alert.warn(format!("⚠️ [{market_name}] {} 포지션 DB 저장 실패 — 수동 확인 필요", fill.symbol));
                    }
                    summary_alert.info(format!(
                        "📥 진입 [{market_name}] {} × {}주 @ {}\n스탑: {} | 목표1: {} | 목표2: {}",
                        fill.symbol, fill.filled_qty, current_price, stop_price, pt1, pt2
                    ));
                } else {
                    // sell fill: partial vs full 구분
                    let pos_qty = pos_states.get(&fill.symbol).map(|(_, q)| *q).unwrap_or(0);
                    if fill.filled_qty >= pos_qty {
                        // 전체 청산
                        let entry_price = pos_states.get(&fill.symbol).map(|(s, _)| s.entry_price).unwrap_or(fill.filled_price);
                        let sym_label = symbol_names.get(&fill.symbol).map(|n| format!("{n} ({})", fill.symbol)).unwrap_or_else(|| fill.symbol.clone());
                        let pnl = (fill.filled_price - entry_price) * Decimal::from(fill.filled_qty);
                        let pnl_pct = if entry_price > Decimal::ZERO {
                            ((fill.filled_price - entry_price) / entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                        } else { 0.0 };
                        let pnl_f = pnl.to_f64().unwrap_or(0.0);
                        pos_states.remove(&fill.symbol);
                        sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                        // 실현 손익 기록
                        let now_rfc = chrono::Utc::now().to_rfc3339();
                        sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                            .bind(uuid::Uuid::new_v4().to_string())
                            .bind(&fill.symbol)
                            .bind(market_name)
                            .bind(fill.filled_qty.to_string())
                            .bind(entry_price.to_string())
                            .bind(fill.filled_price.to_string())
                            .bind(pnl.to_string())
                            .bind(pnl_pct)
                            .bind(&now_rfc)
                            .execute(&db_pool).await.ok();
                        realized_today += pnl_f;
                        realized_month += pnl_f;
                        realized_total += pnl_f;
                        summary_alert.info(format!(
                            "📤 청산 [{market_name}] {sym_label} × {}주 @ {} ({:+.2}%) / 손익 {:+.0}",
                            fill.filled_qty, fill.filled_price, pnl_pct, pnl_f
                        ));
                    } else {
                        // 부분 체결 (1차 익절)
                        if let Some((state, qty)) = pos_states.get_mut(&fill.symbol) {
                            let entry_price = state.entry_price;
                            let remaining = qty.saturating_sub(fill.filled_qty);
                            *qty = remaining;
                            state.partial_exit_done = true; // fill 체결 시점에 설정
                            state.exit_pending = false; // 다음 주문 가능하게 리셋
                            let now = chrono::Utc::now().to_rfc3339();
                            sqlx::query("UPDATE positions SET qty = ?, partial_exit_done = 1, updated_at = ? WHERE symbol = ?")
                                .bind(remaining.to_string()).bind(&now).bind(&fill.symbol).execute(&db_pool).await.ok();
                            let pnl = (fill.filled_price - entry_price) * Decimal::from(fill.filled_qty);
                            let pnl_pct = if entry_price > Decimal::ZERO {
                                ((fill.filled_price - entry_price) / entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                            } else { 0.0 };
                            let pnl_f = pnl.to_f64().unwrap_or(0.0);
                            // 실현 손익 기록 (1차 익절)
                            sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                .bind(uuid::Uuid::new_v4().to_string())
                                .bind(&fill.symbol)
                                .bind(market_name)
                                .bind(fill.filled_qty.to_string())
                                .bind(entry_price.to_string())
                                .bind(fill.filled_price.to_string())
                                .bind(pnl.to_string())
                                .bind(pnl_pct)
                                .bind(&now)
                                .execute(&db_pool).await.ok();
                            realized_today += pnl_f;
                            realized_month += pnl_f;
                            realized_total += pnl_f;
                            let sym_label = symbol_names.get(&fill.symbol).map(|n| format!("{n} ({})", fill.symbol)).unwrap_or_else(|| fill.symbol.clone());
                            summary_alert.info(format!(
                                "🎯 1차익절 체결 [{market_name}] {sym_label} × {}주 @ {} ({:+.2}%)\n잔여 {}주 보유 중",
                                fill.filled_qty, fill.filled_price, pnl_pct, remaining
                            ));
                        }
                    }
                }
            }
            Some(tick) = tick_pos_rx.recv() => {
                // exit_pending 타임아웃 체크: 5회 초과 시 VTS 잔고없음 등으로 매도 불가 → 포지션 강제 제거
                let force_remove = if let Some((state, _)) = pos_states.get_mut(&tick.symbol) {
                    if state.exit_pending {
                        if let Some(since) = state.exit_pending_since {
                            if since.elapsed() > std::time::Duration::from_secs(30) {
                                state.exit_timeout_count += 1;
                                state.exit_pending = false;
                                state.exit_pending_since = None;
                                tracing::warn!(symbol = %tick.symbol, count = state.exit_timeout_count, "exit_pending 90초 타임아웃 → 리셋");
                                state.exit_timeout_count >= 5
                            } else { false }
                        } else { false }
                    } else { false }
                } else { false };

                if force_remove {
                    let sym_label = symbol_names.get(&tick.symbol).map(|n| format!("{n} ({})", tick.symbol)).unwrap_or_else(|| tick.symbol.clone());
                    // T+1 등 실제 보유 중인 포지션은 강제 제거하지 않음: balance API 최종 확인
                    let still_held = match adapter.balance().await {
                        Ok(bal) => bal.positions.iter().any(|p| {
                            p.symbol == tick.symbol && p.qty.to_u64().unwrap_or(0) > 0
                        }),
                        Err(_) => false,
                    };
                    if still_held {
                        tracing::warn!(symbol = %tick.symbol, "exit_timeout: balance에 실제 보유 — 강제 제거 스킵 (T+1 추정)");
                        if let Some((state, _)) = pos_states.get_mut(&tick.symbol) {
                            state.exit_pending = false;
                            state.exit_pending_since = None;
                        }
                    } else {
                    tracing::error!(symbol = %tick.symbol, "exit 5회 타임아웃 — 매도 불가 포지션 강제 제거");
                    summary_alert.info(format!("⚠️ [{market_name}] {sym_label} exit 5회 실패 → 포지션 강제 제거 (VTS 잔고 불일치 추정)"));
                    pos_states.remove(&tick.symbol);
                    sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&tick.symbol).execute(&db_pool).await.ok();
                    }
                } else if let Some((state, qty)) = pos_states.get_mut(&tick.symbol) {
                    last_prices.insert(tick.symbol.clone(), tick.price);
                    // 프리마켓/포스트마켓 틱으로 인한 오발주 방지
                    let now_utc = chrono::Utc::now();
                    let today = adapter.local_today();
                    let in_trading_hours = match (
                        adapter.market_open_utc(today),
                        adapter.market_close_utc(today),
                    ) {
                        (Some(open), Some(close)) => now_utc >= open && now_utc <= close,
                        _ => true,
                    };
                    let decision = if in_trading_hours {
                        evaluate_exit(state, tick.price)
                    } else {
                        ExitDecision::Hold
                    };
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
                            // partial_exit_done: 주문 발송이 아닌 fill 수신 시 세움
                            // exit_pending으로 중복 주문 방지 (취소 시 다음 틱에서 재발주됨)
                            if !state.partial_exit_done && !state.exit_pending {
                                let exit_qty = *qty / 2;
                                let sym_label = symbol_names.get(&tick.symbol).map(|n| format!("{n} ({})", tick.symbol)).unwrap_or_else(|| tick.symbol.clone());
                                let pnl_pct = ((tick.price - state.entry_price) / state.entry_price.max(Decimal::ONE) * Decimal::from(100)).to_f64().unwrap_or(0.0);
                                if exit_qty > 0 {
                                    if force_order_tx.send(OrderRequest {
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: exit_qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                                    }).await.is_ok() {
                                        state.exit_pending = true;
                                        state.exit_pending_since = Some(std::time::Instant::now());
                                    } else {
                                        tracing::error!(symbol = %tick.symbol, qty = exit_qty, "PartialExit 주문 전송 실패 — 채널 닫힘");
                                    }
                                    summary_alert.info(format!(
                                        "🎯 1차익절 주문 [{market_name}] {sym_label} × {}주 @ {} ({:+.2}%)",
                                        exit_qty, tick.price, pnl_pct
                                    ));
                                } else {
                                    // qty=1 등 절반이 0이 되는 경우: 전량 청산으로 대체
                                    if force_order_tx.send(OrderRequest {
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                                    }).await.is_ok() {
                                        state.exit_pending = true;
                                        state.exit_pending_since = Some(std::time::Instant::now());
                                    } else {
                                        tracing::error!(symbol = %tick.symbol, qty = *qty, "PartialExit→FullExit 주문 전송 실패 — 채널 닫힘");
                                    }
                                    state.partial_exit_done = true;
                                    summary_alert.info(format!(
                                        "🎯 목표가 달성 [{market_name}] {sym_label} × {}주 @ {} ({:+.2}%) — 전량청산 (qty=1)",
                                        *qty, tick.price, pnl_pct
                                    ));
                                }
                            }
                        }
                        ExitDecision::StopLoss | ExitDecision::FullExit | ExitDecision::TrailingStop => {
                            if !state.exit_pending {
                                if force_order_tx.send(OrderRequest {
                                    symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false,
                                }).await.is_ok() {
                                    state.exit_pending = true;
                                    state.exit_pending_since = Some(std::time::Instant::now());
                                } else {
                                    tracing::error!(symbol = %tick.symbol, ?decision, "Exit 주문 전송 실패 — 채널 닫힘");
                                }
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
        }

        publish_live_state(
            &live_state_tx,
            &pos_states,
            &last_prices,
            &regime_rx,
            &symbol_names,
            available_cash,
            realized_today,
            realized_month,
            realized_total,
            initial_equity,
        );
    }
}

async fn query_pnl_summary(db: &SqlitePool, market: &str) -> (f64, f64, f64) {
    let today: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0.0) FROM realized_pnl_log \
         WHERE date(exited_at, '+9 hours') = date('now', '+9 hours') AND market = ?",
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let month: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0.0) FROM realized_pnl_log \
         WHERE strftime('%Y-%m', exited_at, '+9 hours') = strftime('%Y-%m', 'now', '+9 hours') AND market = ?"
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let total: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0.0) FROM realized_pnl_log WHERE market = ?",
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    (today, month, total)
}

async fn query_initial_equity(db: &SqlitePool) -> Option<f64> {
    sqlx::query_scalar::<_, String>(
        "SELECT value FROM portfolio_config WHERE key = 'initial_equity'",
    )
    .fetch_optional(db)
    .await
    .ok()
    .flatten()
    .and_then(|v| v.parse::<f64>().ok())
}

#[allow(clippy::too_many_arguments)]
fn publish_live_state(
    live_state_tx: &watch::Sender<MarketLiveState>,
    pos_states: &HashMap<String, (PositionState, u64)>,
    last_prices: &HashMap<String, Decimal>,
    regime_rx: &RegimeReceiver,
    symbol_names: &HashMap<String, String>,
    available_cash: Option<Decimal>,
    realized_today: f64,
    realized_month: f64,
    realized_total: f64,
    initial_equity: Option<f64>,
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
                name: symbol_names.get(symbol).cloned(),
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
            daily_pnl_r: realized_today,
            regime: format!("{:?}", *regime_rx.borrow()),
            last_updated: Some(chrono::Utc::now()),
            available_cash,
            realized_today,
            realized_month,
            realized_total,
            initial_equity,
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
            exit_pending: false,
            exit_pending_since: None,
            exit_timeout_count: 0,
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
