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
    /// 목표 보유 일수 (swing: 1~5일, position: 7일+)
    pub max_holding_days: u32,
    /// 진입 날짜 (KST 기준) — 보유기간 초과 판단에 사용
    pub entered_date: chrono::NaiveDate,
    /// 청산 사유 (fill 수신 시 realized_pnl_log에 기록)
    pub pending_exit_reason: String,
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
        // 1차 익절 전이라도 trailing_stop이 fixed stop보다 높으면 ATR 기반 손절 적용
        // (상승 후 되돌림 구간에서 손실 확대 방지)
        if let Some(ts) = pos.trailing_stop_price {
            if ts > pos.stop_price && current_price <= ts {
                return ExitDecision::TrailingStop;
            }
        }
    }

    ExitDecision::Hold
}

/// 레짐별 trailing stop 가격 계산. 스펙 Section 7 기반.
/// Chandelier Exit 방식 trailing stop.
/// Trending/Volatile: ATR × regime_multiplier, Quiet: ATR × 3.0 (보수적).
/// 반환값은 최근 고가(롤링 최고가) - ATR × k.
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
        // Quiet 레짐에서도 3× ATR 보수적 trailing stop 적용 (포지션 보호)
        MarketRegime::Quiet => Decimal::from(3),
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
    if let Ok(rows) = sqlx::query("SELECT symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, trailing_stop_price, partial_exit_done, regime_at_entry, qty, exchange_code, max_holding_days, entered_at FROM positions")
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
            let max_holding_days: u32 = row.try_get::<i64, _>("max_holding_days").unwrap_or(5) as u32;
            let entered_at_str: String = row.try_get("entered_at").unwrap_or_default();
            let entered_date = chrono::DateTime::parse_from_rfc3339(&entered_at_str)
                .ok()
                .map(|dt| dt.with_timezone(&chrono_tz::Asia::Seoul).date_naive())
                .unwrap_or_else(|| {
                    chrono::Utc::now().with_timezone(&chrono_tz::Asia::Seoul).date_naive()
                });

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
                max_holding_days,
                entered_date,
                pending_exit_reason: "Unknown".to_string(),
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
                // 손절가: ATR 기반 + 주가대별 % 상한(좁은 쪽 선택) + 85% 하한
                let max_stop_pct = if entry < dec!(5.0) {
                    dec!(0.08)
                } else if entry < dec!(50.0) {
                    dec!(0.05)
                } else {
                    dec!(0.03)
                };
                let stop = (entry - pos_cfg.stop_atr_multiplier * atr)
                    .max(entry - entry * max_stop_pct)
                    .max(entry * dec!(0.85));
                // 실제 위험(entry - stop) 기준으로 목표가 계산 (R:R 일관성 유지)
                let actual_risk = (entry - stop).max(dec!(0.01));
                let pt1 = entry + pos_cfg.profit_target_1_atr * actual_risk;
                let pt2 = entry + pos_cfg.profit_target_2_atr * actual_risk;
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
                    max_holding_days: 5,
                    entered_date: chrono::Utc::now()
                        .with_timezone(&chrono_tz::Asia::Seoul)
                        .date_naive(),
                    pending_exit_reason: "Unknown".to_string(),
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
                    sqlx::query("DELETE FROM positions WHERE symbol = ?")
                        .bind(&symbol)
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
                if let Ok(Ok(balance)) = tokio::time::timeout(std::time::Duration::from_secs(30), adapter.balance()).await {
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
                        // API에 없는 종목은 청산된 것으로 처리
                        // exit_pending 중이더라도 balance API에 없으면 phantom 포지션 → 제거
                        let stale_symbols: Vec<String> = pos_states.iter()
                            .filter(|(s, _)| !api_qty_map.contains_key(*s))
                            .map(|(s, _)| s.clone())
                            .collect();
                        for sym in stale_symbols {
                            tracing::warn!(symbol = %sym, "balance sync: API에 없음 → 포지션 삭제");
                            // 포지션 제거 전 실현 손익 기록 (last_price 기준 추정)
                            if let Some((state, qty)) = pos_states.get(&sym) {
                                let exit_price = last_prices.get(&sym).copied().unwrap_or(state.entry_price);
                                let pnl = (exit_price - state.entry_price) * Decimal::from(*qty);
                                let commission = calculate_commission(market_id, state.entry_price, exit_price, *qty, adapter.fx_spread_pct());
                                let net_pnl = pnl - commission;
                                let pnl_pct = if state.entry_price > Decimal::ZERO {
                                    ((exit_price - state.entry_price) / state.entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                                } else { 0.0 };
                                let now_rfc = chrono::Utc::now().to_rfc3339();
                                sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, commission, net_pnl, exit_reason, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                    .bind(uuid::Uuid::new_v4().to_string())
                                    .bind(&sym)
                                    .bind(market_name)
                                    .bind(qty.to_string())
                                    .bind(state.entry_price.to_string())
                                    .bind(exit_price.to_string())
                                    .bind(pnl.to_string())
                                    .bind(pnl_pct)
                                    .bind(commission.to_string())
                                    .bind(net_pnl.to_string())
                                    .bind("BalanceSync")
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
                write_session_stats(&db_pool, market_name).await;
                let eod_symbols: Vec<String> = pos_states.keys().cloned().collect();
                for symbol in &eod_symbols {
                    if let Some((state, qty)) = pos_states.get(symbol) {
                        // last_prices 없으면(봇 재시작 등) entry_price 폴백 — price=None(=0)은 호가단위 오류
                        let price = last_prices.get(symbol).copied()
                            .or(Some(state.entry_price));
                        if let Err(e) = force_order_tx.send(OrderRequest {
                            symbol: symbol.clone(), side: Side::Sell, qty: *qty, price, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false, max_holding_days: 0,
                        }).await {
                            tracing::error!(symbol = %symbol, error = %e, "EOD fallback: Failed to send force order for full exit");
                        }
                    }
                }
                // exit_pending 설정 — 이후 틱에서 중복 청산 주문 방지
                let now_inst = std::time::Instant::now();
                for symbol in &eod_symbols {
                    if let Some((state, _)) = pos_states.get_mut(symbol) {
                        state.exit_pending = true;
                        state.exit_pending_since = Some(now_inst);
                    }
                }
            }
            Some(_) = eod_rx.recv() => {
                tracing::info!(market = %market_name, "EOD trigger received — closing all positions");
                write_session_stats(&db_pool, market_name).await;
                let eod_symbols: Vec<String> = pos_states.keys().cloned().collect();
                for symbol in &eod_symbols {
                    if let Some((state, qty)) = pos_states.get(symbol) {
                        // last_prices 없으면 entry_price 폴백
                        let price = last_prices.get(symbol).copied()
                            .or(Some(state.entry_price));
                        if let Err(e) = force_order_tx.send(OrderRequest {
                            symbol: symbol.clone(), side: Side::Sell, qty: *qty, price, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false, max_holding_days: 0,
                        }).await {
                            tracing::error!(symbol = %symbol, error = %e, "EOD trigger: Failed to send force order for full exit");
                        }
                    }
                }
                // exit_pending 설정 — 이후 틱에서 중복 청산 주문 방지
                let now_inst = std::time::Instant::now();
                for symbol in &eod_symbols {
                    if let Some((state, _)) = pos_states.get_mut(symbol) {
                        state.exit_pending = true;
                        state.exit_pending_since = Some(now_inst);
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
                    // EOD 발동 후 청산 실패 → 최대 3회 자동 재시도 (호가단위 오류 등 일시적 실패 대응)
                    if eod_fallback_fired {
                        if let Some((state, qty)) = pos_states.get_mut(&fill.symbol) {
                            let retry = state.exit_timeout_count;
                            state.exit_timeout_count += 1;
                            state.exit_pending = false;
                            state.exit_pending_since = None;
                            if retry < 3 {
                                let price = last_prices.get(&fill.symbol).copied()
                                    .or(Some(state.entry_price));
                                let exchange_code = state.exchange_code.clone();
                                let qty = *qty;
                                tracing::warn!(
                                    symbol = %fill.symbol,
                                    retry = retry + 1,
                                    "EOD 청산 실패 — 재시도 ({}/3)", retry + 1
                                );
                                if force_order_tx.try_send(OrderRequest {
                                    symbol: fill.symbol.clone(), side: Side::Sell, qty, price,
                                    atr: None, exchange_code, strength: None,
                                    is_short: false, max_holding_days: 0,
                                }).is_ok() {
                                    state.exit_pending = true;
                                    state.exit_pending_since = Some(std::time::Instant::now());
                                }
                            } else {
                                tracing::warn!(symbol = %fill.symbol, "EOD 청산 3회 재시도 초과 — 포기, 내일 개장 시 처리");
                            }
                        }
                    } else if let Some((state, _)) = pos_states.get_mut(&fill.symbol) {
                        state.exit_pending = false;
                        state.exit_pending_since = None;
                        // exit_timeout_count는 유지 — 리셋하면 force_remove(count≥5) 카운트가 무한 초기화됨
                        tracing::info!(symbol = %fill.symbol, count = state.exit_timeout_count, "Order failure/cancel received, resetting exit_pending");
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
                    // 손절가: ATR 기반 + 주가대별 % 상한(좁은 쪽 선택) + 85% 하한
                    let max_stop_pct = if current_price < rust_decimal_macros::dec!(5.0) {
                        rust_decimal_macros::dec!(0.08)
                    } else if current_price < rust_decimal_macros::dec!(50.0) {
                        rust_decimal_macros::dec!(0.05)
                    } else {
                        rust_decimal_macros::dec!(0.03)
                    };
                    let stop_price = (current_price - atr * pos_cfg.stop_atr_multiplier)
                        .max(current_price - current_price * max_stop_pct)
                        .max(current_price * rust_decimal_macros::dec!(0.85));
                    // 실제 위험(entry - stop) 기준으로 목표가 계산.
                    // ATR이 큰 종목(KR 주식 등)에서 % 상한으로 stop이 좁아진 경우에도
                    // R:R 비율이 유지되도록 actual_risk를 기준으로 pt 산출.
                    let actual_risk = (current_price - stop_price).max(rust_decimal_macros::dec!(0.01));
                    let pt1 = current_price + pos_cfg.profit_target_1_atr * actual_risk;
                    let pt2 = current_price + pos_cfg.profit_target_2_atr * actual_risk;

                    let entered_date = chrono::Utc::now()
                        .with_timezone(&chrono_tz::Asia::Seoul)
                        .date_naive();
                    let max_holding_days = fill.max_holding_days;
                    let holding_type = if max_holding_days <= 5 { "swing" } else { "position" };
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
                        max_holding_days,
                        entered_date,
                        pending_exit_reason: "Unknown".to_string(),
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
                    sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                    if let Err(e) = sqlx::query("INSERT OR REPLACE INTO positions (id, order_id, symbol, entry_price, stop_price, atr_at_entry, profit_target_1, profit_target_2, regime_at_entry, qty, exchange_code, holding_type, max_holding_days, entered_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
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
                        .bind(holding_type)
                        .bind(max_holding_days as i64)
                        .bind(&now)
                        .bind(&now)
                        .execute(&db_pool).await {
                        tracing::error!(symbol = %fill.symbol, "포지션 DB 저장 실패 — 재시작 후 복구 불가: {e}");
                        summary_alert.warn(format!("⚠️ [{market_name}] {} 포지션 DB 저장 실패 — 수동 확인 필요", fill.symbol));
                    }
                    summary_alert.info(format!(
                        "📥 진입 [{market_name}] {} × {}주 @ {} [{}]\n스탑: {} | 목표1: {} | 목표2: {}",
                        fill.symbol, fill.filled_qty, current_price, holding_type, stop_price, pt1, pt2
                    ));
                } else {
                    // sell fill: partial vs full 구분
                    let pos_qty = pos_states.get(&fill.symbol).map(|(_, q)| *q).unwrap_or(0);
                    if fill.filled_qty >= pos_qty {
                        // 전체 청산
                        let entry_price = pos_states.get(&fill.symbol).map(|(s, _)| s.entry_price).unwrap_or(fill.filled_price);
                        let exit_reason = pos_states.get(&fill.symbol).map(|(s, _)| s.pending_exit_reason.clone()).unwrap_or_else(|| "Unknown".to_string());
                        let sym_label = symbol_names.get(&fill.symbol).map(|n| format!("{n} ({})", fill.symbol)).unwrap_or_else(|| fill.symbol.clone());
                        let pnl = (fill.filled_price - entry_price) * Decimal::from(fill.filled_qty);
                        let commission = calculate_commission(market_id, entry_price, fill.filled_price, fill.filled_qty, adapter.fx_spread_pct());
                        let net_pnl = pnl - commission;
                        let pnl_pct = if entry_price > Decimal::ZERO {
                            ((fill.filled_price - entry_price) / entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                        } else { 0.0 };
                        let net_pnl_f = net_pnl.to_f64().unwrap_or(0.0);
                        pos_states.remove(&fill.symbol);
                        sqlx::query("DELETE FROM positions WHERE symbol = ?").bind(&fill.symbol).execute(&db_pool).await.ok();
                        let now_rfc = chrono::Utc::now().to_rfc3339();
                        sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, commission, net_pnl, exit_reason, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                            .bind(uuid::Uuid::new_v4().to_string())
                            .bind(&fill.symbol)
                            .bind(market_name)
                            .bind(fill.filled_qty.to_string())
                            .bind(entry_price.to_string())
                            .bind(fill.filled_price.to_string())
                            .bind(pnl.to_string())
                            .bind(pnl_pct)
                            .bind(commission.to_string())
                            .bind(net_pnl.to_string())
                            .bind(&exit_reason)
                            .bind(&now_rfc)
                            .execute(&db_pool).await.ok();
                        realized_today += net_pnl_f;
                        realized_month += net_pnl_f;
                        realized_total += net_pnl_f;
                        summary_alert.info(format!(
                            "📤 청산 [{market_name}] {sym_label} × {}주 @ {} ({:+.2}%) / 순손익 {:+.0} (수수료 {:.0})",
                            fill.filled_qty, fill.filled_price, pnl_pct, net_pnl_f, commission.to_f64().unwrap_or(0.0)
                        ));
                    } else {
                        // 부분 체결 (1차 익절)
                        if let Some((state, qty)) = pos_states.get_mut(&fill.symbol) {
                            let entry_price = state.entry_price;
                            let remaining = qty.saturating_sub(fill.filled_qty);
                            *qty = remaining;
                            state.partial_exit_done = true;
                            state.exit_pending = false;
                            let now = chrono::Utc::now().to_rfc3339();
                            sqlx::query("UPDATE positions SET qty = ?, partial_exit_done = 1, updated_at = ? WHERE symbol = ?")
                                .bind(remaining.to_string()).bind(&now).bind(&fill.symbol).execute(&db_pool).await.ok();
                            let pnl = (fill.filled_price - entry_price) * Decimal::from(fill.filled_qty);
                            let commission = calculate_commission(market_id, entry_price, fill.filled_price, fill.filled_qty, adapter.fx_spread_pct());
                            let net_pnl = pnl - commission;
                            let pnl_pct = if entry_price > Decimal::ZERO {
                                ((fill.filled_price - entry_price) / entry_price * Decimal::from(100)).to_f64().unwrap_or(0.0)
                            } else { 0.0 };
                            let net_pnl_f = net_pnl.to_f64().unwrap_or(0.0);
                            sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, commission, net_pnl, exit_reason, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                .bind(uuid::Uuid::new_v4().to_string())
                                .bind(&fill.symbol)
                                .bind(market_name)
                                .bind(fill.filled_qty.to_string())
                                .bind(entry_price.to_string())
                                .bind(fill.filled_price.to_string())
                                .bind(pnl.to_string())
                                .bind(pnl_pct)
                                .bind(commission.to_string())
                                .bind(net_pnl.to_string())
                                .bind("PartialExit")
                                .bind(&now)
                                .execute(&db_pool).await.ok();
                            realized_today += net_pnl_f;
                            realized_month += net_pnl_f;
                            realized_total += net_pnl_f;
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
                // exit_pending 타임아웃 체크: 2회 이상 시 balance 확인 후 강제 제거 (5회 대기 → 60초로 단축)
                let force_remove = if let Some((state, _)) = pos_states.get_mut(&tick.symbol) {
                    if state.exit_pending {
                        if let Some(since) = state.exit_pending_since {
                            if since.elapsed() > std::time::Duration::from_secs(30) {
                                state.exit_timeout_count += 1;
                                state.exit_pending = false;
                                state.exit_pending_since = None;
                                tracing::warn!(symbol = %tick.symbol, count = state.exit_timeout_count, "exit_pending 30초 타임아웃 → 리셋");
                                state.exit_timeout_count >= 2
                            } else { false }
                        } else { false }
                    } else { false }
                } else { false };

                if force_remove {
                    let sym_label = symbol_names.get(&tick.symbol).map(|n| format!("{n} ({})", tick.symbol)).unwrap_or_else(|| tick.symbol.clone());
                    // T+1 등 실제 보유 중인 포지션은 강제 제거하지 않음: balance API 최종 확인
                    // balance API 실패 시 안전하게 보유 중으로 간주 (false positive 방지)
                    let still_held = match tokio::time::timeout(std::time::Duration::from_secs(30), adapter.balance()).await {
                        Ok(Ok(bal)) => bal.positions.iter().any(|p| {
                            p.symbol == tick.symbol && p.qty.to_u64().unwrap_or(0) > 0
                        }),
                        Ok(Err(e)) => {
                            tracing::warn!(symbol = %tick.symbol, "exit_timeout: balance 조회 실패 — 강제 제거 스킵: {e}");
                            true
                        }
                        Err(_) => {
                            tracing::warn!(symbol = %tick.symbol, "exit_timeout: balance 조회 타임아웃 — 강제 제거 스킵");
                            true
                        }
                    };
                    if still_held {
                        tracing::warn!(symbol = %tick.symbol, "exit_timeout: balance에 실제 보유 — 강제 제거 스킵 (T+1 추정)");
                        if let Some((state, _)) = pos_states.get_mut(&tick.symbol) {
                            state.exit_pending = false;
                            state.exit_pending_since = None;
                        }
                    } else {
                        // balance에 없음 = 이미 매도 완료 (재시작 전 체결된 주문). 정상 정리 + PnL 기록.
                        let exit_price = last_prices.get(&tick.symbol).copied().unwrap_or(Decimal::ZERO);
                        if let Some((st, qty)) = pos_states.get(&tick.symbol) {
                            let ep = st.entry_price;
                            let xp = if exit_price > Decimal::ZERO { exit_price } else { ep };
                            let pnl = (xp - ep) * Decimal::from(*qty);
                            let commission = calculate_commission(market_id, ep, xp, *qty, adapter.fx_spread_pct());
                            let net_pnl = pnl - commission;
                            let pnl_pct = if ep > Decimal::ZERO { ((xp - ep) / ep * Decimal::from(100)).to_f64().unwrap_or(0.0) } else { 0.0 };
                            let net_pnl_f = net_pnl.to_f64().unwrap_or(0.0);
                            let now_rfc = chrono::Utc::now().to_rfc3339();
                            sqlx::query("INSERT INTO realized_pnl_log (id, symbol, market, qty, entry_price, exit_price, pnl, pnl_pct, commission, net_pnl, exit_reason, exited_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                .bind(uuid::Uuid::new_v4().to_string())
                                .bind(&tick.symbol)
                                .bind(market_name)
                                .bind(qty.to_string())
                                .bind(ep.to_string())
                                .bind(xp.to_string())
                                .bind(pnl.to_string())
                                .bind(pnl_pct)
                                .bind(commission.to_string())
                                .bind(net_pnl.to_string())
                                .bind("ExitTimeout")
                                .bind(&now_rfc)
                                .execute(&db_pool).await.ok();
                            realized_today += net_pnl_f;
                            realized_month += net_pnl_f;
                            realized_total += net_pnl_f;
                            tracing::warn!(symbol = %tick.symbol, pnl_pct = %format!("{pnl_pct:.2}%"), "exit_timeout: VTS 잔고 없음 확인 → DB 포지션 정리 (재시작 전 매도 완료 추정)");
                            summary_alert.info(format!("✅ [{market_name}] {sym_label} VTS 잔고 없음 → 정리 완료 ({pnl_pct:+.2}%)"));
                        }
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
                    // 보유기간 초과 체크: max_holding_days 초과 시 강제 청산
                    let holding_expired = {
                        let today_local = chrono::Utc::now()
                            .with_timezone(&chrono_tz::Asia::Seoul)
                            .date_naive();
                        let days_held = (today_local - state.entered_date).num_days() as u32;
                        days_held >= state.max_holding_days
                    };
                    let decision = if holding_expired && !state.exit_pending {
                        tracing::info!(
                            symbol = %tick.symbol,
                            max_holding_days = state.max_holding_days,
                            "보유기간 초과 → 강제 청산"
                        );
                        state.pending_exit_reason = "HoldingExpired".to_string();
                        ExitDecision::FullExit
                    } else if in_trading_hours {
                        let d = evaluate_exit(state, tick.price);
                        // exit 결정 시점에 사유를 PositionState에 기록
                        match &d {
                            ExitDecision::StopLoss => state.pending_exit_reason = "StopLoss".to_string(),
                            ExitDecision::TrailingStop => state.pending_exit_reason = "TrailingStop".to_string(),
                            ExitDecision::FullExit => state.pending_exit_reason = "TargetHit".to_string(),
                            ExitDecision::PartialExit { .. } => state.pending_exit_reason = "PartialExit".to_string(),
                            ExitDecision::Hold => {}
                        }
                        d
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
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: exit_qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false, max_holding_days: 0,
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
                                        symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false, max_holding_days: 0,
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
                                    symbol: tick.symbol.clone(), side: Side::Sell, qty: *qty, price: None, atr: None, exchange_code: state.exchange_code.clone(), strength: None, is_short: false, max_holding_days: 0,
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
        "SELECT COALESCE(SUM(CAST(net_pnl AS REAL)), 0.0) FROM realized_pnl_log \
         WHERE date(exited_at, '+9 hours') = date('now', '+9 hours') AND market = ?",
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let month: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(net_pnl AS REAL)), 0.0) FROM realized_pnl_log \
         WHERE strftime('%Y-%m', exited_at, '+9 hours') = strftime('%Y-%m', 'now', '+9 hours') AND market = ?"
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let total: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(net_pnl AS REAL)), 0.0) FROM realized_pnl_log WHERE market = ?",
    )
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    (today, month, total)
}

/// 수수료 계산.
/// KR: 매수 0.015% + 매도(거래세+수수료) 0.195% = 왕복 0.21%
/// US: 환전 스프레드 양방향 (fx_spread_pct는 단방향 기준)
fn calculate_commission(
    market_id: crate::market::MarketId,
    entry: Decimal,
    exit: Decimal,
    qty: u64,
    fx_spread_pct: Decimal,
) -> Decimal {
    use rust_decimal_macros::dec;
    let qty_d = Decimal::from(qty);
    match market_id {
        crate::market::MarketId::Kr | crate::market::MarketId::KrVts => {
            entry * dec!(0.00015) * qty_d + exit * dec!(0.00195) * qty_d
        }
        crate::market::MarketId::Us | crate::market::MarketId::UsVts => {
            (entry + exit) * qty_d * fx_spread_pct
        }
    }
}

async fn write_session_stats(db: &SqlitePool, market: &str) {
    let now = chrono::Utc::now().to_rfc3339();
    let date: String = sqlx::query_scalar("SELECT date('now', '+9 hours')")
        .fetch_one(db)
        .await
        .unwrap_or_else(|_| chrono::Utc::now().format("%Y-%m-%d").to_string());

    let pnl: f64 = sqlx::query_scalar(
        "SELECT COALESCE(SUM(CAST(pnl AS REAL)), 0.0) FROM realized_pnl_log \
         WHERE date(exited_at, '+9 hours') = ? AND market = ?",
    )
    .bind(&date)
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    let trade_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM realized_pnl_log \
         WHERE date(exited_at, '+9 hours') = ? AND market = ?",
    )
    .bind(&date)
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0);

    // 오늘 최대 단일 손실 (절대값)
    let max_drawdown: f64 = sqlx::query_scalar(
        "SELECT COALESCE(ABS(MIN(CAST(pnl AS REAL))), 0.0) FROM realized_pnl_log \
         WHERE date(exited_at, '+9 hours') = ? AND market = ? AND CAST(pnl AS REAL) < 0",
    )
    .bind(&date)
    .bind(market)
    .fetch_one(db)
    .await
    .unwrap_or(0.0);

    // 최근 연속 손실 횟수
    let consecutive_losses: i64 = {
        let pnls: Vec<f64> = sqlx::query_scalar(
            "SELECT CAST(pnl AS REAL) FROM realized_pnl_log \
             WHERE date(exited_at, '+9 hours') = ? AND market = ? ORDER BY exited_at DESC",
        )
        .bind(&date)
        .bind(market)
        .fetch_all(db)
        .await
        .unwrap_or_default();
        pnls.iter().take_while(|&&p| p < 0.0).count() as i64
    };

    sqlx::query(
        "INSERT INTO session_stats (date, pnl, consecutive_losses, trade_count, max_drawdown, profile_active, updated_at) \
         VALUES (?, ?, ?, ?, ?, 'default', ?) \
         ON CONFLICT(date) DO UPDATE SET \
           pnl = excluded.pnl, \
           consecutive_losses = excluded.consecutive_losses, \
           trade_count = excluded.trade_count, \
           max_drawdown = excluded.max_drawdown, \
           updated_at = excluded.updated_at",
    )
    .bind(&date)
    .bind(pnl.to_string())
    .bind(consecutive_losses)
    .bind(trade_count)
    .bind(max_drawdown.to_string())
    .bind(&now)
    .execute(db)
    .await
    .ok();

    tracing::info!(market, date, pnl, trade_count, "session_stats 기록 완료");
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
            max_holding_days: 5,
            entered_date: chrono::Utc::now()
                .with_timezone(&chrono_tz::Asia::Seoul)
                .date_naive(),
            pending_exit_reason: "Unknown".to_string(),
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
    fn trailing_stop_price_for_quiet_regime() {
        // Quiet 레짐: 3.0× ATR conservative trailing stop
        let stop = calculate_trailing_stop(
            dec!(120),
            dec!(5),
            &MarketRegime::Quiet,
            dec!(2.0),
            dec!(1.0),
        );
        // 120 - 5 × 3.0 = 105
        assert_eq!(stop, Some(dec!(105)));
    }
}
