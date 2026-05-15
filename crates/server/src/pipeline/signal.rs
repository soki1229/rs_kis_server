use crate::market::{MarketAdapter, MarketId};
use crate::pipeline::TickData;
use crate::state::BotState;
use crate::strategy::{
    Direction, Portfolio, QualResult, QualificationStrategy, RiskStrategy, SignalCandidate,
    SignalContext as StrategySignalContext, SignalStrategy,
};
use crate::types::{CandleBar, MarketRegime, QuoteSnapshot};
use crate::types::{OrderRequest, Side, WatchlistSet};
use rust_decimal::Decimal;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use tokio::sync::{broadcast, mpsc, watch, RwLock as TokioRwLock};
use tokio::time::{Duration, Instant};

const MAX_COMPLETED_CANDLES: usize = 120;
const BALANCE_CACHE_TTL_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct CompletedCandle {
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
    pub ts: chrono::DateTime<chrono::Utc>,
}

struct CandleAccumulator {
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: u64,
    count: usize,
}

impl CandleAccumulator {
    fn new() -> Self {
        Self {
            open: Decimal::ZERO,
            high: Decimal::MIN,
            low: Decimal::MAX,
            close: Decimal::ZERO,
            volume: 0,
            count: 0,
        }
    }
    fn push(&mut self, tick: TickData) {
        if self.count == 0 {
            self.open = tick.price;
        }
        self.high = self.high.max(tick.price);
        self.low = self.low.min(tick.price);
        self.close = tick.price;
        self.volume += tick.volume;
        self.count += 1;
    }
    fn is_signal_eligible(&self) -> bool {
        self.count >= 1
    }
    fn reset(&mut self) {
        self.open = Decimal::ZERO;
        self.high = Decimal::MIN;
        self.low = Decimal::MAX;
        self.close = Decimal::ZERO;
        self.volume = 0;
        self.count = 0;
    }
}

struct SignalState {
    candles: HashMap<String, CandleAccumulator>,
    completed: HashMap<String, VecDeque<CompletedCandle>>,
    latest_quotes: HashMap<String, QuoteSnapshot>,
    watchlist: WatchlistSet,
    candle_start: HashMap<String, Instant>,
    cached_balance: Option<(Decimal, Instant)>,
    symbol_exchange: HashMap<String, String>,
    pending_symbols: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    last_order_sent: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
}

struct SignalContext {
    symbol: String,
    market: MarketId,
    entry_cutoff_utc: Option<chrono::DateTime<chrono::Utc>>,
    db_pool: sqlx::SqlitePool,
    order_tx: mpsc::Sender<OrderRequest>,
    regime: MarketRegime,
    completed: Vec<CompletedCandle>,
    quote: Option<QuoteSnapshot>,
    summary: Arc<StdRwLock<crate::state::MarketSummary>>,
    account_balance: Decimal,
    exchange_code: Option<String>,
    strategy: crate::config::StrategyProfile,
    activity: crate::shared::activity::ActivityLog,
    notion: Option<Arc<TokioRwLock<crate::notion::NotionClient>>>,
    signal_strategy: Arc<dyn SignalStrategy>,
    qual_strategy: Arc<dyn QualificationStrategy>,
    risk_strategy: Arc<dyn RiskStrategy>,
    live_state_rx: watch::Receiver<crate::state::MarketLiveState>,
    last_order_sent: Arc<std::sync::Mutex<HashMap<String, Instant>>>,
}

fn entry_cutoff_utc(
    adapter: &dyn MarketAdapter,
    entry_blackout_close_mins: u32,
) -> Option<chrono::DateTime<chrono::Utc>> {
    adapter
        .market_close_utc(adapter.local_today())
        .map(|close| close - chrono::Duration::minutes(i64::from(entry_blackout_close_mins)))
}

fn is_entry_blackout(
    now: chrono::DateTime<chrono::Utc>,
    cutoff_utc: Option<chrono::DateTime<chrono::Utc>>,
) -> bool {
    cutoff_utc.is_some_and(|cutoff| now >= cutoff)
}

#[allow(clippy::too_many_arguments)]
async fn log_signal(
    db: &sqlx::SqlitePool,
    symbol: &str,
    setup_score: i32,
    rule_direction: Option<&str>,
    rule_strength: Option<f64>,
    action: &str,
    block_reason: Option<&str>,
    regime: &str,
) {
    sqlx::query(
        "INSERT INTO signal_log \
         (symbol, setup_score, rule_direction, rule_strength, action, llm_block_reason, regime, logged_at) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(symbol)
    .bind(setup_score)
    .bind(rule_direction)
    .bind(rule_strength)
    .bind(action)
    .bind(block_reason)
    .bind(regime)
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(db)
    .await
    .ok();
}

async fn evaluate_and_maybe_order(ctx: SignalContext) {
    let SignalContext {
        symbol,
        market,
        entry_cutoff_utc,
        db_pool,
        order_tx,
        regime,
        completed,
        quote,
        summary,
        account_balance,
        exchange_code,
        strategy,
        activity,
        notion,
        signal_strategy,
        qual_strategy,
        risk_strategy,
        live_state_rx,
        last_order_sent,
    } = ctx;
    use rust_decimal::prelude::ToPrimitive;

    let strategy_id = strategy.id.clone();

    let last = match completed.last() {
        Some(c) => c,
        None => return,
    };

    let current_price = last.close;
    if is_entry_blackout(chrono::Utc::now(), entry_cutoff_utc) {
        tracing::info!(
            market = %market.label(),
            symbol = %symbol,
            "신규 진입 skip — 장마감 진입금지 구간"
        );
        activity.record_eval(market.label(), &symbol, 0, "skip", "entry_blackout_close");
        log_signal(
            &db_pool,
            &symbol,
            0,
            None,
            None,
            "skip",
            Some("entry_blackout_close"),
            &format!("{:?}", regime),
        )
        .await;
        return;
    }

    let rolling_high = completed
        .iter()
        .map(|c| c.high)
        .fold(Decimal::MIN, Decimal::max);

    let candles: Vec<CandleBar> = completed
        .iter()
        .map(|c| CandleBar {
            date: c.ts.format("%Y%m%d%H%M%S").to_string(), // YYYYMMDDHHMMSS for intraday
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: c.volume,
        })
        .collect();

    // daily_bars 로드 (strategy trait의 db 의존성 제거를 위해 파이프라인이 사전 로드)
    use sqlx::Row;
    let daily_bars: Vec<CandleBar> = {
        let rows = sqlx::query(
            "SELECT date, open, high, low, close, volume FROM daily_ohlc \
             WHERE symbol = ? AND date != '0000-00-00' ORDER BY date DESC LIMIT 30",
        )
        .bind(&symbol)
        .fetch_all(&db_pool)
        .await
        .unwrap_or_default();

        rows.iter()
            .map(|r| CandleBar {
                date: r.get::<String, _>(0),
                open: r.get::<String, _>(1).parse().unwrap_or(Decimal::ZERO),
                high: r.get::<String, _>(2).parse().unwrap_or(Decimal::ZERO),
                low: r.get::<String, _>(3).parse().unwrap_or(Decimal::ZERO),
                close: r.get::<String, _>(4).parse().unwrap_or(Decimal::ZERO),
                volume: r
                    .get::<String, _>(5)
                    .parse::<Decimal>()
                    .unwrap_or(Decimal::ZERO)
                    .to_u64()
                    .unwrap_or(0),
            })
            .collect()
    };

    // 전일 종가 대비 당일 등락률 — HardBlock DailyGain/Loss 판정용 (daily_bars move 전에 계산)
    let daily_change_pct = daily_bars
        .get(1)
        .filter(|prev| prev.close > Decimal::ZERO)
        .map(|prev| {
            ((current_price - prev.close) / prev.close * rust_decimal_macros::dec!(100))
                .to_f64()
                .unwrap_or(0.0)
        })
        .unwrap_or(0.0);

    let strategy_ctx = StrategySignalContext {
        symbol: symbol.clone(),
        market,
        candles,
        daily_bars,
        quote,
        current_price,
        rolling_high,
        account_balance,
        regime: regime.clone(),
        setup_score_min: strategy.setup_score_min,
        regime_filter: strategy.regime_filter,
    };

    let trade_signal = match signal_strategy.evaluate(&strategy_ctx).await {
        Some(sig) => sig,
        None => {
            tracing::debug!("[{}] {} 신호 없음", market.label(), symbol);
            activity.record_eval(market.label(), &symbol, 0, "skip", "no_signal");
            log_signal(
                &db_pool,
                &symbol,
                0,
                None,
                None,
                "skip",
                Some("no_signal"),
                &format!("{:?}", regime),
            )
            .await;
            return;
        }
    };

    let score = (trade_signal.strength * 100.0) as i32;
    let setup_score = trade_signal.setup_score.unwrap_or(0);
    let direction_str = match trade_signal.direction {
        Direction::Long => "long",
        Direction::Short => "short",
    };
    let regime_str = format!("{:?}", regime);
    tracing::info!(
        "[{}] 📶 신호 후보: {} setup_score={} strength={:.2} {:?} @ {}",
        market.label(),
        symbol,
        trade_signal.setup_score.unwrap_or(0),
        trade_signal.strength,
        trade_signal.direction,
        current_price
    );

    let candidate = SignalCandidate {
        signal: trade_signal.clone(),
        regime: regime.clone(),
        setup_score: trade_signal.setup_score,
        daily_change_pct,
        // TODO: 파이프라인 레벨에서 실적/FOMC 캘린더 연동 후 실제 값 반영 필요
        has_earnings_event: false,
        has_fomc_today: false,
    };
    let qual_result = qual_strategy.qualify(&candidate);

    if let QualResult::Block { reason } = qual_result {
        tracing::info!("[{}] 🚫 {} 진입 차단 — {}", market.label(), symbol, reason);
        activity.record_eval(market.label(), &symbol, score, "blocked", &reason);
        log_signal(
            &db_pool,
            &symbol,
            setup_score,
            Some(direction_str),
            Some(trade_signal.strength),
            "block",
            Some(&reason),
            &regime_str,
        )
        .await;
        return;
    }

    let already_held = {
        let live = live_state_rx.borrow();
        live.positions.iter().any(|p| p.symbol == symbol)
    };
    // 아직 체결 안 된 Submitted 주문이 있으면 중복 진입 차단
    let has_pending_order = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM orders WHERE symbol = ? AND state = 'Submitted'",
    )
    .bind(&symbol)
    .fetch_one(&db_pool)
    .await
    .unwrap_or(0)
        > 0;
    if already_held || has_pending_order {
        let reason = match (already_held, has_pending_order) {
            (true, true) => "already_held_and_pending",
            (true, false) => "already_held",
            (false, true) => "pending_order",
            (false, false) => "unknown",
        };
        tracing::info!(
            market = %market.label(),
            symbol = %symbol,
            setup_score,
            strength = trade_signal.strength,
            already_held,
            has_pending_order,
            reason,
            "신규 진입 skip — 이미 보유 중이거나 제출 대기 주문 존재"
        );
        activity.record_eval(market.label(), &symbol, score, "skip", reason);
        log_signal(
            &db_pool,
            &symbol,
            setup_score,
            Some(direction_str),
            Some(trade_signal.strength),
            "skip",
            Some(reason),
            &regime_str,
        )
        .await;
        return;
    }

    let portfolio = {
        let live = live_state_rx.borrow();
        Portfolio {
            balance: account_balance,
            open_position_count: live.positions.len() as u32,
            daily_pnl_r: live.daily_pnl_r,
            positions: live.positions.clone(),
        }
    };
    let sized_qty = risk_strategy.size(&trade_signal, &portfolio);

    let qty = sized_qty.to_u64().unwrap_or(0);
    if qty == 0 {
        // 슬롯 포화 시 교체 후보 확인
        let replace_target = risk_strategy.replacement_candidate(&trade_signal, &portfolio);
        if let Some(ref target_symbol) = replace_target {
            let replace_qty = portfolio
                .positions
                .iter()
                .find(|p| &p.symbol == target_symbol)
                .map(|p| p.qty.unsigned_abs())
                .unwrap_or(0);
            if replace_qty > 0 {
                tracing::info!(
                    "[{}] 🔄 포지션 교체: {} → {} (신규 신호 score={})",
                    market.label(),
                    target_symbol,
                    symbol,
                    setup_score
                );
                let sell_req = OrderRequest {
                    symbol: target_symbol.clone(),
                    side: Side::Sell,
                    qty: replace_qty,
                    price: None,
                    atr: None,
                    exchange_code: exchange_code.clone(),
                    strength: None,
                    is_short: false,
                    max_holding_days: 0,
                };
                let _ = order_tx.send(sell_req).await;
                log_signal(
                    &db_pool,
                    &symbol,
                    setup_score,
                    Some(direction_str),
                    Some(trade_signal.strength),
                    "replace_pending",
                    Some(target_symbol),
                    &regime_str,
                )
                .await;
                return;
            }
        }

        tracing::info!(
            market = %market.label(),
            symbol = %symbol,
            balance = %account_balance,
            open_positions = portfolio.open_position_count,
            held_symbols = %portfolio.positions.iter().map(|p| p.symbol.as_str()).collect::<Vec<_>>().join(","),
            setup_score,
            strength = trade_signal.strength,
            "수량 산출 0 — 잔고 부족 또는 리스크 제한으로 주문 skip"
        );
        activity.record_eval(market.label(), &symbol, score, "skip", "qty_zero");
        log_signal(
            &db_pool,
            &symbol,
            setup_score,
            Some(direction_str),
            Some(trade_signal.strength),
            "skip",
            Some("qty_zero"),
            &regime_str,
        )
        .await;
        return;
    }

    // 최소 주문 금액 체크: TWAP 5분할 기준 슬라이스당 $10 이상 → 총합 $50 이상
    const MIN_ORDER_NOTIONAL: &str = "50";
    let notional = Decimal::from(qty) * current_price;
    if notional
        < Decimal::from_str_exact(MIN_ORDER_NOTIONAL).unwrap_or(rust_decimal_macros::dec!(50))
    {
        tracing::info!(
            market = %market.label(),
            symbol = %symbol,
            notional = %notional,
            "주문 금액 미달 — skip (notional < $50, TWAP 슬라이스당 $10 미만)"
        );
        activity.record_eval(market.label(), &symbol, score, "skip", "min_notional");
        log_signal(
            &db_pool,
            &symbol,
            setup_score,
            Some(direction_str),
            Some(trade_signal.strength),
            "skip",
            Some("min_notional"),
            &regime_str,
        )
        .await;
        return;
    }

    activity.record_eval(market.label(), &symbol, score, "order", &regime_str);
    log_signal(
        &db_pool,
        &symbol,
        setup_score,
        Some(direction_str),
        Some(trade_signal.strength),
        "enter",
        None,
        &regime_str,
    )
    .await;
    if let Some(nc) = notion {
        let row = crate::notion::SignalEvalRow {
            timestamp: chrono::Utc::now().to_rfc3339(),
            symbol: symbol.clone(),
            score,
            market: market.label().to_string(),
            regime: format!("{:?}", regime),
            action: "order".into(),
            strategy: strategy_id.clone(),
        };
        tokio::spawn(async move {
            let client = nc.read().await;
            let _ = client.add_signal_eval(&row).await;
        });
    }

    let bot_state = summary.read().unwrap().bot_state.clone();
    if matches!(bot_state, BotState::Active) {
        tracing::info!(
            "[{}] 📤 주문 전송: {} {}주 @ ~{} ({:?})",
            market.label(),
            symbol,
            qty,
            current_price,
            regime
        );
        let (order_side, is_short) = match trade_signal.direction {
            Direction::Short => (Side::Sell, true),
            _ => (Side::Buy, false),
        };
        let req = OrderRequest {
            symbol: symbol.clone(),
            side: order_side,
            qty,
            price: None,
            atr: Some(trade_signal.atr),
            exchange_code,
            strength: Some(trade_signal.strength),
            is_short,
            max_holding_days: strategy.holding_days_target,
        };
        if order_tx.send(req).await.is_ok() {
            last_order_sent
                .lock()
                .unwrap()
                .insert(symbol.clone(), Instant::now());
        }
    } else {
        tracing::info!(
            "[{}] 🔍 평가 전용: {} {}주 @ ~{} (봇 비활성 — 실제 주문 없음)",
            market.label(),
            symbol,
            qty,
            current_price
        );
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_signal_task(
    mut tick_rx: broadcast::Receiver<TickData>,
    mut quote_rx: mpsc::Receiver<QuoteSnapshot>,
    order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: watch::Receiver<MarketRegime>,
    db_pool: sqlx::SqlitePool,
    adapter: Arc<dyn MarketAdapter>,
    mut watchlist_rx: watch::Receiver<WatchlistSet>,
    summary: Arc<StdRwLock<crate::state::MarketSummary>>,
    signal_cfg: crate::config::SignalConfig,
    entry_blackout_close_mins: u32,
    strategies: Vec<crate::config::StrategyProfile>,
    activity: crate::shared::activity::ActivityLog,
    notion: Option<Arc<TokioRwLock<crate::notion::NotionClient>>>,
    live_state_rx: watch::Receiver<crate::state::MarketLiveState>,
    signal_strategy: Arc<dyn SignalStrategy>,
    qual_strategy: Arc<dyn QualificationStrategy>,
    risk_strategy: Arc<dyn RiskStrategy>,
    token: tokio_util::sync::CancellationToken,
) {
    let eval_sem = Arc::new(tokio::sync::Semaphore::new(20));
    let mut state = SignalState {
        candles: HashMap::new(),
        completed: HashMap::new(),
        latest_quotes: HashMap::new(),
        watchlist: WatchlistSet::default(),
        candle_start: HashMap::new(),
        cached_balance: None,
        symbol_exchange: HashMap::new(),
        pending_symbols: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        last_order_sent: Arc::new(std::sync::Mutex::new(HashMap::new())),
    };
    let candle_interval = Duration::from_secs(signal_cfg.candle_interval_secs);
    let initial_wl = watchlist_rx.borrow().clone();
    state.watchlist = initial_wl.clone();
    for sym in initial_wl.all_unique() {
        state.candles.insert(sym.clone(), CandleAccumulator::new());
    }

    let market_id = adapter.market_id();

    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            Ok(_) = watchlist_rx.changed() => {
                let wl_set = watchlist_rx.borrow().clone();
                let new_wl = wl_set.all_unique();
                let new_syms: Vec<String> = new_wl.iter().filter(|s| !state.watchlist.all_unique().contains(s)).cloned().collect();
                state.watchlist = wl_set;
                if !new_syms.is_empty() {
                    let (exch_map, _bad): (HashMap<String, String>, std::collections::HashSet<String>) = seed_symbols(&new_syms, adapter.as_ref(), &db_pool, false).await;
                    state.symbol_exchange.extend(exch_map);
                }
                for sym in &new_wl {
                    if !state.candles.contains_key(sym) { state.candles.insert(sym.clone(), CandleAccumulator::new()); }
                }
            }
            Ok(tick) = tick_rx.recv() => {
                if state.watchlist.all_unique().is_empty() { state.watchlist = watchlist_rx.borrow().clone(); }
                if !state.watchlist.all_unique().contains(&tick.symbol) { continue; }
                let candle = state.candles.entry(tick.symbol.clone()).or_insert_with(CandleAccumulator::new);
                candle.push(tick.clone());
                if state.candle_start.entry(tick.symbol.clone()).or_insert_with(Instant::now).elapsed() >= candle_interval && candle.is_signal_eligible() {
                    let entry_cutoff_utc = entry_cutoff_utc(adapter.as_ref(), entry_blackout_close_mins);
                    if is_entry_blackout(chrono::Utc::now(), entry_cutoff_utc) {
                        tracing::debug!("[{market_id}] 장마감 진입금지 구간 캔들 → 신호 평가 skip ({})", tick.symbol);
                        candle.reset();
                        state.candle_start.insert(tick.symbol.clone(), Instant::now());
                        continue;
                    }
                    tracing::info!("[{market_id}] 🕯️ {} O={} H={} L={} C={} V={}", tick.symbol, candle.open, candle.high, candle.low, candle.close, candle.volume);
                    let completed_candle = CompletedCandle { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume, ts: tick.timestamp };
                    let cq = state.completed.entry(tick.symbol.clone()).or_default();
                    cq.push_back(completed_candle); if cq.len() > MAX_COMPLETED_CANDLES { cq.pop_front(); }

                    if state.cached_balance.map(|(_, t)| t.elapsed().as_secs() >= BALANCE_CACHE_TTL_SECS).unwrap_or(true) {
                        match adapter.balance().await {
                            Ok(resp) => { state.cached_balance = Some((resp.available_cash, Instant::now())); }
                            Err(e) => {
                                tracing::warn!(market = %market_id, "SignalTask: balance() failed (using 0, retry in {BALANCE_CACHE_TTL_SECS}s): {e}");
                                // Store sentinel so we don't hammer the API on every candle
                                state.cached_balance = Some((Decimal::ZERO, Instant::now()));
                            }
                        }
                    }
                    let account_balance = state.cached_balance.map(|(v, _)| v).unwrap_or(Decimal::ZERO);
                    {
                        let los = state.last_order_sent.lock().unwrap();
                        if los.get(&tick.symbol)
                            .map(|t| t.elapsed() < Duration::from_secs(300))
                            .unwrap_or(false)
                        {
                            tracing::debug!("[{market_id}] {} 5분 쿨다운 중 → skip", tick.symbol);
                            candle.reset();
                            state.candle_start.insert(tick.symbol.clone(), Instant::now());
                            continue;
                        }
                    }
                    { let mut ps = state.pending_symbols.lock().unwrap(); if ps.contains(&tick.symbol) { candle.reset(); continue; } ps.insert(tick.symbol.clone()); }
                    let sym_for_eval = tick.symbol.clone(); let sym_for_guard = tick.symbol.clone();
                    let pool = db_pool.clone(); let order_tx = order_tx.clone(); let regime = regime_rx.borrow().clone();
                    let completed_snap = cq.iter().cloned().collect::<Vec<_>>(); let quote_snap = state.latest_quotes.get(&sym_for_eval).cloned();
                    let summary_clone = summary.clone();
                    let ex_code = state.symbol_exchange.get(&sym_for_eval).cloned();
                    let ps_clone = Arc::clone(&state.pending_symbols);
                    let los_clone = Arc::clone(&state.last_order_sent);
                    let permit = eval_sem.clone().acquire_owned().await.unwrap();
                    let act = activity.clone(); let strategies_snap = strategies.clone(); let notion_clone = notion.clone();
                    let sig_strat = Arc::clone(&signal_strategy);
                    let q_strat = Arc::clone(&qual_strategy);
                    let r_strat = Arc::clone(&risk_strategy);
                    let live_rx = live_state_rx.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        struct PendingGuard(Arc<std::sync::Mutex<std::collections::HashSet<String>>>, String);
                        impl Drop for PendingGuard { fn drop(&mut self) { self.0.lock().unwrap().remove(&self.1); } }
                        let _guard = PendingGuard(ps_clone, sym_for_guard);
                        for strategy in strategies_snap {
                            evaluate_and_maybe_order(SignalContext {
                                symbol: sym_for_eval.clone(), market: market_id, entry_cutoff_utc, db_pool: pool.clone(), order_tx: order_tx.clone(), regime: regime.clone(),
                                completed: completed_snap.clone(), quote: quote_snap.clone(),
                                summary: summary_clone.clone(), account_balance, exchange_code: ex_code.clone(),
                                strategy, activity: act.clone(), notion: notion_clone.clone(),
                                signal_strategy: Arc::clone(&sig_strat), qual_strategy: Arc::clone(&q_strat), risk_strategy: Arc::clone(&r_strat),
                                live_state_rx: live_rx.clone(),
                                last_order_sent: Arc::clone(&los_clone),
                            }).await;
                        }
                    });
                    candle.reset(); state.candle_start.insert(tick.symbol.clone(), Instant::now());
                }
            }
            snap = quote_rx.recv() => if let Some(s) = snap { state.latest_quotes.insert(s.symbol.clone(), s); }
        }
    }
}

pub async fn seed_symbols(
    wl: &[String],
    adapter: &dyn MarketAdapter,
    pool: &sqlx::SqlitePool,
    force: bool,
) -> (HashMap<String, String>, std::collections::HashSet<String>) {
    let mut exch_map = HashMap::new();
    let mut bad = std::collections::HashSet::new();
    let market_id = adapter.market_id();

    for sym in wl {
        let exch = if market_id.is_kr() {
            "J".to_string()
        } else if sym.len() <= 3 {
            "NYSE".to_string()
        } else {
            "NASD".to_string()
        };

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM daily_ohlc WHERE symbol = ? AND date != '0000-00-00'",
        )
        .bind(sym)
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        if !force && count >= 30 {
            exch_map.insert(sym.clone(), exch);
            continue;
        }
        match adapter.daily_chart(sym, 150).await {
            Ok(bars) => {
                let bar_count = bars.len();
                let symbol_name = bars.first().and_then(|b| b.symbol_name.clone());
                for b in bars {
                    let date_str = b.date.format("%Y%m%d").to_string();
                    let _ = sqlx::query("INSERT OR REPLACE INTO daily_ohlc (symbol, name, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
                        .bind(sym).bind(symbol_name.clone()).bind(date_str).bind(b.open.to_string()).bind(b.high.to_string()).bind(b.low.to_string()).bind(b.close.to_string()).bind(b.volume.to_string()).execute(pool).await;
                }

                // If name is still missing, try to fetch it via current_price to update DB
                if symbol_name.is_none() {
                    let _ = adapter.current_price(sym).await;
                }

                let sym_label = match &symbol_name {
                    Some(n) if !n.is_empty() => format!("{n}({sym})"),
                    _ => sym.clone(),
                };
                tracing::info!("히스토리 로드: {sym_label} ({bar_count}봉)");
                exch_map.insert(sym.clone(), exch);
            }
            Err(_) => {
                bad.insert(sym.clone());
            }
        }
    }
    (exch_map, bad)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BotError;
    use crate::market::{
        MarketId, MarketTiming, PollOutcome, UnifiedBalance, UnifiedCandleBar, UnifiedDailyBar,
        UnifiedOrderHistoryItem, UnifiedOrderRequest, UnifiedOrderResult, UnifiedUnfilledOrder,
    };
    use crate::strategy::{Direction, TradeSignal};
    use crate::types::FillInfo;
    use async_trait::async_trait;
    use rust_decimal_macros::dec;

    struct MockAdapter {
        market_id: MarketId,
        daily_bars: Vec<UnifiedDailyBar>,
        fail_daily_chart: bool,
    }

    #[async_trait]
    impl MarketAdapter for MockAdapter {
        fn market_id(&self) -> MarketId {
            self.market_id
        }
        fn name(&self) -> &'static str {
            "Mock"
        }
        async fn place_order(
            &self,
            _: UnifiedOrderRequest,
        ) -> Result<UnifiedOrderResult, BotError> {
            unimplemented!()
        }
        async fn cancel_order(&self, _: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
            unimplemented!()
        }
        async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
            Ok(vec![])
        }
        async fn order_history(
            &self,
            _: &str,
            _: &str,
        ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
            Ok(vec![])
        }
        async fn poll_order_status(
            &self,
            _: &str,
            _: &str,
            _: u64,
        ) -> Result<PollOutcome, BotError> {
            unimplemented!()
        }
        async fn balance(&self) -> Result<UnifiedBalance, BotError> {
            Ok(UnifiedBalance {
                total_equity: dec!(100000),
                available_cash: dec!(100000),
                positions: vec![],
            })
        }
        async fn daily_chart(&self, _: &str, _: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
            if self.fail_daily_chart {
                Err(BotError::ApiError {
                    msg: "API Down".into(),
                })
            } else {
                Ok(self.daily_bars.clone())
            }
        }
        async fn intraday_candles(
            &self,
            _: &str,
            _: u32,
        ) -> Result<Vec<UnifiedCandleBar>, BotError> {
            Ok(vec![])
        }
        async fn current_price(&self, _: &str) -> Result<Decimal, BotError> {
            Ok(dec!(150.0))
        }
        fn market_timing(&self) -> MarketTiming {
            MarketTiming {
                is_open: true,
                mins_since_open: 10,
                mins_until_close: 10,
                mins_until_open: 0,
                is_holiday: false,
            }
        }

        async fn is_holiday(&self) -> Result<bool, BotError> {
            Ok(false)
        }

        fn market_open_utc(
            &self,
            date: chrono::NaiveDate,
        ) -> Option<chrono::DateTime<chrono::Utc>> {
            use chrono::TimeZone;
            use chrono_tz::America::New_York;
            let open_time = chrono::NaiveTime::from_hms_opt(9, 30, 0)?;
            New_York
                .from_local_datetime(&date.and_time(open_time))
                .earliest()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }

        fn market_close_utc(
            &self,
            date: chrono::NaiveDate,
        ) -> Option<chrono::DateTime<chrono::Utc>> {
            use chrono::TimeZone;
            use chrono_tz::America::New_York;
            let close_time = chrono::NaiveTime::from_hms_opt(16, 0, 0)?;
            New_York
                .from_local_datetime(&date.and_time(close_time))
                .earliest()
                .map(|dt| dt.with_timezone(&chrono::Utc))
        }

        fn local_today(&self) -> chrono::NaiveDate {
            use chrono_tz::America::New_York;
            chrono::Utc::now().with_timezone(&New_York).date_naive()
        }
    }

    struct AlwaysBuySignal;
    #[async_trait]
    impl SignalStrategy for AlwaysBuySignal {
        async fn evaluate(&self, ctx: &StrategySignalContext) -> Option<TradeSignal> {
            Some(TradeSignal {
                symbol: ctx.symbol.clone(),
                direction: Direction::Long,
                strength: 1.0,
                llm_verdict: None,
                entry_price: ctx.current_price,
                atr: dec!(2.5),
                setup_score: Some(80),
                regime: None,
            })
        }
    }

    struct AlwaysPassQual;
    impl QualificationStrategy for AlwaysPassQual {
        fn qualify(&self, _: &SignalCandidate) -> QualResult {
            QualResult::Pass
        }
    }

    struct FixedQtyRisk(Decimal);
    impl RiskStrategy for FixedQtyRisk {
        fn size(&self, _: &TradeSignal, _: &Portfolio) -> Decimal {
            self.0
        }
    }

    async fn setup_test_db() -> sqlx::SqlitePool {
        let pool = sqlx::SqlitePool::connect(":memory:").await.unwrap();
        sqlx::query("CREATE TABLE daily_ohlc (symbol TEXT, name TEXT, date TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, PRIMARY KEY(symbol, date))")
            .execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE signal_log (symbol TEXT, setup_score INTEGER, rule_direction TEXT, rule_strength REAL, action TEXT, llm_block_reason TEXT, regime TEXT, logged_at TEXT)")
            .execute(&pool).await.unwrap();
        pool
    }

    #[test]
    fn test_entry_blackout_after_cutoff() {
        let now = chrono::Utc::now();
        assert!(is_entry_blackout(
            now,
            Some(now - chrono::Duration::minutes(1))
        ));
        assert!(!is_entry_blackout(
            now,
            Some(now + chrono::Duration::minutes(1))
        ));
        assert!(!is_entry_blackout(now, None));
    }

    #[tokio::test]
    async fn test_evaluate_skips_orders_in_entry_blackout() {
        let db = setup_test_db().await;
        let (order_tx, mut order_rx) = mpsc::channel(1);
        let (_live_tx, live_state_rx) = watch::channel(crate::state::MarketLiveState::default());
        let summary = Arc::new(StdRwLock::new(crate::state::MarketSummary::new()));
        summary.write().unwrap().bot_state = crate::state::BotState::Active;

        evaluate_and_maybe_order(SignalContext {
            symbol: "GOOGL".into(),
            market: MarketId::Us,
            entry_cutoff_utc: Some(chrono::Utc::now() - chrono::Duration::minutes(1)),
            db_pool: db.clone(),
            order_tx,
            regime: MarketRegime::Trending,
            completed: vec![CompletedCandle {
                open: dec!(100),
                high: dec!(102),
                low: dec!(99),
                close: dec!(101),
                volume: 1000,
                ts: chrono::Utc::now(),
            }],
            quote: None,
            summary,
            account_balance: dec!(100000),
            exchange_code: Some("NASD".into()),
            strategy: crate::config::StrategyProfile {
                id: "test".into(),
                allocation_pct: dec!(1.0),
                setup_score_min: 60,
                regime_filter: true,
                aggressive_mode: false,
                holding_days_target: 5,
            },
            activity: crate::shared::activity::ActivityLog::new(),
            notion: None,
            signal_strategy: Arc::new(AlwaysBuySignal),
            qual_strategy: Arc::new(AlwaysPassQual),
            risk_strategy: Arc::new(FixedQtyRisk(dec!(10))),
            live_state_rx,
            last_order_sent: Arc::new(std::sync::Mutex::new(HashMap::new())),
        })
        .await;

        assert!(order_rx.try_recv().is_err());
        let reason: String =
            sqlx::query_scalar("SELECT llm_block_reason FROM signal_log WHERE symbol = 'GOOGL'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(reason, "entry_blackout_close");
    }

    #[tokio::test]
    async fn test_seed_symbols_writes_to_db_us() {
        let db = setup_test_db().await;
        let adapter = MockAdapter {
            market_id: MarketId::Us,
            daily_bars: vec![UnifiedDailyBar {
                symbol_name: None,
                date: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),

                open: dec!(100),
                high: dec!(110),
                low: dec!(90),
                close: dec!(105),
                volume: 1000,
            }],
            fail_daily_chart: false,
        };

        // AAPL (4) -> NASD, IBM (3) -> NYSE
        let symbols = vec!["AAPL".to_string(), "IBM".to_string()];
        let (exch_map, bad) = seed_symbols(&symbols, &adapter, &db, false).await;

        assert_eq!(bad.len(), 0);
        assert_eq!(exch_map.get("AAPL").unwrap(), "NASD");
        assert_eq!(exch_map.get("IBM").unwrap(), "NYSE");

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM daily_ohlc WHERE symbol = 'AAPL'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(count, 1);
        let count2: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM daily_ohlc WHERE symbol = 'IBM'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(count2, 1);
    }

    #[tokio::test]
    async fn test_seed_symbols_writes_to_db_kr() {
        let db = setup_test_db().await;
        let adapter = MockAdapter {
            market_id: MarketId::Kr,
            daily_bars: vec![UnifiedDailyBar {
                symbol_name: None,
                date: chrono::NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),

                open: dec!(50000),
                high: dec!(51000),
                low: dec!(49000),
                close: dec!(50500),
                volume: 1000000,
            }],
            fail_daily_chart: false,
        };

        let symbols = vec!["005930".to_string()];
        let (exch_map, bad) = seed_symbols(&symbols, &adapter, &db, false).await;

        assert_eq!(bad.len(), 0);
        assert_eq!(exch_map.get("005930").unwrap(), "J");

        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM daily_ohlc WHERE symbol = '005930'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn test_seed_symbols_handles_failure() {
        let db = setup_test_db().await;
        let adapter = MockAdapter {
            market_id: MarketId::Us,
            daily_bars: vec![],
            fail_daily_chart: true,
        };

        let symbols = vec!["MSFT".to_string()];
        let (exch_map, bad) = seed_symbols(&symbols, &adapter, &db, false).await;

        assert_eq!(bad.len(), 1);
        assert!(bad.contains("MSFT"));
        assert!(!exch_map.contains_key("MSFT"));
    }

    #[tokio::test]
    async fn test_strategy_delegation_logic() {
        // Use Mock strategies to resolve dead_code warnings and verify logic
        let _db = setup_test_db().await;
        let sig_strat = AlwaysBuySignal;
        let qual_strat = AlwaysPassQual;
        let risk_strat = FixedQtyRisk(dec!(50));

        let ctx = StrategySignalContext {
            symbol: "TSLA".into(),
            market: MarketId::Us,
            candles: vec![],
            daily_bars: vec![],
            quote: None,
            current_price: dec!(200.0),
            rolling_high: dec!(210.0),
            account_balance: dec!(10000),
            regime: MarketRegime::Trending,
            setup_score_min: 60,
            regime_filter: true,
        };

        // 1. Signal
        let signal = sig_strat.evaluate(&ctx).await.unwrap();
        assert_eq!(signal.strength, 1.0);
        assert_eq!(signal.atr, dec!(2.5));

        // 2. Qualification
        let candidate = SignalCandidate {
            signal: signal.clone(),
            regime: MarketRegime::Trending,
            setup_score: signal.setup_score,
            daily_change_pct: 0.0,
            has_earnings_event: false,
            has_fomc_today: false,
        };
        let qual = qual_strat.qualify(&candidate);
        assert_eq!(qual, QualResult::Pass);

        // 3. Risk Sizing
        let portfolio = Portfolio {
            balance: dec!(10000),
            open_position_count: 0,
            daily_pnl_r: 0.0,
            positions: vec![],
        };
        let qty = risk_strat.size(&signal, &portfolio);
        assert_eq!(qty, dec!(50));
    }

    #[test]
    fn test_atr_data_lineage_and_stop_calculation() {
        let atr_value = dec!(2.5);
        let entry_price = dec!(150.0);

        let signal = TradeSignal {
            symbol: "NVDA".into(),
            direction: Direction::Long,
            strength: 1.0,
            llm_verdict: None,
            entry_price,
            atr: atr_value,
            setup_score: None,
            regime: None,
        };

        let req = OrderRequest {
            symbol: signal.symbol.clone(),
            side: Side::Buy,
            qty: 10,
            price: None,
            atr: Some(signal.atr),
            exchange_code: None,
            strength: None,
            is_short: false,
            max_holding_days: 5,
        };
        assert_eq!(req.atr, Some(atr_value));

        let fill = FillInfo {
            order_id: "ord-1".into(),
            symbol: "NVDA".into(),
            filled_qty: 10,
            filled_price: entry_price,
            atr: req.atr,
            exchange_code: None,
            fatal: false,
            max_holding_days: 5,
        };

        let pos_cfg = crate::config::PositionConfig {
            stop_atr_multiplier: dec!(1.5),
            profit_target_1_atr: dec!(2.0),
            profit_target_2_atr: dec!(4.0),
            ..Default::default()
        };

        let atr = fill.atr.unwrap_or(Decimal::ONE);
        let stop_price = fill.filled_price - atr * pos_cfg.stop_atr_multiplier;
        let pt1 = fill.filled_price + atr * pos_cfg.profit_target_1_atr;

        assert_eq!(stop_price, dec!(146.25));
        assert_eq!(pt1, dec!(155.0));
    }

    #[tokio::test]
    async fn cooldown_prevents_second_order() {
        use std::collections::HashMap;
        use tokio::time::Duration;

        let los: Arc<std::sync::Mutex<HashMap<String, Instant>>> =
            Arc::new(std::sync::Mutex::new(HashMap::new()));

        // 10초 전 주문 기록 → 쿨다운 중이어야 함
        los.lock()
            .unwrap()
            .insert("AAPL".to_string(), Instant::now() - Duration::from_secs(10));
        let in_cooldown = los
            .lock()
            .unwrap()
            .get("AAPL")
            .map(|t| t.elapsed() < Duration::from_secs(300))
            .unwrap_or(false);
        assert!(in_cooldown, "10초 경과 → 아직 쿨다운 중이어야 함");

        // 400초 전 주문 기록 → 쿨다운 만료여야 함
        los.lock().unwrap().insert(
            "AAPL".to_string(),
            Instant::now() - Duration::from_secs(400),
        );
        let expired = los
            .lock()
            .unwrap()
            .get("AAPL")
            .map(|t| t.elapsed() < Duration::from_secs(300))
            .unwrap_or(false);
        assert!(!expired, "400초 경과 → 쿨다운 만료여야 함");
    }
}
