use crate::market::MarketAdapter;
use crate::pipeline::{QuoteSnapshot, TickData};
use crate::state::BotState;
use crate::strategy::{
    Portfolio, QualResult, QualificationStrategy, RiskStrategy, SignalCandidate,
    SignalContext as StrategySignalContext, SignalStrategy,
};
use crate::types::MarketRegime;
use crate::types::{OrderRequest, Side, WatchlistSet};
use kis_api::CandleBar;
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
}

struct SignalContext {
    symbol: String,
    market: String,
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
}

async fn evaluate_and_maybe_order(ctx: SignalContext) {
    let SignalContext {
        symbol,
        market,
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
    } = ctx;
    use rust_decimal::prelude::ToPrimitive;

    let strategy_id = strategy.id.clone();

    let last = match completed.last() {
        Some(c) => c,
        None => return,
    };

    let current_price = last.close;
    let rolling_high = completed
        .iter()
        .map(|c| c.high)
        .fold(Decimal::MIN, Decimal::max);

    let candles: Vec<CandleBar> = completed
        .iter()
        .map(|c| CandleBar {
            date: c.ts.format("%Y%m%d").to_string(),
            open: c.open,
            high: c.high,
            low: c.low,
            close: c.close,
            volume: Decimal::from(c.volume),
        })
        .collect();

    let strategy_ctx = StrategySignalContext {
        symbol: symbol.clone(),
        market: market.clone(),
        candles,
        quote,
        current_price,
        rolling_high,
        account_balance,
        regime: regime.clone(),
        setup_score_min: strategy.setup_score_min,
        regime_filter: strategy.regime_filter,
    };

    let trade_signal = match signal_strategy.evaluate(&strategy_ctx, &db_pool).await {
        Some(sig) => sig,
        None => {
            activity.record_eval(&market, &symbol, 0, "skip", "no_signal");
            return;
        }
    };

    let score = (trade_signal.strength * 100.0) as i32;

    let candidate = SignalCandidate {
        signal: trade_signal.clone(),
        regime: regime.clone(),
        setup_score: trade_signal.setup_score,
    };
    let qual_result = qual_strategy.qualify(&candidate);

    if let QualResult::Block { reason } = qual_result {
        activity.record_eval(&market, &symbol, score, "blocked", &reason);
        return;
    }

    let portfolio = {
        let live = live_state_rx.borrow();
        Portfolio {
            balance: account_balance,
            open_position_count: live.positions.len() as u32,
            daily_pnl_r: live.daily_pnl_r,
        }
    };
    let sized_qty = risk_strategy.size(&trade_signal, &portfolio);

    let qty = sized_qty.to_u64().unwrap_or(0);
    if qty == 0 {
        activity.record_eval(&market, &symbol, score, "skip", "qty_zero");
        return;
    }

    activity.record_eval(&market, &symbol, score, "order", &format!("{:?}", regime));
    if let Some(nc) = notion {
        let row = crate::notion::SignalEvalRow {
            timestamp: chrono::Utc::now().to_rfc3339(),
            symbol: symbol.clone(),
            score,
            market: market.clone(),
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
        let req = OrderRequest {
            symbol: symbol.clone(),
            side: Side::Buy,
            qty,
            price: None,
            atr: Some(trade_signal.quantity),
            exchange_code,
            strength: Some(trade_signal.strength),
        };
        let _ = order_tx.send(req).await;
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
    };
    let candle_interval = Duration::from_secs(signal_cfg.candle_interval_secs);
    let initial_wl = watchlist_rx.borrow().clone();
    state.watchlist = initial_wl.clone();
    for sym in initial_wl.all_unique() {
        state.candles.insert(sym.clone(), CandleAccumulator::new());
    }

    let market_label = adapter.market_id().label().to_string();

    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            Ok(_) = watchlist_rx.changed() => {
                let wl_set = watchlist_rx.borrow().clone();
                let new_wl = wl_set.all_unique();
                let new_syms: Vec<String> = new_wl.iter().filter(|s| !state.watchlist.all_unique().contains(s)).cloned().collect();
                state.watchlist = wl_set;
                if !new_syms.is_empty() {
                    let mut news_cache = HashMap::new();
                    let (exch_map, _bad): (HashMap<String, String>, std::collections::HashSet<String>) = seed_symbols(&new_syms, adapter.as_ref(), &db_pool, &mut news_cache, false).await;
                    state.symbol_exchange.extend(exch_map);
                }
                for sym in &new_wl { if !state.candles.contains_key(sym) { state.candles.insert(sym.clone(), CandleAccumulator::new()); } }
            }
            Ok(tick) = tick_rx.recv() => {
                if state.watchlist.all_unique().is_empty() { state.watchlist = watchlist_rx.borrow().clone(); }
                if !state.watchlist.all_unique().contains(&tick.symbol) { continue; }
                let candle = state.candles.entry(tick.symbol.clone()).or_insert_with(CandleAccumulator::new);
                candle.push(tick.clone());
                if state.candle_start.entry(tick.symbol.clone()).or_insert_with(Instant::now).elapsed() >= candle_interval && candle.is_signal_eligible() {
                    tracing::info!(symbol = %tick.symbol, "🕯️ 캔들 완성 (분석 시작)");
                    let completed_candle = CompletedCandle { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume, ts: tick.timestamp };
                    let cq = state.completed.entry(tick.symbol.clone()).or_default();
                    cq.push_back(completed_candle); if cq.len() > MAX_COMPLETED_CANDLES { cq.pop_front(); }

                    if state.cached_balance.map(|(_, t)| t.elapsed().as_secs() >= BALANCE_CACHE_TTL_SECS).unwrap_or(true) {
                        match adapter.balance().await {
                            Ok(resp) => { state.cached_balance = Some((resp.available_cash, Instant::now())); }
                            Err(e) => { tracing::error!(market = %market_label, "SignalTask: balance() failed: {e}"); }
                        }
                    }
                    let account_balance = state.cached_balance.map(|(v, _)| v).unwrap_or(Decimal::ZERO);
                    { let mut ps = state.pending_symbols.lock().unwrap(); if ps.contains(&tick.symbol) { candle.reset(); continue; } ps.insert(tick.symbol.clone()); }
                    let sym_for_eval = tick.symbol.clone(); let sym_for_guard = tick.symbol.clone();
                    let pool = db_pool.clone(); let order_tx = order_tx.clone(); let regime = regime_rx.borrow().clone();
                    let completed_snap = cq.iter().cloned().collect::<Vec<_>>(); let quote_snap = state.latest_quotes.get(&sym_for_eval).cloned();
                    let summary_clone = summary.clone();
                    let ex_code = state.symbol_exchange.get(&sym_for_eval).cloned();
                    let ps_clone = Arc::clone(&state.pending_symbols);
                    let permit = eval_sem.clone().acquire_owned().await.unwrap();
                    let act = activity.clone(); let strategies_snap = strategies.clone(); let notion_clone = notion.clone();
                    let sig_strat = Arc::clone(&signal_strategy);
                    let q_strat = Arc::clone(&qual_strategy);
                    let r_strat = Arc::clone(&risk_strategy);
                    let live_rx = live_state_rx.clone();
                    let market_name_inner = market_label.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        struct PendingGuard(Arc<std::sync::Mutex<std::collections::HashSet<String>>>, String);
                        impl Drop for PendingGuard { fn drop(&mut self) { self.0.lock().unwrap().remove(&self.1); } }
                        let _guard = PendingGuard(ps_clone, sym_for_guard);
                        for strategy in strategies_snap {
                            evaluate_and_maybe_order(SignalContext {
                                symbol: sym_for_eval.clone(), market: market_name_inner.clone(), db_pool: pool.clone(), order_tx: order_tx.clone(), regime: regime.clone(),
                                completed: completed_snap.clone(), quote: quote_snap.clone(),
                                summary: summary_clone.clone(), account_balance, exchange_code: ex_code.clone(),
                                strategy, activity: act.clone(), notion: notion_clone.clone(),
                                signal_strategy: Arc::clone(&sig_strat), qual_strategy: Arc::clone(&q_strat), risk_strategy: Arc::clone(&r_strat),
                                live_state_rx: live_rx.clone(),
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
    _news_cache: &mut HashMap<String, (bool, Instant)>,
    force: bool,
) -> (HashMap<String, String>, std::collections::HashSet<String>) {
    let mut exch_map = HashMap::new();
    let mut bad = std::collections::HashSet::new();
    let market_id = adapter.market_id();
    let default_exch = if market_id.is_kr() {
        "J".to_string()
    } else {
        "NASD".to_string()
    };

    for sym in wl {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM daily_ohlc WHERE symbol = ? AND date != '0000-00-00'",
        )
        .bind(sym)
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        if !force && count >= 30 {
            exch_map.insert(sym.clone(), default_exch.clone());
            continue;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        match adapter.daily_chart(sym, 150).await {
            Ok(bars) => {
                let bar_count = bars.len();
                for b in bars {
                    let date_str = b.date.format("%Y%m%d").to_string();
                    let _ = sqlx::query("INSERT OR REPLACE INTO daily_ohlc (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)")
                        .bind(sym).bind(date_str).bind(b.open.to_string()).bind(b.high.to_string()).bind(b.low.to_string()).bind(b.close.to_string()).bind(b.volume.to_string()).execute(pool).await;
                }
                tracing::info!(symbol = %sym, bars = bar_count, "History seeded successfully");
                exch_map.insert(sym.clone(), default_exch.clone());
            }
            Err(_) => {
                bad.insert(sym.clone());
            }
        }
    }
    (exch_map, bad)
}
