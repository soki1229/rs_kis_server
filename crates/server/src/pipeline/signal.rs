use crate::pipeline::{QuoteSnapshot, TickData};
use crate::state::BotState;
use crate::types::{OrderRequest, Side, WatchlistSet};
use kis_api::{DomesticExchange, KisApi, KisDomesticApi};
use crate::monitoring::alert::AlertRouter;
use crate::types::MarketRegime;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::AtomicU32;
use std::sync::Arc;
use std::sync::RwLock as StdRwLock;
use tokio::sync::{broadcast, mpsc, watch, RwLock as TokioRwLock};
use tokio::time::{Duration, Instant};

const MAX_COMPLETED_CANDLES: usize = 120;
const BALANCE_CACHE_TTL_SECS: u64 = 60;

#[derive(Debug, Clone)]
pub struct CompletedCandle {
    #[allow(dead_code)]
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    #[allow(dead_code)]
    pub volume: u64,
    #[allow(dead_code)]
    pub ts: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone)]
pub struct DailyBar {
    #[allow(dead_code)]
    pub date: String,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: Decimal,
}

struct CandleAccumulator {
    #[allow(dead_code)]
    symbol: String,
    open: Decimal,
    high: Decimal,
    low: Decimal,
    close: Decimal,
    volume: u64,
    count: usize,
}

impl CandleAccumulator {
    fn new(symbol: &str) -> Self {
        Self {
            symbol: symbol.to_string(),
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
    #[allow(dead_code)]
    rolling_highs: HashMap<String, VecDeque<(chrono::DateTime<chrono::Utc>, Decimal)>>,
    watchlist: WatchlistSet,
    candle_start: HashMap<String, Instant>,
    cached_balance: Option<(Decimal, Instant)>,
    symbol_exchange: HashMap<String, DomesticExchange>,
    us_exchange: HashMap<String, String>,
    pending_symbols: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    llm_fail_count: Arc<AtomicU32>,
}

struct SignalContext {
    symbol: String,
    market: String,
    db_pool: sqlx::SqlitePool,
    order_tx: mpsc::Sender<OrderRequest>,
    regime: MarketRegime,
    completed: Vec<CompletedCandle>,
    quote: Option<QuoteSnapshot>,
    #[allow(dead_code)]
    rolling_highs: Vec<(chrono::DateTime<chrono::Utc>, Decimal)>,
    summary: Arc<StdRwLock<crate::state::MarketSummary>>,
    mins_until_close: i64,
    account_balance: Decimal,
    exchange_code: Option<String>,
    risk_cfg: crate::config::RiskConfig,
    signal_cfg: crate::config::SignalConfig,
    strategy: crate::config::StrategyProfile,
    activity: crate::shared::activity::ActivityLog,
    notion: Option<Arc<TokioRwLock<crate::notion::NotionClient>>>,
    #[allow(dead_code)]
    alert: AlertRouter,
    #[allow(dead_code)]
    llm_sem: Arc<tokio::sync::Semaphore>,
    #[allow(dead_code)]
    llm_fail_count: Arc<AtomicU32>,
    #[allow(dead_code)]
    pending_count: Arc<AtomicU32>,
}

#[allow(dead_code)]
async fn get_stock_name(db: &sqlx::SqlitePool, symbol: &str) -> String {
    let name: Option<String> = sqlx::query_scalar(
        "SELECT name FROM daily_ohlc WHERE symbol = ? AND name IS NOT NULL LIMIT 1",
    )
    .bind(symbol)
    .fetch_optional(db)
    .await
    .unwrap_or(None);
    match name {
        Some(n) if !n.is_empty() => format!("{}({})", n, symbol),
        _ => symbol.to_string(),
    }
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
        rolling_highs: _,
        summary,
        mins_until_close,
        account_balance,
        exchange_code,
        risk_cfg,
        signal_cfg,
        strategy,
        activity,
        notion,
        alert: _,
        llm_sem: _,
        llm_fail_count: _,
        pending_count: _,
    } = ctx;
    use rust_decimal::prelude::ToPrimitive;
    use sqlx::Row;
    let strategy_id = strategy.id.clone();
    let rows = sqlx::query("SELECT date, open, high, low, close, volume FROM daily_ohlc WHERE symbol = ? AND date != '0000-00-00' ORDER BY date DESC LIMIT 30").bind(&symbol).fetch_all(&db_pool).await.unwrap_or_default();
    let min_history = if strategy.aggressive_mode { 1 } else { 15 };
    if rows.len() < min_history {
        let reason = format!("insufficient_data({}/{} days)", rows.len(), min_history);
        activity.record_eval(&market, &symbol, 0, "skip", &reason);
        if let Some(nc) = notion {
            let row = crate::notion::SignalEvalRow {
                timestamp: chrono::Utc::now().to_rfc3339(),
                symbol: symbol.clone(),
                score: 0,
                market: market.clone(),
                regime: format!("{:?}", regime),
                action: "skip".into(),
                strategy: format!("{}:{}", strategy_id, reason),
            };
            tokio::spawn(async move {
                let client = nc.read().await;
                let _ = client.add_signal_eval(&row).await;
            });
        }
        return;
    }
    let bars: Vec<DailyBar> = rows
        .iter()
        .map(|r| DailyBar {
            date: r.get(0),
            open: r.get::<String, _>(1).parse().unwrap_or(Decimal::ZERO),
            high: r.get::<String, _>(2).parse().unwrap_or(Decimal::ZERO),
            low: r.get::<String, _>(3).parse().unwrap_or(Decimal::ZERO),
            close: r.get::<String, _>(4).parse().unwrap_or(Decimal::ZERO),
            volume: r.get::<String, _>(5).parse().unwrap_or(Decimal::ZERO),
        })
        .collect();
    let atr_14 = compute_atr14(&bars)
        .unwrap_or_else(|| (bars[0].high - bars[0].low).max(bars[0].close * dec!(0.02)));
    let ma5 = if bars.len() >= 5 {
        bars[..5].iter().map(|b| b.close).sum::<Decimal>() / dec!(5)
    } else {
        bars.iter().map(|b| b.close).sum::<Decimal>() / Decimal::from(bars.len())
    };
    let ma20 = if bars.len() >= 20 {
        bars[..20].iter().map(|b| b.close).sum::<Decimal>() / dec!(20)
    } else {
        bars.iter().map(|b| b.close).sum::<Decimal>() / Decimal::from(bars.len())
    };
    let prev_close = if bars.len() > 1 {
        bars[1].close
    } else {
        bars[0].open
    };
    let avg_vol_20 = if bars.len() > 1 {
        let count = (bars.len() - 1).min(20);
        bars[1..1 + count].iter().map(|b| b.volume).sum::<Decimal>() / Decimal::from(count)
    } else {
        bars[0].volume
    };
    let last = match completed.last() {
        Some(c) => c,
        None => return,
    };
    let today_volume: Decimal = completed.iter().map(|c| Decimal::from(c.volume)).sum();
    let volume_ratio = if avg_vol_20.is_zero() {
        1.0
    } else {
        (today_volume / avg_vol_20).to_f64().unwrap_or(1.0) * (390.0 / 60.0)
    };
    let (bid_qty, ask_qty) = quote
        .as_ref()
        .map(|q| (q.bid_qty, q.ask_qty))
        .unwrap_or((1, 1));
    let bid_ask_imbalance = if ask_qty == 0 {
        1.0
    } else {
        bid_qty as f64 / ask_qty as f64
    };
    let daily_change_pct = if prev_close.is_zero() {
        0.0
    } else {
        ((last.close - prev_close) / prev_close * dec!(100))
            .to_f64()
            .unwrap_or(0.0)
    };

    let score = if strategy.aggressive_mode {
        let mut s = 40;
        if bid_ask_imbalance > 2.0 {
            s += 25;
        } else if bid_ask_imbalance > 1.5 {
            s += 15;
        }
        if matches!(regime, MarketRegime::Volatile) {
            s += 10;
        }
        let day_low = completed
            .iter()
            .map(|c| c.low)
            .fold(Decimal::MAX, Decimal::min);
        if last.close > day_low && (last.close - day_low) < (atr_14 * dec!(0.5)) {
            s += 15;
        }
        if last.close > ma5 {
            s += 10;
        }
        s
    } else {
        let input = crate::qualification::setup_score::SetupScoreInput {
            ma5_above_ma20: ma5 > ma20,
            volume_ratio: volume_ratio * (2.0 / signal_cfg.volume_ratio_threshold),
            recent_5min_volume_ratio: 1.0,
            bid_ask_imbalance,
            new_high_last_10min: last.high > ma5,
            daily_change_pct,
            entry_blackout_close_mins: 15,
            has_news_catalyst: false,
            mins_until_close,
            regime: regime.clone(),
        };
        crate::qualification::setup_score::calculate_setup_score(&input) as i32
    };

    activity.record_eval(
        &market,
        &symbol,
        score,
        if score >= 70 { "order" } else { "skip" },
        &format!("{:?}", regime),
    );
    if let Some(nc) = notion {
        let row = crate::notion::SignalEvalRow {
            timestamp: chrono::Utc::now().to_rfc3339(),
            symbol: symbol.clone(),
            score,
            market: market.clone(),
            regime: format!("{:?}", regime),
            action: if score >= 70 {
                "order".into()
            } else {
                "skip".into()
            },
            strategy: strategy_id.clone(),
        };
        tokio::spawn(async move {
            let client = nc.read().await;
            let _ = client.add_signal_eval(&row).await;
        });
    }
    if score >= 70 && mins_until_close > 10 {
        let bot_state = summary.read().unwrap().bot_state.clone();
        if matches!(bot_state, BotState::Active) {
            let qty = (account_balance * risk_cfg.risk_per_trade_pct / (atr_14 * dec!(2.0)))
                .to_u64()
                .unwrap_or(0);
            if qty > 0 {
                let req = OrderRequest {
                    symbol: symbol.clone(),
                    side: Side::Buy,
                    qty,
                    price: None,
                    atr: Some(atr_14),
                    exchange_code,
                };
                let _ = order_tx.send(req).await;
            }
        }
    }
}

pub fn compute_atr14(bars: &[DailyBar]) -> Option<Decimal> {
    if bars.len() < 15 {
        return None;
    }
    let mut trs = Vec::new();
    for i in 0..bars.len() - 1 {
        let h = bars[i].high;
        let l = bars[i].low;
        let pc = bars[i + 1].close;
        let tr = h.max(pc) - l.min(pc);
        trs.push(tr);
    }
    Some(trs.iter().take(14).sum::<Decimal>() / Decimal::from(14))
}

#[allow(clippy::too_many_arguments)]
pub async fn run_signal_task(
    mut tick_rx: broadcast::Receiver<TickData>,
    mut quote_rx: mpsc::Receiver<QuoteSnapshot>,
    order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: watch::Receiver<MarketRegime>,
    db_pool: sqlx::SqlitePool,
    client: Arc<dyn KisApi>,
    mut watchlist_rx: watch::Receiver<WatchlistSet>,
    summary: Arc<StdRwLock<crate::state::MarketSummary>>,
    alert: AlertRouter,
    pending_count: Arc<AtomicU32>,
    risk_cfg: crate::config::RiskConfig,
    signal_cfg: crate::config::SignalConfig,
    strategies: Vec<crate::config::StrategyProfile>,
    activity: crate::shared::activity::ActivityLog,
    notion: Option<Arc<TokioRwLock<crate::notion::NotionClient>>>,
    token: tokio_util::sync::CancellationToken,
) {
    let eval_sem = Arc::new(tokio::sync::Semaphore::new(20));
    let mut state = SignalState {
        candles: HashMap::new(),
        completed: HashMap::new(),
        latest_quotes: HashMap::new(),
        rolling_highs: HashMap::new(),
        watchlist: WatchlistSet::default(),
        candle_start: HashMap::new(),
        cached_balance: None,
        symbol_exchange: HashMap::new(),
        us_exchange: HashMap::new(),
        pending_symbols: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        llm_fail_count: Arc::new(AtomicU32::new(0)),
    };
    let llm_semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let candle_interval = Duration::from_secs(signal_cfg.candle_interval_secs);
    let initial_wl = watchlist_rx.borrow().clone();
    state.watchlist = initial_wl.clone();
    for sym in initial_wl.all_unique() {
        state
            .candles
            .insert(sym.clone(), CandleAccumulator::new(&sym));
    }

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
                    let (exch_map, _bad): (HashMap<String, String>, std::collections::HashSet<String>) = seed_symbols(&new_syms, &client, &db_pool, &mut news_cache, false).await;
                    state.us_exchange.extend(exch_map);
                }
                for sym in &new_wl { if !state.candles.contains_key(sym) { state.candles.insert(sym.clone(), CandleAccumulator::new(sym)); } }
            }
            Ok(tick) = tick_rx.recv() => {
                if state.watchlist.all_unique().is_empty() { state.watchlist = watchlist_rx.borrow().clone(); }
                if !state.watchlist.all_unique().contains(&tick.symbol) { continue; }
                let candle = state.candles.entry(tick.symbol.clone()).or_insert_with(|| CandleAccumulator::new(&tick.symbol));
                candle.push(tick.clone());
                if state.candle_start.entry(tick.symbol.clone()).or_insert_with(Instant::now).elapsed() >= candle_interval && candle.is_signal_eligible() {
                    tracing::info!(symbol = %tick.symbol, "🕯️ 캔들 완성 (분석 시작)");
                    let completed_candle = CompletedCandle { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume, ts: tick.timestamp };
                    let cq = state.completed.entry(tick.symbol.clone()).or_default();
                    cq.push_back(completed_candle); if cq.len() > MAX_COMPLETED_CANDLES { cq.pop_front(); }
                    let mins_until_close = { use chrono_tz::America::New_York; let et_now = chrono::Utc::now().with_timezone(&New_York);
                        let market_close = et_now.date_naive().and_hms_opt(16, 0, 0).unwrap();
                        let market_close_utc = chrono::NaiveDateTime::and_local_timezone(&market_close, New_York).unwrap().to_utc();
                        (market_close_utc - chrono::Utc::now()).num_minutes().max(0)
                    };
                    if state.cached_balance.map(|(_, t)| t.elapsed().as_secs() >= BALANCE_CACHE_TTL_SECS).unwrap_or(true) {
                        match client.balance().await {
                            Ok(resp) => { state.cached_balance = Some((resp.summary.purchase_amount, Instant::now())); }
                            Err(e) => { tracing::error!("UsSignalTask: balance() failed: {e}"); }
                        }
                    }
                    let account_balance = state.cached_balance.map(|(v, _)| v).unwrap_or(Decimal::ZERO);
                    { let mut ps = state.pending_symbols.lock().unwrap(); if ps.contains(&tick.symbol) { candle.reset(); continue; } ps.insert(tick.symbol.clone()); }
                    let sym_for_eval = tick.symbol.clone(); let sym_for_guard = tick.symbol.clone();
                    let pool = db_pool.clone(); let order_tx = order_tx.clone(); let regime = regime_rx.borrow().clone();
                    let completed_snap = cq.iter().cloned().collect::<Vec<_>>(); let quote_snap = state.latest_quotes.get(&sym_for_eval).cloned();
                    let summary_clone = summary.clone(); let sem = llm_semaphore.clone();
                    let pc = pending_count.clone(); let us_exchange_code = Some(state.us_exchange.get(&sym_for_eval).cloned().unwrap_or_else(|| "NASD".to_string()));
                    let rc = risk_cfg.clone(); let sc = signal_cfg.clone(); let ps_clone = Arc::clone(&state.pending_symbols);
                    let permit = eval_sem.clone().acquire_owned().await.unwrap();
                    let llm_fail_count = Arc::clone(&state.llm_fail_count); let act = activity.clone(); let strategies_snap = strategies.clone(); let notion_clone = notion.clone();
                    let alert_router = alert.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        struct PendingGuard(Arc<std::sync::Mutex<std::collections::HashSet<String>>>, String);
                        impl Drop for PendingGuard { fn drop(&mut self) { self.0.lock().unwrap().remove(&self.1); } }
                        let _guard = PendingGuard(ps_clone, sym_for_guard);
                        for strategy in strategies_snap {
                            evaluate_and_maybe_order(SignalContext {
                                symbol: sym_for_eval.clone(), market: "US".to_string(), db_pool: pool.clone(), order_tx: order_tx.clone(), regime: regime.clone(),
                                completed: completed_snap.clone(), quote: quote_snap.clone(), rolling_highs: vec![],
                                summary: summary_clone.clone(), mins_until_close, account_balance, exchange_code: us_exchange_code.clone(),
                                risk_cfg: rc.clone(), signal_cfg: sc.clone(), strategy, activity: act.clone(), notion: notion_clone.clone(),
                                alert: alert_router.clone(), llm_sem: sem.clone(), llm_fail_count: llm_fail_count.clone(), pending_count: pc.clone(),
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

#[allow(clippy::too_many_arguments)]
pub async fn run_kr_signal_task(
    mut tick_rx: broadcast::Receiver<TickData>,
    mut quote_rx: mpsc::Receiver<QuoteSnapshot>,
    order_tx: mpsc::Sender<OrderRequest>,
    regime_rx: watch::Receiver<MarketRegime>,
    db_pool: sqlx::SqlitePool,
    client: Arc<dyn KisDomesticApi>,
    mut watchlist_rx: watch::Receiver<WatchlistSet>,
    summary: Arc<StdRwLock<crate::state::MarketSummary>>,
    alert: AlertRouter,
    pending_count: Arc<AtomicU32>,
    risk_cfg: crate::config::RiskConfig,
    signal_cfg: crate::config::SignalConfig,
    strategies: Vec<crate::config::StrategyProfile>,
    activity: crate::shared::activity::ActivityLog,
    notion: Option<Arc<TokioRwLock<crate::notion::NotionClient>>>,
    token: tokio_util::sync::CancellationToken,
) {
    let eval_sem = Arc::new(tokio::sync::Semaphore::new(20));
    let mut state = SignalState {
        candles: HashMap::new(),
        completed: HashMap::new(),
        latest_quotes: HashMap::new(),
        rolling_highs: HashMap::new(),
        watchlist: WatchlistSet::default(),
        candle_start: HashMap::new(),
        cached_balance: None,
        symbol_exchange: HashMap::new(),
        us_exchange: HashMap::new(),
        pending_symbols: Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
        llm_fail_count: Arc::new(AtomicU32::new(0)),
    };
    let llm_semaphore = Arc::new(tokio::sync::Semaphore::new(1));
    let candle_interval = Duration::from_secs(signal_cfg.candle_interval_secs);
    let initial_wl = watchlist_rx.borrow().clone();
    state.watchlist = initial_wl.clone();
    for sym in initial_wl.all_unique() {
        state
            .candles
            .insert(sym.clone(), CandleAccumulator::new(&sym));
    }

    loop {
        tokio::select! {
            _ = token.cancelled() => return,
            Ok(_) = watchlist_rx.changed() => {
                let wl_set = watchlist_rx.borrow().clone();
                let new_wl = wl_set.all_unique();
                let new_syms: Vec<String> = new_wl.iter().filter(|s| !state.watchlist.all_unique().contains(s)).cloned().collect();
                state.watchlist = wl_set;
                if !new_syms.is_empty() {
                    let (ex_map, _bad): (HashMap<String, DomesticExchange>, std::collections::HashSet<String>) = seed_kr_symbols(&new_syms, &client, &db_pool, false).await;
                    state.symbol_exchange.extend(ex_map);
                }
                for sym in &new_wl { if !state.candles.contains_key(sym) { state.candles.insert(sym.clone(), CandleAccumulator::new(sym)); } }
            }
            Ok(tick) = tick_rx.recv() => {
                if state.watchlist.all_unique().is_empty() { state.watchlist = watchlist_rx.borrow().clone(); }
                if !state.watchlist.all_unique().contains(&tick.symbol) { continue; }
                let candle = state.candles.entry(tick.symbol.clone()).or_insert_with(|| CandleAccumulator::new(&tick.symbol));
                candle.push(tick.clone());
                if state.candle_start.entry(tick.symbol.clone()).or_insert_with(Instant::now).elapsed() >= candle_interval && candle.is_signal_eligible() {
                    tracing::info!(symbol = %tick.symbol, "🕯️ 캔들 완성 (분석 시작)");
                    let completed_candle = CompletedCandle { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume, ts: tick.timestamp };
                    let cq = state.completed.entry(tick.symbol.clone()).or_default();
                    cq.push_back(completed_candle); if cq.len() > MAX_COMPLETED_CANDLES { cq.pop_front(); }
                    let mins_until_close = { use chrono_tz::Asia::Seoul; let kst_now = chrono::Utc::now().with_timezone(&Seoul);
                        let market_close = kst_now.date_naive().and_hms_opt(15, 30, 0).unwrap();
                        let market_close_utc = chrono::NaiveDateTime::and_local_timezone(&market_close, Seoul).unwrap().to_utc();
                        (market_close_utc - chrono::Utc::now()).num_minutes().max(0)
                    };
                    if state.cached_balance.map(|(_, t)| t.elapsed().as_secs() >= BALANCE_CACHE_TTL_SECS).unwrap_or(true) {
                        match client.domestic_balance().await {
                            Ok(resp) => { state.cached_balance = Some((resp.summary.purchase_amount, Instant::now())); }
                            Err(e) => { tracing::error!("KrSignalTask: domestic_balance() failed: {e}"); }
                        }
                    }
                    let kr_account_balance = state.cached_balance.map(|(v, _)| v).unwrap_or(Decimal::ZERO);
                    { let mut ps = state.pending_symbols.lock().unwrap(); if ps.contains(&tick.symbol) { candle.reset(); continue; } ps.insert(tick.symbol.clone()); }
                    let sym_for_eval = tick.symbol.clone(); let sym_for_guard = tick.symbol.clone();
                    let pool = db_pool.clone(); let order_tx = order_tx.clone(); let regime = regime_rx.borrow().clone();
                    let completed_snap = cq.iter().cloned().collect::<Vec<_>>(); let quote_snap = state.latest_quotes.get(&sym_for_eval).cloned();
                    let summary_clone = summary.clone(); let sem = llm_semaphore.clone();
                    let pc = pending_count.clone(); let kr_exchange_code = Some(state.symbol_exchange.get(&sym_for_eval).map(|ex| ex.market_code().to_string()).unwrap_or_else(|| "J".to_string()));
                    let rc = risk_cfg.clone(); let sc = signal_cfg.clone(); let ps_clone = Arc::clone(&state.pending_symbols);
                    let permit = eval_sem.clone().acquire_owned().await.unwrap();
                    let llm_fail_count = Arc::clone(&state.llm_fail_count); let act = activity.clone(); let strategies_snap = strategies.clone(); let notion_clone = notion.clone();
                    let alert_router = alert.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        struct PendingGuard(Arc<std::sync::Mutex<std::collections::HashSet<String>>>, String);
                        impl Drop for PendingGuard { fn drop(&mut self) { self.0.lock().unwrap().remove(&self.1); } }
                        let _guard = PendingGuard(ps_clone, sym_for_guard);
                        for strategy in strategies_snap {
                            evaluate_and_maybe_order(SignalContext {
                                symbol: sym_for_eval.clone(), market: "KR".to_string(), db_pool: pool.clone(), order_tx: order_tx.clone(), regime: regime.clone(),
                                completed: completed_snap.clone(), quote: quote_snap.clone(), rolling_highs: vec![],
                                summary: summary_clone.clone(), mins_until_close, account_balance: kr_account_balance, exchange_code: kr_exchange_code.clone(),
                                risk_cfg: rc.clone(), signal_cfg: sc.clone(), strategy, activity: act.clone(), notion: notion_clone.clone(),
                                alert: alert_router.clone(), llm_sem: sem.clone(), llm_fail_count: llm_fail_count.clone(), pending_count: pc.clone(),
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
    client: &Arc<dyn KisApi>,
    pool: &sqlx::SqlitePool,
    _news_cache: &mut HashMap<String, (bool, Instant)>,
    force: bool,
) -> (HashMap<String, String>, std::collections::HashSet<String>) {
    let mut exch_map = HashMap::new();
    let mut bad = std::collections::HashSet::new();
    for sym in wl {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM daily_ohlc WHERE symbol = ? AND date != '0000-00-00'",
        )
        .bind(sym)
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        if !force && count >= 30 {
            exch_map.insert(sym.clone(), "NASD".to_string());
            continue;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        match client
            .daily_chart(kis_api::DailyChartRequest {
                symbol: sym.clone(),
                exchange: kis_api::Exchange::NASD,
                period: kis_api::ChartPeriod::Daily,
                adj_price: true,
            })
            .await
        {
            Ok(bars) => {
                let bar_count = bars.len();
                for b in bars {
                    let _ = sqlx::query("INSERT OR REPLACE INTO daily_ohlc (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)")
                        .bind(sym).bind(b.date).bind(b.open.to_string()).bind(b.high.to_string()).bind(b.low.to_string()).bind(b.close.to_string()).bind(b.volume.to_string()).execute(pool).await;
                }
                tracing::info!(symbol = %sym, bars = bar_count, "US History seeded successfully");
                exch_map.insert(sym.clone(), "NASD".to_string());
            }
            Err(_) => {
                bad.insert(sym.clone());
            }
        }
    }
    (exch_map, bad)
}

pub async fn seed_kr_symbols(
    wl: &[String],
    client: &Arc<dyn KisDomesticApi>,
    pool: &sqlx::SqlitePool,
    force: bool,
) -> (
    HashMap<String, DomesticExchange>,
    std::collections::HashSet<String>,
) {
    let mut exch_map = HashMap::new();
    let mut bad = std::collections::HashSet::new();
    for sym in wl {
        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM daily_ohlc WHERE symbol = ? AND date != '0000-00-00'",
        )
        .bind(sym)
        .fetch_one(pool)
        .await
        .unwrap_or(0);
        let exchange = if sym.starts_with('0') || sym.starts_with('3') {
            DomesticExchange::KOSPI
        } else {
            DomesticExchange::KOSDAQ
        };
        if !force && count >= 30 {
            exch_map.insert(sym.clone(), exchange);
            continue;
        }
        tokio::time::sleep(Duration::from_millis(300)).await;
        let today_str = chrono::Local::now().format("%Y%m%d").to_string();
        let start_date_str = (chrono::Local::now() - chrono::Duration::days(150))
            .format("%Y%m%d")
            .to_string();
        match client
            .domestic_daily_chart(kis_api::DomesticDailyChartRequest {
                symbol: sym.clone(),
                exchange,
                period: kis_api::ChartPeriod::Daily,
                adj_price: true,
                start_date: Some(start_date_str),
                end_date: Some(today_str),
            })
            .await
        {
            Ok(bars) => {
                let bar_count = bars.len();
                for b in bars {
                    let _ = sqlx::query("INSERT OR REPLACE INTO daily_ohlc (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)")
                        .bind(sym).bind(b.date).bind(b.open.to_string()).bind(b.high.to_string()).bind(b.low.to_string()).bind(b.close.to_string()).bind(b.volume.to_string()).execute(pool).await;
                }
                tracing::info!(symbol = %sym, bars = bar_count, "History seeded successfully");
                exch_map.insert(sym.clone(), exchange);
            }
            Err(_) => {
                bad.insert(sym.clone());
            }
        }
    }
    (exch_map, bad)
}
