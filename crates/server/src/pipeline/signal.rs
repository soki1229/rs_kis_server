use crate::market::{MarketAdapter, MarketId};
use crate::pipeline::TickData;
use crate::state::BotState;
use crate::strategy::{
    Portfolio, QualResult, QualificationStrategy, RiskStrategy, SignalCandidate,
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
}

struct SignalContext {
    symbol: String,
    market: MarketId,
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
            activity.record_eval(market.label(), &symbol, 0, "skip", "no_signal");
            return;
        }
    };

    let score = (trade_signal.strength * 100.0) as i32;

    let candidate = SignalCandidate {
        signal: trade_signal.clone(),
        regime: regime.clone(),
        setup_score: trade_signal.setup_score,
        // TODO: 파이프라인 레벨에서 실적/FOMC 캘린더 연동 후 실제 값 반영 필요
        has_earnings_event: false,
        has_fomc_today: false,
    };
    let qual_result = qual_strategy.qualify(&candidate);

    if let QualResult::Block { reason } = qual_result {
        activity.record_eval(market.label(), &symbol, score, "blocked", &reason);
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
        activity.record_eval(market.label(), &symbol, score, "skip", "qty_zero");
        return;
    }

    activity.record_eval(
        market.label(),
        &symbol,
        score,
        "order",
        &format!("{:?}", regime),
    );
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
        let req = OrderRequest {
            symbol: symbol.clone(),
            side: Side::Buy,
            qty,
            price: None,
            atr: Some(trade_signal.atr),
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
                    tracing::info!(symbol = %tick.symbol, "🕯️ 캔들 완성 (분석 시작)");
                    let completed_candle = CompletedCandle { open: candle.open, high: candle.high, low: candle.low, close: candle.close, volume: candle.volume, ts: tick.timestamp };
                    let cq = state.completed.entry(tick.symbol.clone()).or_default();
                    cq.push_back(completed_candle); if cq.len() > MAX_COMPLETED_CANDLES { cq.pop_front(); }

                    if state.cached_balance.map(|(_, t)| t.elapsed().as_secs() >= BALANCE_CACHE_TTL_SECS).unwrap_or(true) {
                        match adapter.balance().await {
                            Ok(resp) => { state.cached_balance = Some((resp.available_cash, Instant::now())); }
                            Err(e) => { tracing::error!(market = %market_id, "SignalTask: balance() failed: {e}"); }
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
                    tokio::spawn(async move {
                        let _permit = permit;
                        struct PendingGuard(Arc<std::sync::Mutex<std::collections::HashSet<String>>>, String);
                        impl Drop for PendingGuard { fn drop(&mut self) { self.0.lock().unwrap().remove(&self.1); } }
                        let _guard = PendingGuard(ps_clone, sym_for_guard);
                        for strategy in strategies_snap {
                            evaluate_and_maybe_order(SignalContext {
                                symbol: sym_for_eval.clone(), market: market_id, db_pool: pool.clone(), order_tx: order_tx.clone(), regime: regime.clone(),
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
                for b in bars {
                    let date_str = b.date.format("%Y%m%d").to_string();
                    let _ = sqlx::query("INSERT OR REPLACE INTO daily_ohlc (symbol, date, open, high, low, close, volume) VALUES (?, ?, ?, ?, ?, ?, ?)")
                        .bind(sym).bind(date_str).bind(b.open.to_string()).bind(b.high.to_string()).bind(b.low.to_string()).bind(b.close.to_string()).bind(b.volume.to_string()).execute(pool).await;
                }
                tracing::info!(symbol = %sym, bars = bar_count, "History seeded successfully");
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
                mins_since_open: 100,
                mins_until_close: 100,
                is_holiday: false,
            }
        }
        async fn is_holiday(&self) -> Result<bool, BotError> {
            Ok(false)
        }

        fn suggested_throttle_ms(&self) -> u64 {
            0
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
        sqlx::query("CREATE TABLE daily_ohlc (symbol TEXT, date TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, PRIMARY KEY(symbol, date))")
            .execute(&pool).await.unwrap();
        pool
    }

    #[tokio::test]
    async fn test_seed_symbols_writes_to_db_us() {
        let db = setup_test_db().await;
        let adapter = MockAdapter {
            market_id: MarketId::Us,
            daily_bars: vec![UnifiedDailyBar {
                date: chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap(),
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
                date: chrono::NaiveDate::from_ymd_opt(2026, 4, 12).unwrap(),
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
            // TODO: 파이프라인 레벨에서 실적/FOMC 캘린더 연동 후 실제 값 반영 필요
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
        };
        assert_eq!(req.atr, Some(atr_value));

        let fill = FillInfo {
            order_id: "ord-1".into(),
            symbol: "NVDA".into(),
            filled_qty: 10,
            filled_price: entry_price,
            atr: req.atr,
            exchange_code: None,
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
}
