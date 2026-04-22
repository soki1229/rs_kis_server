use crate::config::MarketConfig;
use crate::market::MarketAdapter;
use crate::monitoring::alert::AlertRouter;
use crate::strategy::DiscoveryStrategy;
use crate::types::WatchlistSet;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use chrono_tz::Asia::Seoul;
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub fn us_market_open_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    let open_time = NaiveTime::from_hms_opt(9, 30, 0)?;
    New_York
        .from_local_datetime(&date.and_time(open_time))
        .earliest()
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn us_market_close_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    let close_time = NaiveTime::from_hms_opt(16, 0, 0)?;
    New_York
        .from_local_datetime(&date.and_time(close_time))
        .earliest()
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn kr_market_open_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    let open_time = NaiveTime::from_hms_opt(9, 0, 0)?;
    Seoul
        .from_local_datetime(&date.and_time(open_time))
        .earliest()
        .map(|dt| dt.with_timezone(&Utc))
}

pub fn kr_market_close_utc(date: NaiveDate) -> Option<DateTime<Utc>> {
    let close_time = NaiveTime::from_hms_opt(15, 30, 0)?;
    Seoul
        .from_local_datetime(&date.and_time(close_time))
        .earliest()
        .map(|dt| dt.with_timezone(&Utc))
}
pub async fn build_watchlist(
    adapter: Arc<dyn MarketAdapter>,
    strategy: &Arc<dyn DiscoveryStrategy>,
    cfg: &MarketConfig,
    _alert: &AlertRouter,
    db: &SqlitePool,
) -> WatchlistSet {
    let stable = strategy
        .build_watchlist(adapter.clone(), cfg.dynamic_watchlist_size)
        .await;
    let fresh = WatchlistSet {
        stable,
        aggressive: vec![],
    };
    merge_with_protected_symbols(fresh, db, cfg).await
}

async fn merge_with_protected_symbols(
    fresh: WatchlistSet,
    db: &SqlitePool,
    _config: &MarketConfig,
) -> WatchlistSet {
    let mut result = fresh;
    if let Ok(rows) = sqlx::query("SELECT symbol FROM positions")
        .fetch_all(db)
        .await
    {
        for r in rows {
            let sym: String = r.get(0);
            if !result.stable.contains(&sym) && !result.aggressive.contains(&sym) {
                result.stable.push(sym);
            }
        }
    }
    result
}

async fn sleep_until_or_cancel(target: DateTime<Utc>, token: &CancellationToken) {
    let now = Utc::now();
    if target > now {
        tokio::select! { _ = token.cancelled() => {} _ = tokio::time::sleep((target - now).to_std().unwrap_or(std::time::Duration::ZERO)) => {} }
    }
}

async fn sleep_until_next_day(token: &CancellationToken) {
    let tomorrow = (Utc::now() + chrono::Duration::days(1)).date_naive();
    let wake_up = Utc
        .from_local_datetime(&tomorrow.and_hms_opt(0, 0, 0).unwrap())
        .unwrap();
    sleep_until_or_cancel(wake_up, token).await;
}

#[allow(clippy::too_many_arguments)]
pub async fn run_kr_scheduler_task(
    adapter: Arc<dyn MarketAdapter>,
    discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
    watchlist_tx: tokio::sync::watch::Sender<WatchlistSet>,
    eod_tx: tokio::sync::mpsc::Sender<()>,
    config: MarketConfig,
    alert: AlertRouter,
    summary_alert: AlertRouter,
    activity: crate::shared::activity::ActivityLog,
    db: SqlitePool,
    token: CancellationToken,
) {
    loop {
        let now_kst = Utc::now().with_timezone(&Seoul);
        let today = now_kst.date_naive();
        let open = kr_market_open_utc(today).unwrap();
        let close = kr_market_close_utc(today).unwrap();
        let pre_open = open - chrono::Duration::hours(1);
        let post_close = close + chrono::Duration::hours(1);
        let now = Utc::now();

        let current_wl = watchlist_tx.borrow().clone();
        if !current_wl.all_unique().is_empty() {
            activity.set_watchlist("KR", &current_wl.all_unique());
        }

        let is_holiday = adapter.is_holiday().await.unwrap_or(false);
        if is_holiday {
            activity.set_phase("KR", "공휴일 휴장");
            watchlist_tx.send(WatchlistSet::default()).ok();
            sleep_until_next_day(&token).await;
        } else if now >= post_close {
            if !watchlist_tx.borrow().all_unique().is_empty() {
                activity.set_phase("KR", "장 마감");
                watchlist_tx.send(WatchlistSet::default()).ok();
                eod_tx.send(()).await.ok();
            }
            sleep_until_next_day(&token).await;
        } else if now >= pre_open {
            activity.set_phase("KR", "장 중");
            let merged =
                build_watchlist(adapter.clone(), &discovery_strategy, &config, &alert, &db).await;
            if merged != *watchlist_tx.borrow() {
                activity.set_watchlist("KR", &merged.all_unique());
                watchlist_tx.send(merged.clone()).ok();
                summary_alert.info(format!(
                    "✅ [국내] 관심종목 동기화 완료 (안정: {}, 적극: {})",
                    merged.stable.len(),
                    merged.aggressive.len()
                ));
            }
            tokio::select! { _ = token.cancelled() => return, _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {} }
        } else {
            activity.set_phase("KR", "장 오픈 대기");
            watchlist_tx.send(WatchlistSet::default()).ok();
            sleep_until_or_cancel(pre_open, &token).await;
        }
        if token.is_cancelled() {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn run_scheduler_task(
    adapter: Arc<dyn MarketAdapter>,
    discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
    watchlist_tx: tokio::sync::watch::Sender<WatchlistSet>,
    eod_tx: tokio::sync::mpsc::Sender<()>,
    config: MarketConfig,
    alert: AlertRouter,
    summary_alert: AlertRouter,
    activity: crate::shared::activity::ActivityLog,
    db: SqlitePool,
    token: CancellationToken,
) {
    loop {
        let now_ny = Utc::now().with_timezone(&New_York);
        let today = now_ny.date_naive();
        let open = us_market_open_utc(today).unwrap();
        let close = us_market_close_utc(today).unwrap();
        let pre_open = open - chrono::Duration::hours(1);
        let post_close = close + chrono::Duration::hours(1);
        let now = Utc::now();

        let current_wl = watchlist_tx.borrow().clone();
        if !current_wl.all_unique().is_empty() {
            activity.set_watchlist("US", &current_wl.all_unique());
        }

        let is_holiday = adapter.is_holiday().await.unwrap_or(false);
        if is_holiday {
            activity.set_phase("US", "공휴일 휴장");
            watchlist_tx.send(WatchlistSet::default()).ok();
            sleep_until_next_day(&token).await;
        } else if now >= post_close {
            if !watchlist_tx.borrow().all_unique().is_empty() {
                activity.set_phase("US", "장 마감");
                watchlist_tx.send(WatchlistSet::default()).ok();
                eod_tx.send(()).await.ok();
            }
            sleep_until_next_day(&token).await;
        } else if now >= pre_open {
            activity.set_phase("US", "장 중");
            let merged =
                build_watchlist(adapter.clone(), &discovery_strategy, &config, &alert, &db).await;
            if merged != *watchlist_tx.borrow() {
                activity.set_watchlist("US", &merged.all_unique());
                watchlist_tx.send(merged.clone()).ok();
                summary_alert.info(format!(
                    "✅ [미국] 관심종목 동기화 완료 (안정: {}, 적극: {})",
                    merged.stable.len(),
                    merged.aggressive.len()
                ));
            }
            tokio::select! { _ = token.cancelled() => return, _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {} }
        } else {
            activity.set_phase("US", "장 오픈 대기");
            watchlist_tx.send(WatchlistSet::default()).ok();
            sleep_until_or_cancel(pre_open, &token).await;
        }
        if token.is_cancelled() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BotError;
    use crate::market::{
        MarketId, MarketTiming, PollOutcome, UnifiedBalance, UnifiedCandleBar, UnifiedDailyBar,
        UnifiedOrderHistoryItem, UnifiedOrderRequest, UnifiedOrderResult, UnifiedUnfilledOrder,
    };
    use rust_decimal::Decimal;
    use sqlx::SqlitePool;

    struct MockDiscovery;
    #[async_trait::async_trait]
    impl crate::strategy::DiscoveryStrategy for MockDiscovery {
        async fn build_watchlist(&self, _: Arc<dyn MarketAdapter>, _: usize) -> Vec<String> {
            vec!["AAPL".into()]
        }
    }

    struct MockAdapter {
        holiday: bool,
    }
    #[async_trait::async_trait]
    impl MarketAdapter for MockAdapter {
        fn market_id(&self) -> MarketId {
            MarketId::Us
        }
        fn name(&self) -> &'static str {
            "MOCK"
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
            unimplemented!()
        }
        async fn daily_chart(&self, _: &str, _: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
            Ok(vec![])
        }
        async fn intraday_candles(
            &self,
            _: &str,
            _: u32,
        ) -> Result<Vec<UnifiedCandleBar>, BotError> {
            Ok(vec![])
        }
        async fn current_price(&self, _: &str) -> Result<Decimal, BotError> {
            Ok(Decimal::ZERO)
        }
        async fn volume_ranking(&self, _: u32) -> Result<Vec<String>, BotError> {
            Ok(vec![])
        }
        fn market_timing(&self) -> MarketTiming {
            MarketTiming {
                is_open: !self.holiday,
                mins_since_open: 10,
                mins_until_close: 10,
                mins_until_open: 0,
                is_holiday: self.holiday,
            }
        }

        async fn is_holiday(&self) -> Result<bool, BotError> {
            Ok(false)
        }
        fn fx_spread_pct(&self) -> Decimal {
            Decimal::ZERO
        }
    }

    async fn test_db() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS daily_ohlc (symbol TEXT, name TEXT, date TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, PRIMARY KEY(symbol, date))").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    #[tokio::test]
    async fn build_kr_watchlist_merges_and_dedups() {
        let db = test_db().await;
        sqlx::query("INSERT INTO positions (symbol) VALUES ('POS1')")
            .execute(&db)
            .await
            .unwrap();

        let adapter = Arc::new(MockAdapter { holiday: false });
        let discovery: Arc<dyn crate::strategy::DiscoveryStrategy> = Arc::new(MockDiscovery);
        let alert = AlertRouter::new(10);
        let config = MarketConfig {
            watchlist: vec!["STATIC1".into()],
            dynamic_watchlist_size: 5,
            db_path: ":memory:".into(),
            kill_switch_path: "/tmp/ks.json".into(),
            trading_account: crate::config::AccountKind::Vts,
            data_provider: crate::config::AccountKind::Real,
            watchlist_refresh_interval_secs: 600,
            strategies: vec![],
            use_generic_pipeline: false,
        };

        let wl = build_watchlist(adapter, &discovery, &config, &alert, &db).await;
        assert!(wl.stable.contains(&"AAPL".to_string()));
        assert!(wl.stable.contains(&"POS1".to_string()));
    }
}
