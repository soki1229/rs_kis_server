use crate::config::MarketConfig;
use crate::market::MarketAdapter;
use crate::monitoring::alert::AlertRouter;
use crate::types::WatchlistSet;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use chrono_tz::Asia::Seoul;
use sqlx::SqlitePool;
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
    _client: &dyn MarketAdapter,
    config: &MarketConfig,
    _alert: &AlertRouter,
    _db: &sqlx::SqlitePool,
) -> WatchlistSet {
    // For now, fallback to static watchlist since volume_ranking is market-specific
    WatchlistSet {
        stable: config.watchlist.clone(),
        aggressive: vec![],
    }
}

pub async fn build_kr_watchlist(
    _client: &dyn MarketAdapter,
    config: &MarketConfig,
    _alert: &AlertRouter,
    _db: &sqlx::SqlitePool,
) -> WatchlistSet {
    // For now, fallback to static watchlist
    WatchlistSet {
        stable: config.watchlist.clone(),
        aggressive: vec![],
    }
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
    _discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
    watchlist_tx: tokio::sync::watch::Sender<WatchlistSet>,
    eod_tx: tokio::sync::mpsc::Sender<()>,
    config: MarketConfig,
    _alert: AlertRouter,
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
                watchlist_tx.send(WatchlistSet::default()).ok();
                let _ = eod_tx.send(()).await;
            }
            activity.set_phase("KR", "장 마감 (EOD)");
            sleep_until_next_day(&token).await;
        } else if now >= pre_open {
            activity.set_phase("KR", if now >= open { "Trading" } else { "Pre-market" });
            // DiscoveryStrategy building still expects specific clients for now
            // This will be fixed in a later phase. For now, use static watchlist.
            let fresh = WatchlistSet {
                stable: config.watchlist.clone(),
                aggressive: vec![],
            };
            let merged: WatchlistSet =
                merge_with_protected_symbols(fresh.clone(), &db, &config).await;
            if merged != *watchlist_tx.borrow() {
                activity.set_watchlist("KR", &merged.all_unique());
                watchlist_tx.send(merged).ok();
                summary_alert.info(format!(
                    "✅ [국내] 관심종목 동기화 완료 (보수적: {} / 적극적: {})",
                    fresh.stable.len(),
                    fresh.aggressive.len()
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
    _discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
    watchlist_tx: tokio::sync::watch::Sender<WatchlistSet>,
    eod_tx: tokio::sync::mpsc::Sender<()>,
    config: MarketConfig,
    _alert: AlertRouter,
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
                watchlist_tx.send(WatchlistSet::default()).ok();
                let _ = eod_tx.send(()).await;
            }
            activity.set_phase("US", "장 마감 (EOD)");
            sleep_until_next_day(&token).await;
        } else if now >= pre_open {
            activity.set_phase("US", if now >= open { "Trading" } else { "Pre-market" });
            let fresh = WatchlistSet {
                stable: config.watchlist.clone(),
                aggressive: vec![],
            };
            let merged: WatchlistSet =
                merge_with_protected_symbols(fresh.clone(), &db, &config).await;
            if merged != *watchlist_tx.borrow() {
                activity.set_watchlist("US", &merged.all_unique());
                watchlist_tx.send(merged).ok();
                summary_alert.info(format!(
                    "✅ [미국] 관심종목 동기화 완료 (보수적: {} / 적극적: {})",
                    fresh.stable.len(),
                    fresh.aggressive.len()
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

use sqlx::Row;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BotError;
    use crate::market::{
        MarketId, MarketTiming, PollOutcome, UnifiedBalance, UnifiedCandleBar, UnifiedDailyBar,
        UnifiedOrderHistoryItem, UnifiedOrderRequest, UnifiedOrderResult, UnifiedUnfilledOrder,
    };
    use async_trait::async_trait;
    use rust_decimal::Decimal;

    struct MockAdapter {
        holiday: bool,
    }
    #[async_trait]
    impl MarketAdapter for MockAdapter {
        fn market_id(&self) -> MarketId {
            MarketId::Kr
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
        fn market_timing(&self) -> MarketTiming {
            MarketTiming {
                is_open: true,
                mins_since_open: 0,
                mins_until_close: 0,
                is_holiday: false,
            }
        }
        async fn is_holiday(&self) -> Result<bool, BotError> {
            Ok(self.holiday)
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

    fn test_config(watchlist: Vec<&str>) -> MarketConfig {
        MarketConfig {
            watchlist: watchlist.iter().map(|s| s.to_string()).collect(),
            dynamic_watchlist_size: 5,
            db_path: ":memory:".into(),
            kill_switch_path: "/tmp/ks.json".into(),
            dry_run: None,
            watchlist_refresh_interval_secs: 600,
            strategies: vec![],
            use_generic_pipeline: false,
        }
    }

    #[tokio::test]
    async fn build_kr_watchlist_merges_and_dedups() {
        let adapter = MockAdapter { holiday: false };
        let config = test_config(vec!["000660"]);
        let db = test_db().await;
        let res = build_kr_watchlist(&adapter, &config, &AlertRouter::new(1), &db).await;
        assert!(res.stable.contains(&"000660".to_string()));
    }
}
