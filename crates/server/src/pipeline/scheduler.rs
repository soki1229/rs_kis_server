use crate::config::MarketConfig;
use crate::monitoring::alert::AlertRouter;
use crate::types::WatchlistSet;
use chrono::{DateTime, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use chrono_tz::Asia::Seoul;
use kis_api::{DomesticExchange, Exchange, KisApi, KisDomesticApi};
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
    client: &Arc<dyn KisApi>,
    config: &MarketConfig,
    _alert: &AlertRouter,
    db: &sqlx::SqlitePool,
) -> WatchlistSet {
    let count = config.dynamic_watchlist_size as u32;
    let mut all_items = Vec::new();
    for exchange in [Exchange::NASD, Exchange::NYSE] {
        if let Ok(Ok(items)) = tokio::time::timeout(
            std::time::Duration::from_secs(10),
            client.volume_ranking(&exchange, count * 2),
        )
        .await
        {
            all_items.extend(items);
        }
    }
    if all_items.is_empty() {
        return WatchlistSet {
            stable: config.watchlist.clone(),
            aggressive: vec![],
        };
    }
    for item in &all_items {
        let _ = sqlx::query("INSERT INTO daily_ohlc (symbol, name, date, open, high, low, close, volume) VALUES (?, ?, '0000-00-00', '0', '0', '0', '0', '0') ON CONFLICT(symbol, date) DO UPDATE SET name = excluded.name")
            .bind(&item.symbol).bind(&item.name).execute(db).await;
    }
    all_items.sort_by(|a, b| {
        b.rate
            .partial_cmp(&a.rate)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    let mid = all_items.len() / 2;
    let (aggressive_items, stable_items) = all_items.split_at(mid);
    WatchlistSet {
        stable: stable_items
            .iter()
            .take(config.dynamic_watchlist_size)
            .map(|i| i.symbol.clone())
            .collect(),
        aggressive: aggressive_items
            .iter()
            .take(config.dynamic_watchlist_size)
            .map(|i| i.symbol.clone())
            .collect(),
    }
}

pub async fn build_kr_watchlist(
    client: &Arc<dyn KisDomesticApi>,
    config: &MarketConfig,
    _alert: &AlertRouter,
    db: &sqlx::SqlitePool,
) -> WatchlistSet {
    let count = config.dynamic_watchlist_size as u32;
    let mut kospi_items = Vec::new();
    let mut kosdaq_items = Vec::new();

    // 1. KOSPI 거래량 상위
    if let Ok(Ok(items)) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.domestic_volume_ranking(&DomesticExchange::KOSPI, count * 2),
    )
    .await
    {
        kospi_items = items;
    }

    // 2. KOSDAQ 거래량 상위
    if let Ok(Ok(items)) = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.domestic_volume_ranking(&DomesticExchange::KOSDAQ, count),
    )
    .await
    {
        kosdaq_items = items;
    }

    // DB에 이름 저장
    for item in kospi_items.iter().chain(kosdaq_items.iter()) {
        let _ = sqlx::query("INSERT INTO daily_ohlc (symbol, name, date, open, high, low, close, volume) VALUES (?, ?, '0000-00-00', '0', '0', '0', '0', '0') ON CONFLICT(symbol, date) DO UPDATE SET name = excluded.name")
            .bind(&item.symbol).bind(&item.name).execute(db).await;
    }

    let mut stable = Vec::new();
    let mut aggressive = Vec::new();

    stable.extend(
        kospi_items
            .iter()
            .take(count as usize)
            .map(|i| i.symbol.clone()),
    );

    if !kosdaq_items.is_empty() {
        aggressive.extend(
            kosdaq_items
                .iter()
                .take(count as usize)
                .map(|i| i.symbol.clone()),
        );
    } else if kospi_items.len() > count as usize {
        tracing::warn!("KOSDAQ ranking empty, using KOSPI 31~60 for Aggressive list");
        aggressive.extend(
            kospi_items
                .iter()
                .skip(count as usize)
                .take(count as usize)
                .map(|i| i.symbol.clone()),
        );
    }

    if stable.is_empty() && aggressive.is_empty() {
        return WatchlistSet {
            stable: config.watchlist.clone(),
            aggressive: vec![],
        };
    }

    WatchlistSet { stable, aggressive }
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
    let wake_up = Utc.from_utc_datetime(&tomorrow.and_hms_opt(0, 0, 0).unwrap());
    sleep_until_or_cancel(wake_up, token).await;
}

#[allow(clippy::too_many_arguments)]
pub async fn run_kr_scheduler_task(
    client: Arc<dyn KisDomesticApi>,
    discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
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

        let is_holiday = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.domestic_holidays(&today.format("%Y%m%d").to_string()),
        )
        .await
        {
            Ok(Ok(h)) => h
                .iter()
                .any(|item| item.date == today.format("%Y%m%d").to_string()),
            _ => false,
        };
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
            let symbols = discovery_strategy
                .build_kr_watchlist(Arc::clone(&client))
                .await;
            let fresh = WatchlistSet {
                stable: symbols,
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
    client: Arc<dyn KisApi>,
    discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy>,
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

        let is_holiday = match tokio::time::timeout(
            std::time::Duration::from_secs(5),
            client.holidays(&today.format("%Y%m%d").to_string()),
        )
        .await
        {
            Ok(Ok(h)) => h
                .iter()
                .any(|item| item.date == today.format("%Y%m%d").to_string()),
            _ => false,
        };
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
            let symbols = discovery_strategy
                .build_us_watchlist(Arc::clone(&client))
                .await;
            let fresh = WatchlistSet {
                stable: symbols,
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
    use async_trait::async_trait;
    use kis_api::{
        CandleBar, DomesticCancelOrderRequest, DomesticCancelOrderResponse,
        DomesticDailyChartRequest, DomesticPlaceOrderRequest, DomesticPlaceOrderResponse,
        DomesticRankingItem, DomesticUnfilledOrder, Holiday, KisError, KisStream,
    };

    async fn test_db() -> SqlitePool {
        let pool = SqlitePool::connect(":memory:").await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS daily_ohlc (symbol TEXT, name TEXT, date TEXT, open TEXT, high TEXT, low TEXT, close TEXT, volume TEXT, PRIMARY KEY(symbol, date))").execute(&pool).await.unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY)")
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    struct MockKrSchedulerApi {
        ranking_result: Result<Vec<DomesticRankingItem>, String>,
        holidays_result: Result<Vec<Holiday>, String>,
    }
    impl MockKrSchedulerApi {
        fn ok(ranking: Vec<DomesticRankingItem>) -> Arc<dyn KisDomesticApi> {
            Arc::new(Self {
                ranking_result: Ok(ranking),
                holidays_result: Ok(vec![]),
            })
        }
    }
    #[async_trait]
    impl KisDomesticApi for MockKrSchedulerApi {
        async fn domestic_stream(&self) -> Result<KisStream, KisError> {
            unimplemented!()
        }
        async fn domestic_volume_ranking(
            &self,
            _: &DomesticExchange,
            _: u32,
        ) -> Result<Vec<DomesticRankingItem>, KisError> {
            match &self.ranking_result {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(KisError::WebSocket(e.clone())),
            }
        }
        async fn domestic_holidays(&self, _: &str) -> Result<Vec<Holiday>, KisError> {
            match &self.holidays_result {
                Ok(v) => Ok(v.clone()),
                Err(e) => Err(KisError::WebSocket(e.clone())),
            }
        }
        async fn domestic_place_order(
            &self,
            _: DomesticPlaceOrderRequest,
        ) -> Result<DomesticPlaceOrderResponse, KisError> {
            unimplemented!()
        }
        async fn domestic_cancel_order(
            &self,
            _: DomesticCancelOrderRequest,
        ) -> Result<DomesticCancelOrderResponse, KisError> {
            unimplemented!()
        }
        async fn domestic_daily_chart(
            &self,
            _: DomesticDailyChartRequest,
        ) -> Result<Vec<CandleBar>, KisError> {
            Ok(vec![])
        }
        async fn domestic_unfilled_orders(&self) -> Result<Vec<DomesticUnfilledOrder>, KisError> {
            Ok(vec![])
        }
        async fn domestic_order_history(
            &self,
            _: kis_api::DomesticOrderHistoryRequest,
        ) -> Result<Vec<kis_api::DomesticOrderHistoryItem>, KisError> {
            Ok(vec![])
        }
        async fn domestic_balance(&self) -> Result<kis_api::BalanceResponse, KisError> {
            unimplemented!()
        }
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
        }
    }

    #[tokio::test]
    async fn build_kr_watchlist_merges_and_dedups() {
        let ranking = vec![DomesticRankingItem {
            symbol: "005930".into(),
            name: "Samsung".into(),
            exchange: "KSC".into(),
            price: rust_decimal_macros::dec!(70000),
            volume: rust_decimal_macros::dec!(1000000),
        }];
        let client = MockKrSchedulerApi::ok(ranking);
        let config = test_config(vec!["000660"]);
        let db = test_db().await;
        let res = build_kr_watchlist(&client, &config, &AlertRouter::new(1), &db).await;
        assert!(res.stable.contains(&"005930".to_string()));
    }
}
