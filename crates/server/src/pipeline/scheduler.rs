use crate::config::MarketConfig;
use crate::market::MarketAdapter;
use crate::monitoring::alert::AlertRouter;
use crate::strategy::DiscoveryStrategy;
use crate::types::WatchlistSet;
use chrono::{DateTime, Utc};
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub async fn build_watchlist(
    adapter: Arc<dyn MarketAdapter>,
    strategy: &Arc<dyn DiscoveryStrategy>,
    cfg: &MarketConfig,
    _alert: &AlertRouter,
    db: &SqlitePool,
) -> WatchlistSet {
    let mut dynamic = strategy
        .build_watchlist(adapter.clone(), cfg.dynamic_watchlist_size)
        .await;

    // config symbol_blacklist + 최근 7일 내 "매매불가" 주문실패 종목 제외
    let auto_blacklist = fetch_order_fail_blacklist(db).await;
    // KR: daily_ohlc 종목명 기반 레버리지/인버스/선물/ETN 제외
    let name_blacklist = fetch_name_blacklist(db, adapter.market_id()).await;
    dynamic.retain(|sym| {
        let in_config = cfg.symbol_blacklist.contains(sym);
        let in_auto = auto_blacklist.contains(sym);
        let in_name = name_blacklist.contains(sym);
        if in_config || in_auto || in_name {
            tracing::debug!(symbol = %sym, config = in_config, auto = in_auto, name = in_name, "Blacklisted symbol excluded from watchlist");
        }
        !in_config && !in_auto && !in_name
    });

    // 정적 watchlist(config)에 있지만 dynamic에 없는 종목을 앞에 삽입
    for sym in cfg.watchlist.iter().rev() {
        if !dynamic.contains(sym) {
            dynamic.insert(0, sym.clone());
        }
    }
    let fresh = WatchlistSet {
        stable: dynamic,
        aggressive: vec![],
    };
    merge_with_protected_symbols(fresh, db, cfg).await
}

/// KR: daily_ohlc 종목명에 레버리지/선물/ETN/2x인버스 키워드가 포함된 종목을 블랙리스트로 반환.
/// 1x 인버스 ETF(예: KODEX 인버스 114800)는 Volatile 하락장 대응을 위해 허용한다.
async fn fetch_name_blacklist(db: &SqlitePool, market_id: crate::market::MarketId) -> Vec<String> {
    if !matches!(
        market_id,
        crate::market::MarketId::Kr | crate::market::MarketId::KrVts
    ) {
        return vec![];
    }
    let rows = sqlx::query(
        "SELECT DISTINCT symbol FROM daily_ohlc
         WHERE name IS NOT NULL
           AND (name LIKE '%레버리지%'
             OR name LIKE '%선물%'
             OR name LIKE '%ETN%'
             OR name LIKE '%곱버스%')",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    rows.into_iter()
        .filter_map(|r| r.try_get::<String, _>("symbol").ok())
        .collect()
}

/// 최근 7일 내 "매매불가 종목" 오류(40070000)가 반복된 종목을 자동 블랙리스트로 반환.
async fn fetch_order_fail_blacklist(db: &SqlitePool) -> Vec<String> {
    let rows = sqlx::query(
        "SELECT DISTINCT symbol FROM audit_log
         WHERE event_type = 'order_failed'
           AND detail LIKE '%40070000%'
           AND created_at >= datetime('now', '-7 days')
           AND symbol IS NOT NULL",
    )
    .fetch_all(db)
    .await
    .unwrap_or_default();

    rows.into_iter()
        .filter_map(|r| r.try_get::<String, _>("symbol").ok())
        .collect()
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
    let wake_up =
        chrono::TimeZone::from_local_datetime(&Utc, &tomorrow.and_hms_opt(0, 0, 0).unwrap())
            .unwrap();
    sleep_until_or_cancel(wake_up, token).await;
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
    let market_label = adapter.market_id().label();
    let locale_label = adapter.market_id().locale_label();
    loop {
        let today = adapter.local_today();
        let open = adapter.market_open_utc(today).unwrap();
        let close = adapter.market_close_utc(today).unwrap();
        let pre_open = open - chrono::Duration::hours(1);
        // EOD fallback backup: close + 10분 (fallback 미발화 시 보조 트리거)
        let post_close = close + chrono::Duration::minutes(10);
        let now = Utc::now();

        let current_wl = watchlist_tx.borrow().clone();
        if !current_wl.all_unique().is_empty() {
            activity.set_watchlist(market_label, &current_wl.all_unique());
        }

        let is_holiday = adapter.is_holiday().await.unwrap_or(false);
        if is_holiday {
            activity.set_phase(market_label, "공휴일 휴장");
            watchlist_tx.send(WatchlistSet::default()).ok();
            sleep_until_next_day(&token).await;
        } else if now >= post_close {
            if !watchlist_tx.borrow().all_unique().is_empty() {
                activity.set_phase(market_label, "장 마감");
                watchlist_tx.send(WatchlistSet::default()).ok();
                eod_tx.send(()).await.ok();
            }
            sleep_until_next_day(&token).await;
        } else if now >= pre_open {
            activity.set_phase(market_label, "장 중");
            let merged =
                build_watchlist(adapter.clone(), &discovery_strategy, &config, &alert, &db).await;
            if merged != *watchlist_tx.borrow() {
                activity.set_watchlist(market_label, &merged.all_unique());
                watchlist_tx.send(merged.clone()).ok();
                summary_alert.info(format!(
                    "✅ [{}] 관심종목 동기화 완료 (안정: {}, 적극: {})",
                    locale_label,
                    merged.stable.len(),
                    merged.aggressive.len()
                ));
                let detail = format!(
                    "stable:{} aggressive:{} symbols:[{}]",
                    merged.stable.len(),
                    merged.aggressive.len(),
                    merged.all_unique().join(",")
                );
                sqlx::query(
                    "INSERT INTO audit_log (event_type, market, symbol, detail, created_at) VALUES ('watchlist_updated', ?, NULL, ?, ?)",
                )
                .bind(market_label)
                .bind(&detail)
                .bind(chrono::Utc::now().to_rfc3339())
                .execute(&db)
                .await
                .ok();
            }
            tokio::select! { _ = token.cancelled() => return, _ = tokio::time::sleep(std::time::Duration::from_secs(600)) => {} }
        } else {
            activity.set_phase(market_label, "장 오픈 대기");
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
            symbol_blacklist: vec![],
        };

        let wl = build_watchlist(adapter, &discovery, &config, &alert, &db).await;
        assert!(wl.stable.contains(&"AAPL".to_string()));
        assert!(wl.stable.contains(&"POS1".to_string()));
    }
}
