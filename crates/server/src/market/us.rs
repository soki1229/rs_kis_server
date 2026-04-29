//! US market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::America::New_York;
use kis_api::models::*;
use kis_api::KisClient;
use rust_decimal::Decimal;

use crate::shared::throttler::KisThrottler;
use std::sync::Arc;

/// Base logic for US market shared by Real and VTS adapters.
struct UsMarketBase {
    client: KisClient,
    cano: String,
    acnt_prdt_cd: String,
    fx_spread_pct: Decimal,
    throttler: Arc<KisThrottler>,
}

impl UsMarketBase {
    fn new(
        client: KisClient,
        cano: String,
        acnt_prdt_cd: String,
        throttler: Arc<KisThrottler>,
    ) -> Self {
        Self {
            client,
            cano,
            acnt_prdt_cd,
            fx_spread_pct: Decimal::new(5, 3),
            throttler,
        }
    }

    fn exchange_from_hint(hint: Option<&str>, symbol: &str) -> String {
        match hint {
            Some("NYSE") => "NYSE".to_string(),
            Some("AMEX") => "AMEX".to_string(),
            Some("NASD") | Some("NASDAQ") => "NASD".to_string(),
            _ => {
                if symbol.len() <= 3 {
                    "NYSE".to_string()
                } else {
                    "NASD".to_string()
                }
            }
        }
    }

    /// 시세 조회용 거래소 코드 매핑 (3글자)
    fn quotation_exchange_from_hint(hint: Option<&str>, symbol: &str) -> String {
        match hint {
            Some("NYSE") => "NYS".to_string(),
            Some("AMEX") => "AMS".to_string(),
            Some("NASD") | Some("NASDAQ") => "NAS".to_string(),
            _ => {
                if symbol.len() <= 3 {
                    "NYS".to_string()
                } else {
                    "NAS".to_string()
                }
            }
        }
    }

    async fn confirm_fill_from_history(
        &self,
        broker_order_no: &str,
        submitted_qty: u64,
    ) -> PollOutcome {
        let today = Utc::now()
            .with_timezone(&New_York)
            .format("%Y%m%d")
            .to_string();
        match us_order_history(self, &today, &today).await {
            Ok(history) => {
                if let Some(h) = history.iter().find(|h| h.order_no == broker_order_no) {
                    let filled_qty = h.filled_qty as u64;
                    if filled_qty >= submitted_qty {
                        return PollOutcome::Filled {
                            filled_qty,
                            filled_price: h.filled_price,
                        };
                    } else if filled_qty > 0 {
                        return PollOutcome::PartialFilled {
                            filled_qty,
                            filled_price: h.filled_price,
                        };
                    }
                }
                PollOutcome::Cancelled
            }
            Err(e) => {
                tracing::warn!(
                    "UsMarketBase: order_history error for {}: {}",
                    broker_order_no,
                    e
                );
                PollOutcome::Failed {
                    reason: format!("order_history error: {}", e),
                }
            }
        }
    }
}

/// US Real Market Adapter.
pub struct UsRealAdapter {
    base: UsMarketBase,
}

impl UsRealAdapter {
    pub fn new(client: KisClient, throttler: Arc<KisThrottler>) -> Self {
        Self {
            base: UsMarketBase::new(
                client,
                std::env::var("KIS_ACCOUNT_NO").unwrap_or_default(),
                std::env::var("KIS_ACCOUNT_CD").unwrap_or_else(|_| "01".to_string()),
                throttler,
            ),
        }
    }
}

#[async_trait]
impl MarketAdapter for UsRealAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::Us
    }

    fn name(&self) -> &'static str {
        "US-Real"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        us_place_order(&self.base, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        us_cancel_order(&self.base, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        us_unfilled_orders(&self.base).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        us_order_history(&self.base, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match us_unfilled_orders(&self.base).await {
            Ok(orders) => {
                let still_open = orders.iter().any(|o| o.order_no == broker_order_no);
                if still_open {
                    Ok(PollOutcome::StillOpen)
                } else {
                    Ok(self
                        .base
                        .confirm_fill_from_history(broker_order_no, expected_qty)
                        .await)
                }
            }
            Err(e) => Err(BotError::ApiError {
                msg: format!("unfilled_orders failed: {}", e),
            }),
        }
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        us_balance(&self.base).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        us_daily_chart(&self.base, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        us_intraday_candles(&self.base, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        us_current_price(&self.base, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        us_volume_ranking(&self.base, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        us_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        us_is_holiday(&self.base).await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let open_time = chrono::NaiveTime::from_hms_opt(9, 30, 0)?;
        New_York
            .from_local_datetime(&date.and_time(open_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let close_time = chrono::NaiveTime::from_hms_opt(16, 0, 0)?;
        New_York
            .from_local_datetime(&date.and_time(close_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn local_today(&self) -> chrono::NaiveDate {
        Utc::now().with_timezone(&New_York).date_naive()
    }

    fn fx_spread_pct(&self) -> Decimal {
        self.base.fx_spread_pct
    }

    fn get_ws_key(&self, symbol: &str) -> String {
        // According to KIS Official Guide for Overseas Real-time:
        // D + (NAS or NYS or AMS) + Symbol
        let exchange = if symbol.len() == 3 || symbol == "QQQ" || symbol == "SPY" {
            "NAS"
        } else if symbol.len() == 1 || symbol.len() == 2 {
            "NYS"
        } else {
            "NAS"
        };
        format!("D{}{}", exchange, symbol)
    }
}

/// US VTS Market Adapter.
pub struct UsVtsAdapter {
    /// Base for trading (orders, balance) - uses VTS client
    base: UsMarketBase,
    /// Base for data (price, ranking) - uses Real client
    data_base: UsMarketBase,
}

impl UsVtsAdapter {
    pub fn new(
        vts_client: KisClient,
        real_client: KisClient,
        throttler: Arc<KisThrottler>,
    ) -> Self {
        let cano = std::env::var("KIS_VTS_ACCOUNT_NO")
            .or_else(|_| std::env::var("KIS_ACCOUNT_NO"))
            .unwrap_or_default();
        let acnt_prdt_cd = std::env::var("KIS_VTS_ACCOUNT_CD")
            .or_else(|_| std::env::var("KIS_ACCOUNT_CD"))
            .unwrap_or_else(|_| "01".to_string());
        Self {
            base: UsMarketBase::new(
                vts_client,
                cano.clone(),
                acnt_prdt_cd.clone(),
                throttler.clone(),
            ),
            data_base: UsMarketBase::new(real_client, cano, acnt_prdt_cd, throttler),
        }
    }
}

#[async_trait]
impl MarketAdapter for UsVtsAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::UsVts
    }

    fn name(&self) -> &'static str {
        "US-VTS"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        us_place_order(&self.base, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        us_cancel_order(&self.base, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        us_unfilled_orders(&self.base).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        us_order_history(&self.base, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match us_unfilled_orders(&self.base).await {
            Ok(orders) => {
                let still_open = orders.iter().any(|o| o.order_no == broker_order_no);
                if still_open {
                    Ok(PollOutcome::StillOpen)
                } else {
                    Ok(self
                        .base
                        .confirm_fill_from_history(broker_order_no, expected_qty)
                        .await)
                }
            }
            Err(e) => Err(BotError::ApiError {
                msg: format!("unfilled_orders failed: {}", e),
            }),
        }
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        us_balance(&self.base).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        us_daily_chart(&self.data_base, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        us_intraday_candles(&self.data_base, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        us_current_price(&self.data_base, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        us_volume_ranking(&self.data_base, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        us_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        us_is_holiday(&self.data_base).await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let open_time = chrono::NaiveTime::from_hms_opt(9, 30, 0)?;
        New_York
            .from_local_datetime(&date.and_time(open_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let close_time = chrono::NaiveTime::from_hms_opt(16, 0, 0)?;
        New_York
            .from_local_datetime(&date.and_time(close_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn local_today(&self) -> chrono::NaiveDate {
        Utc::now().with_timezone(&New_York).date_naive()
    }

    fn fx_spread_pct(&self) -> Decimal {
        self.base.fx_spread_pct
    }

    fn get_ws_key(&self, symbol: &str) -> String {
        // According to KIS Official Guide for Overseas Real-time:
        // D + (NAS or NYS or AMS) + Symbol
        let exchange = if symbol.len() == 3 || symbol == "QQQ" || symbol == "SPY" {
            "NAS"
        } else if symbol.len() == 1 || symbol.len() == 2 {
            "NYS"
        } else {
            "NAS"
        };
        format!("D{}{}", exchange, symbol)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Internal Functions
// ─────────────────────────────────────────────────────────────────────────────

async fn us_place_order(
    base: &UsMarketBase,
    req: UnifiedOrderRequest,
    adapter: &impl MarketAdapter,
) -> Result<UnifiedOrderResult, BotError> {
    base.throttler.wait().await;
    let exchange =
        UsMarketBase::exchange_from_hint(req.metadata.exchange_hint.as_deref(), &req.symbol);
    let adjusted_price = req
        .price
        .map(|p| adapter.adjust_aggressive_price(p, req.side, req.strength));

    let order_req = OverseasStockV1TradingOrderRequest {
        cano: base.cano.clone(),
        acnt_prdt_cd: base.acnt_prdt_cd.clone(),
        ovrs_excg_cd: exchange,
        pdno: req.symbol.clone(),
        ord_qty: req.qty.to_string(),
        ovrs_ord_unpr: adjusted_price.unwrap_or(Decimal::ZERO).to_string(),
        ..Default::default()
    };
    let trading = base.client.overseas().trading();
    let resp = match req.side {
        UnifiedSide::Buy => trading.overseas_stock_v1_trading_order_buy(order_req).await,
        UnifiedSide::Sell => {
            trading
                .overseas_stock_v1_trading_order_sell(order_req)
                .await
        }
    }
    .map_err(|e| BotError::ApiError {
        msg: format!("overseas order: {}", e),
    })?;

    let order_no = resp
        .output
        .as_ref()
        .map(|o| o.odno.clone())
        .unwrap_or_default();

    Ok(UnifiedOrderResult {
        internal_id: uuid::Uuid::new_v4().to_string(),
        broker_id: order_no,
        symbol: req.symbol,
        side: req.side,
        qty: req.qty,
        price: adjusted_price,
    })
}

async fn us_cancel_order(
    base: &UsMarketBase,
    order: &UnifiedUnfilledOrder,
) -> Result<bool, BotError> {
    base.throttler.wait().await;
    base.client
        .overseas()
        .trading()
        .overseas_stock_v1_trading_order_rvsecncl(OverseasStockV1TradingOrderRvsecnclRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ovrs_excg_cd: order
                .exchange_code
                .clone()
                .unwrap_or_else(|| "NASD".to_string()),
            pdno: order.symbol.clone(),
            orgn_odno: order.order_no.clone(),
            rvse_cncl_dvsn_cd: "02".to_string(),
            ord_qty: order.remaining_qty.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas cancel: {}", e),
        })?;
    Ok(true)
}

async fn us_unfilled_orders(base: &UsMarketBase) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
    // KIS inquire_nccs requires a specific exchange code — query all 3 and merge.
    let mut results = Vec::new();
    for excd in ["NASD", "NYSE", "AMEX"] {
        base.throttler.wait().await;
        let resp = base
            .client
            .overseas()
            .trading()
            .overseas_stock_v1_trading_inquire_nccs(OverseasStockV1TradingInquireNccsRequest {
                cano: base.cano.clone(),
                acnt_prdt_cd: base.acnt_prdt_cd.clone(),
                ovrs_excg_cd: excd.to_string(),
                ..Default::default()
            })
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("inquire_nccs({excd}): {}", e),
            })?;

        for o in &resp.output {
            results.push(UnifiedUnfilledOrder {
                order_no: o.odno.clone(),
                symbol: o.pdno.clone(),
                side: match o.sll_buy_dvsn_cd.as_str() {
                    "02" => UnifiedSide::Buy,
                    _ => UnifiedSide::Sell,
                },
                qty: o.ft_ord_qty.to_string().parse().unwrap_or(0),
                remaining_qty: o.nccs_qty.to_string().parse().unwrap_or(0),
                price: o.ft_ord_unpr3,
                exchange_code: Some(o.ovrs_excg_cd.clone()),
            });
        }
    }
    Ok(results)
}

async fn us_order_history(
    base: &UsMarketBase,
    _start_date: &str,
    _end_date: &str,
) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .overseas()
        .trading()
        .overseas_stock_v1_trading_inquire_ccnl(OverseasStockV1TradingInquireCcnlRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_ccnl: {}", e),
        })?;

    Ok(resp
        .output
        .iter()
        .map(|h| UnifiedOrderHistoryItem {
            order_no: h.odno.clone(),
            symbol: h.pdno.clone(),
            side: match h.sll_buy_dvsn_cd.as_str() {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: h.ft_ccld_qty.to_string().parse().unwrap_or(0),
            filled_qty: h.ft_ccld_qty.to_string().parse().unwrap_or(0),
            filled_price: h.ft_ccld_unpr3,
            price: h.ft_ord_unpr3,
            status: h.rvse_cncl_dvsn.clone(),
        })
        .collect())
}

async fn us_balance(base: &UsMarketBase) -> Result<UnifiedBalance, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .overseas()
        .trading()
        .overseas_stock_v1_trading_inquire_balance(OverseasStockV1TradingInquireBalanceRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ovrs_excg_cd: "NASD".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas balance: {}", e),
        })?;

    let positions = resp
        .output1
        .iter()
        .map(|item| UnifiedPosition {
            symbol: item.ovrs_pdno.clone(),
            name: Some(item.ovrs_item_name.clone()),
            qty: item.ovrs_cblc_qty,
            avg_price: item.pchs_avg_pric,
            current_price: item.now_pric2.parse().unwrap_or(Decimal::ZERO),
            unrealized_pnl: item.frcr_evlu_pfls_amt,
            pnl_pct: item.evlu_pfls_rt.to_string().parse().unwrap_or(0.0),
        })
        .collect();

    // inquire-balance/output2 has no deposit field; use inquire-present-balance for cash.
    base.throttler.wait().await;
    let pres_resp = base
        .client
        .overseas()
        .trading()
        .overseas_stock_v1_trading_inquire_present_balance(
            OverseasStockV1TradingInquirePresentBalanceRequest {
                cano: base.cano.clone(),
                acnt_prdt_cd: base.acnt_prdt_cd.clone(),
                wcrc_frcr_dvsn_cd: "01".to_string(),
                natn_cd: "840".to_string(),
                tr_mket_cd: "00".to_string(),
                inqr_dvsn_cd: "00".to_string(),
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas present-balance: {}", e),
        })?;

    let cash = pres_resp
        .output2
        .first()
        .map(|o| {
            o.frcr_dncl_amt_2
                .parse::<Decimal>()
                .unwrap_or(Decimal::ZERO)
        })
        .unwrap_or(Decimal::ZERO);

    Ok(UnifiedBalance {
        total_equity: cash,
        available_cash: cash,
        positions,
    })
}

async fn us_daily_chart(
    base: &UsMarketBase,
    symbol: &str,
    _days: u32,
) -> Result<Vec<UnifiedDailyBar>, BotError> {
    base.throttler.wait().await;
    let now = Utc::now().with_timezone(&New_York);

    let exchange = if symbol.len() == 3 || symbol == "QQQ" || symbol == "SPY" {
        "NAS".to_string()
    } else if symbol.len() == 1 || symbol.len() == 2 {
        "NYS".to_string()
    } else {
        "NAS".to_string() // Fallback
    };

    let yesterday = (now - chrono::Duration::days(1))
        .format("%Y%m%d")
        .to_string();

    let resp = base
        .client
        .overseas()
        .quotations()
        .overseas_price_v1_quotations_dailyprice(OverseasPriceV1QuotationsDailypriceRequest {
            excd: exchange,
            symb: symbol.to_string(),
            bymd: yesterday,
            gubn: "0".to_string(),
            modp: "1".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("us daily_chart: {}", e),
        })?;

    // output1 has only rsym/zdiv/nrec; no company name in dailyprice API
    let symbol_name: Option<String> = None;

    Ok(resp
        .output2
        .iter()
        .filter_map(|b| {
            chrono::NaiveDate::parse_from_str(&b.xymd, "%Y%m%d")
                .ok()
                .map(|date| UnifiedDailyBar {
                    symbol_name: symbol_name.clone(),
                    date,
                    open: b.open.parse().unwrap_or(Decimal::ZERO),
                    high: b.high.parse().unwrap_or(Decimal::ZERO),
                    low: b.low.parse().unwrap_or(Decimal::ZERO),
                    close: b.clos.parse().unwrap_or(Decimal::ZERO),
                    volume: b.tvol.parse().unwrap_or(0),
                })
        })
        .collect())
}

async fn us_volume_ranking(base: &UsMarketBase, count: u32) -> Result<Vec<String>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .overseas()
        .ranking()
        .overseas_stock_v1_ranking_trade_vol(OverseasStockV1RankingTradeVolRequest {
            excd: "NAS".to_string(),
            ..Default::default()
        })
        .await
        .map_err(BotError::from)
        .or_else(|e| e.handle_vts_error("us_volume_ranking"))?;

    Ok(resp
        .output2
        .iter()
        .take(count as usize)
        .filter_map(|i| {
            let s = i.symb.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect())
}

async fn us_is_holiday(base: &UsMarketBase) -> Result<bool, BotError> {
    base.throttler.wait().await;
    let today = Utc::now().with_timezone(&New_York).date_naive();
    Ok(is_nyse_holiday(today))
}

fn is_nyse_holiday(date: chrono::NaiveDate) -> bool {
    use chrono::Weekday;
    let (y, m, d) = (date.year(), date.month(), date.day());
    let weekday = date.weekday();

    // Weekends
    if weekday == Weekday::Sat || weekday == Weekday::Sun {
        return true;
    }

    // New Year's Day (Jan 1)
    if is_observed_holiday(y, 1, 1, date) {
        return true;
    }
    // If Jan 1 is a Saturday, it's observed on Dec 31 of previous year
    if m == 12 && d == 31 && weekday == Weekday::Fri {
        if let Some(next_jan1) = chrono::NaiveDate::from_ymd_opt(y + 1, 1, 1) {
            if next_jan1.weekday() == Weekday::Sat {
                return true;
            }
        }
    }

    // MLK Day (3rd Mon in Jan)
    if m == 1 && (15..=21).contains(&d) && weekday == Weekday::Mon {
        return true;
    }

    // Presidents' Day (3rd Mon in Feb)
    if m == 2 && (15..=21).contains(&d) && weekday == Weekday::Mon {
        return true;
    }

    // Good Friday (Friday before Easter Sunday)
    if let Some(good_friday) = get_good_friday(y) {
        if date == good_friday {
            return true;
        }
    }

    // Memorial Day (Last Mon in May)
    if m == 5 && (25..=31).contains(&d) && weekday == Weekday::Mon {
        return true;
    }

    // Juneteenth (Jun 19) - since 2021
    if y >= 2021 && is_observed_holiday(y, 6, 19, date) {
        return true;
    }

    // Independence Day (Jul 4)
    if is_observed_holiday(y, 7, 4, date) {
        return true;
    }

    // Labor Day (1st Mon in Sep)
    if m == 9 && (1..=7).contains(&d) && weekday == Weekday::Mon {
        return true;
    }

    // Thanksgiving (4th Thu in Nov)
    if m == 11 && (22..=28).contains(&d) && weekday == Weekday::Thu {
        return true;
    }

    // Christmas (Dec 25)
    if is_observed_holiday(y, 12, 25, date) {
        return true;
    }

    false
}

fn is_observed_holiday(y: i32, m: u32, d: u32, date: chrono::NaiveDate) -> bool {
    use chrono::Weekday;
    let holiday = match chrono::NaiveDate::from_ymd_opt(y, m, d) {
        Some(h) => h,
        None => return false,
    };
    let weekday = holiday.weekday();

    match weekday {
        Weekday::Sat => date == holiday.pred_opt().unwrap(), // Observed Friday
        Weekday::Sun => date == holiday.succ_opt().unwrap(), // Observed Monday
        _ => date == holiday,
    }
}

fn get_good_friday(year: i32) -> Option<chrono::NaiveDate> {
    // Meeus/Jones/Butcher algorithm for Easter Sunday
    let a = year % 19;
    let b = year / 100;
    let c = year % 100;
    let d = b / 4;
    let e = b % 4;
    let f = (b + 8) / 25;
    let g = (b - f + 1) / 3;
    let h = (19 * a + b - d - g + 15) % 30;
    let i = c / 4;
    let k = c % 4;
    let l = (32 + 2 * e + 2 * i - h - k) % 7;
    let m = (a + 11 * h + 22 * l) / 451;
    let month = (h + l - 7 * m + 114) / 31;
    let day = ((h + l - 7 * m + 114) % 31) + 1;

    chrono::NaiveDate::from_ymd_opt(year, month as u32, day as u32)?
        .pred_opt()? // Sat
        .pred_opt() // Fri
}

async fn us_intraday_candles(
    base: &UsMarketBase,
    symbol: &str,
    _interval_mins: u32,
) -> Result<Vec<UnifiedCandleBar>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .overseas()
        .quotations()
        .overseas_price_v1_quotations_inquire_time_itemchartprice(
            OverseasPriceV1QuotationsInquireTimeItemchartpriceRequest {
                excd: UsMarketBase::quotation_exchange_from_hint(None, symbol),
                symb: symbol.to_string(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("us intraday_candles: {}", e),
        })?;

    Ok(resp
        .output2
        .iter()
        .filter_map(|b| {
            let dt_str = format!("{} {}", b.xymd, b.khms);

            chrono::NaiveDateTime::parse_from_str(&dt_str, "%Y%m%d %H%M%S")
                .ok()
                .map(|naive| {
                    let dt = New_York
                        .from_local_datetime(&naive)
                        .unwrap()
                        .with_timezone(&Utc);
                    UnifiedCandleBar {
                        timestamp: dt,
                        open: b.open.parse().unwrap_or(Decimal::ZERO),
                        high: b.high.parse().unwrap_or(Decimal::ZERO),
                        low: b.low.parse().unwrap_or(Decimal::ZERO),
                        close: b.last.parse().unwrap_or(Decimal::ZERO),
                        volume: b.evol.parse().unwrap_or(0),
                    }
                })
        })
        .collect())
}

async fn us_current_price(base: &UsMarketBase, symbol: &str) -> Result<Decimal, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .overseas()
        .quotations()
        .overseas_price_v1_quotations_price(OverseasPriceV1QuotationsPriceRequest {
            excd: UsMarketBase::quotation_exchange_from_hint(None, symbol),
            symb: symbol.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("us current_price: {}", e),
        })?;

    resp.output
        .as_ref()
        .map(|o| {
            o.last.parse().map_err(|e| BotError::ApiError {
                msg: format!("parse last price: {}", e),
            })
        })
        .unwrap_or(Ok(Decimal::ZERO))
}

fn us_market_timing() -> MarketTiming {
    let now = Utc::now().with_timezone(&New_York);
    let today = now.date_naive();
    let is_holiday = is_nyse_holiday(today);

    let hour = now.hour();
    let minute = now.minute();
    let total_mins = (hour * 60 + minute) as i64;

    let open_mins = 9 * 60 + 30;
    let close_mins = 16 * 60;

    let is_open = !is_holiday && total_mins >= open_mins && total_mins < close_mins;

    let mins_since_open = if is_open { total_mins - open_mins } else { -1 };
    let mins_until_close = if is_open { close_mins - total_mins } else { -1 };

    let weekday = now.weekday();
    let mins_until_open = if is_holiday {
        // Very simplified: if holiday, assume next open is at least 1440 mins away
        // or just calculate based on days until Monday if it's a weekend.
        // For a more accurate value, we'd need to find the next business day.
        let days_to_mon = match weekday {
            chrono::Weekday::Sat => 2,
            chrono::Weekday::Sun => 1,
            _ => 1, // Holiday on weekday, assume tomorrow
        };
        (days_to_mon * 1440) + (open_mins - total_mins)
    } else if total_mins < open_mins {
        open_mins - total_mins
    } else {
        (1440 - total_mins) + open_mins
    };

    MarketTiming {
        is_open,
        mins_since_open,
        mins_until_close,
        mins_until_open,
        is_holiday,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_is_nyse_holiday() {
        // Weekends
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 12, 28).unwrap()
        )); // Saturday
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 12, 29).unwrap()
        )); // Sunday

        // Fixed holidays 2024
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 1, 1).unwrap()
        )); // New Year
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 6, 19).unwrap()
        )); // Juneteenth
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 7, 4).unwrap()
        )); // Independence Day
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 12, 25).unwrap()
        )); // Christmas

        // Moving holidays 2024
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap()
        )); // MLK Day
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 2, 19).unwrap()
        )); // Presidents' Day
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 3, 29).unwrap()
        )); // Good Friday
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 5, 27).unwrap()
        )); // Memorial Day
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 9, 2).unwrap()
        )); // Labor Day
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 11, 28).unwrap()
        )); // Thanksgiving

        // Observation rules
        // Jul 4, 2026 (Saturday) -> Observed on Jul 3
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2026, 7, 3).unwrap()
        ));
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2026, 7, 4).unwrap()
        )); // It's Saturday anyway

        // Jul 4, 2027 (Sunday) -> Observed on Jul 5
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2027, 7, 4).unwrap()
        )); // It's Sunday anyway
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2027, 7, 5).unwrap()
        ));

        // Jan 1, 2022 (Saturday) -> Observed on Dec 31, 2021
        assert!(is_nyse_holiday(
            NaiveDate::from_ymd_opt(2021, 12, 31).unwrap()
        ));

        // Business days
        assert!(!is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 4, 29).unwrap()
        )); // Monday
        assert!(!is_nyse_holiday(
            NaiveDate::from_ymd_opt(2024, 12, 24).unwrap()
        )); // Xmas Eve (Open)
    }

    #[test]
    fn test_good_friday() {
        assert_eq!(
            get_good_friday(2024),
            Some(NaiveDate::from_ymd_opt(2024, 3, 29).unwrap())
        );
        assert_eq!(
            get_good_friday(2025),
            Some(NaiveDate::from_ymd_opt(2025, 4, 18).unwrap())
        );
        assert_eq!(
            get_good_friday(2026),
            Some(NaiveDate::from_ymd_opt(2026, 4, 3).unwrap())
        );
    }
}
