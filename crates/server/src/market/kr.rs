//! Korean market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Datelike, TimeZone, Timelike, Utc};
use chrono_tz::Asia::Seoul;
use kis_api::models::*;
use kis_api::KisClient;
use rust_decimal::Decimal;

use crate::shared::throttler::KisThrottler;
use std::sync::Arc;

/// Base logic for Korean market shared by Real and VTS adapters.
struct KrMarketBase {
    client: KisClient,
    cano: String,
    acnt_prdt_cd: String,
    throttler: Arc<KisThrottler>,
}

impl KrMarketBase {
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
            throttler,
        }
    }

    async fn confirm_fill_from_history(
        &self,
        broker_order_no: &str,
        submitted_qty: u64,
    ) -> PollOutcome {
        let today = Utc::now()
            .with_timezone(&Seoul)
            .format("%Y%m%d")
            .to_string();
        match kr_order_history(self, &today, &today).await {
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
                    "KrMarketBase: order_history error for {}: {}",
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

/// Korean Real Market Adapter.
pub struct KrRealAdapter {
    base: KrMarketBase,
}

impl KrRealAdapter {
    pub fn new(client: KisClient, throttler: Arc<KisThrottler>) -> Self {
        Self {
            base: KrMarketBase::new(
                client,
                std::env::var("KIS_ACCOUNT_NO").unwrap_or_default(),
                std::env::var("KIS_ACCOUNT_CD").unwrap_or_else(|_| "01".to_string()),
                throttler,
            ),
        }
    }
}

#[async_trait]
impl MarketAdapter for KrRealAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::Kr
    }

    fn name(&self) -> &'static str {
        "KR-Real"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        kr_place_order(&self.base, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        kr_cancel_order(&self.base, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        kr_unfilled_orders(&self.base).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        kr_order_history(&self.base, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match kr_unfilled_orders(&self.base).await {
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
        kr_balance(&self.base).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        kr_daily_chart(&self.base, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        kr_intraday_candles(&self.base, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        kr_current_price(&self.base, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        kr_volume_ranking(&self.base, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        kr_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        kr_is_holiday(&self.base).await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let open_time = chrono::NaiveTime::from_hms_opt(9, 0, 0)?;
        Seoul
            .from_local_datetime(&date.and_time(open_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let close_time = chrono::NaiveTime::from_hms_opt(15, 30, 0)?;
        Seoul
            .from_local_datetime(&date.and_time(close_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn local_today(&self) -> chrono::NaiveDate {
        Utc::now().with_timezone(&Seoul).date_naive()
    }
}

/// Korean VTS Market Adapter.
pub struct KrVtsAdapter {
    /// Base for trading (orders, balance) - uses VTS client
    base: KrMarketBase,
    /// Base for data (price, ranking) - uses Real client
    data_base: KrMarketBase,
}

impl KrVtsAdapter {
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
            base: KrMarketBase::new(
                vts_client,
                cano.clone(),
                acnt_prdt_cd.clone(),
                throttler.clone(),
            ),
            data_base: KrMarketBase::new(real_client, cano, acnt_prdt_cd, throttler),
        }
    }
}

#[async_trait]
impl MarketAdapter for KrVtsAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::KrVts
    }

    fn name(&self) -> &'static str {
        "KR-VTS"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        kr_place_order(&self.base, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        kr_cancel_order(&self.base, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        kr_unfilled_orders(&self.base).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        kr_order_history(&self.base, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match kr_unfilled_orders(&self.base).await {
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
        kr_balance(&self.base).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        kr_daily_chart(&self.data_base, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        kr_intraday_candles(&self.data_base, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        kr_current_price(&self.data_base, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        kr_volume_ranking(&self.data_base, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        kr_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        kr_is_holiday(&self.data_base).await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let open_time = chrono::NaiveTime::from_hms_opt(9, 0, 0)?;
        Seoul
            .from_local_datetime(&date.and_time(open_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        let close_time = chrono::NaiveTime::from_hms_opt(15, 30, 0)?;
        Seoul
            .from_local_datetime(&date.and_time(close_time))
            .earliest()
            .map(|dt| dt.with_timezone(&Utc))
    }

    fn local_today(&self) -> chrono::NaiveDate {
        Utc::now().with_timezone(&Seoul).date_naive()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Internal Functions
// ─────────────────────────────────────────────────────────────────────────────

async fn kr_daily_chart(
    base: &KrMarketBase,
    symbol: &str,
    _days: u32,
) -> Result<Vec<UnifiedDailyBar>, BotError> {
    base.throttler.wait().await;
    let now = Utc::now().with_timezone(&Seoul);
    let today = now.format("%Y%m%d").to_string();
    // 150일치 데이터를 얻기 위해 주말 포함 200일 전부터 조회
    let start_date = (now - chrono::Duration::days(200))
        .format("%Y%m%d")
        .to_string();

    let resp = base
        .client
        .stock()
        .quotations()
        .domestic_stock_v1_quotations_inquire_daily_itemchartprice(
            DomesticStockV1QuotationsInquireDailyItemchartpriceRequest {
                fid_cond_mrkt_div_code: "J".to_string(),
                fid_input_iscd: symbol.to_string(),
                fid_input_date_1: start_date,
                fid_input_date_2: today,
                fid_period_div_code: "D".to_string(),
                fid_org_adj_prc: "0".to_string(),
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr daily_chart: {}", e),
        })?;

    let symbol_name = resp.output1.first().map(|o| o.hts_kor_isnm.clone());

    Ok(resp
        .output2
        .iter()
        .filter_map(|b| {
            chrono::NaiveDate::parse_from_str(&b.stck_bsop_date, "%Y%m%d")
                .ok()
                .map(|date| UnifiedDailyBar {
                    symbol_name: symbol_name.clone(),
                    date,
                    open: b.stck_oprc.parse().unwrap_or(Decimal::ZERO),
                    high: b.stck_hgpr.parse().unwrap_or(Decimal::ZERO),
                    low: b.stck_lwpr.parse().unwrap_or(Decimal::ZERO),
                    close: b.stck_clpr.parse().unwrap_or(Decimal::ZERO),
                    volume: b.acml_vol.to_string().parse().unwrap_or(0),
                })
        })
        .collect())
}

async fn kr_place_order(
    base: &KrMarketBase,
    req: UnifiedOrderRequest,
    adapter: &impl MarketAdapter,
) -> Result<UnifiedOrderResult, BotError> {
    base.throttler.wait().await;
    let adjusted_price = req
        .price
        .map(|p| adapter.adjust_aggressive_price(p, req.side, req.strength));

    let order_req = DomesticStockV1TradingOrderCashRequest {
        cano: base.cano.clone(),
        acnt_prdt_cd: base.acnt_prdt_cd.clone(),
        pdno: req.symbol.clone(),
        ord_dvsn: "00".to_string(), // 지정가
        ord_qty: req.qty.to_string(),
        ord_unpr: adjusted_price.unwrap_or(Decimal::ZERO).to_string(),
        ..Default::default()
    };
    let trading = base.client.stock().trading();
    let resp = match req.side {
        UnifiedSide::Buy => {
            trading
                .domestic_stock_v1_trading_order_cash_buy(order_req)
                .await
        }
        UnifiedSide::Sell => {
            trading
                .domestic_stock_v1_trading_order_cash_sell(order_req)
                .await
        }
    }
    .map_err(|e| BotError::ApiError {
        msg: format!("order_cash: {}", e),
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

async fn kr_cancel_order(
    base: &KrMarketBase,
    order: &UnifiedUnfilledOrder,
) -> Result<bool, BotError> {
    base.throttler.wait().await;
    base.client
        .stock()
        .trading()
        .domestic_stock_v1_trading_order_rvsecncl(DomesticStockV1TradingOrderRvsecnclRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            orgn_odno: order.order_no.clone(),
            rvse_cncl_dvsn_cd: "02".to_string(), // 취소
            ord_qty: order.remaining_qty.to_string(),
            ord_unpr: Decimal::ZERO.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("order_rvsecncl: {}", e),
        })?;
    Ok(true)
}

async fn kr_unfilled_orders(base: &KrMarketBase) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .stock()
        .trading()
        .domestic_stock_v1_trading_inquire_psbl_rvsecncl(
            DomesticStockV1TradingInquirePsblRvsecnclRequest {
                cano: base.cano.clone(),
                acnt_prdt_cd: base.acnt_prdt_cd.clone(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_psbl_rvsecncl: {}", e),
        })?;

    Ok(resp
        .output
        .iter()
        .map(|o| UnifiedUnfilledOrder {
            order_no: o.odno.clone(),
            symbol: o.pdno.clone(),
            side: match o.sll_buy_dvsn_cd.as_str() {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: o.ord_qty.to_string().parse().unwrap_or(0),
            remaining_qty: o.psbl_qty.to_string().parse().unwrap_or(0),
            price: o.ord_unpr,
            exchange_code: None,
        })
        .collect())
}

async fn kr_order_history(
    base: &KrMarketBase,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .stock()
        .trading()
        .domestic_stock_v1_trading_inquire_daily_ccld_recent(
            DomesticStockV1TradingInquireDailyCcldRequest {
                cano: base.cano.clone(),
                acnt_prdt_cd: base.acnt_prdt_cd.clone(),
                inqr_strt_dt: start_date.to_string(),
                inqr_end_dt: end_date.to_string(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_daily_ccld: {}", e),
        })?;

    Ok(resp
        .output1
        .iter()
        .map(|h| UnifiedOrderHistoryItem {
            order_no: h.odno.clone(),
            symbol: h.pdno.clone(),
            side: match h.sll_buy_dvsn_cd.as_str() {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: h.ord_qty.to_string().parse().unwrap_or(0),
            filled_qty: h.tot_ccld_qty.to_string().parse().unwrap_or(0),
            filled_price: h.avg_prvs.parse().unwrap_or(Decimal::ZERO),
            price: h.ord_unpr,
            status: h.cncl_yn.clone(),
        })
        .collect())
}

async fn kr_balance(base: &KrMarketBase) -> Result<UnifiedBalance, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .stock()
        .trading()
        .domestic_stock_v1_trading_inquire_balance(DomesticStockV1TradingInquireBalanceRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr balance: {}", e),
        })?;

    let positions = resp
        .output1
        .iter()
        .map(|item| UnifiedPosition {
            symbol: item.pdno.clone(),
            name: Some(item.prdt_name.clone()),
            qty: item.hldg_qty,
            avg_price: item.pchs_avg_pric,
            current_price: item.prpr,
            unrealized_pnl: item.evlu_pfls_amt,
            pnl_pct: item.evlu_pfls_rt.to_string().parse().unwrap_or(0.0),
        })
        .collect();

    let cash = resp
        .output2
        .first()
        .map(|o| o.dnca_tot_amt)
        .unwrap_or(Decimal::ZERO);

    Ok(UnifiedBalance {
        total_equity: cash,
        available_cash: cash,
        positions,
    })
}

async fn kr_intraday_candles(
    base: &KrMarketBase,
    symbol: &str,
    interval_mins: u32,
) -> Result<Vec<UnifiedCandleBar>, BotError> {
    base.throttler.wait().await;
    if interval_mins != 1 {
        tracing::warn!(
            "kr_intraday_candles: interval_mins={} ignored, only 1-min bars supported",
            interval_mins
        );
    }

    let resp = base
        .client
        .stock()
        .quotations()
        .domestic_stock_v1_quotations_inquire_time_itemchartprice(
            DomesticStockV1QuotationsInquireTimeItemchartpriceRequest {
                fid_cond_mrkt_div_code: "J".to_string(),
                fid_input_iscd: symbol.to_string(),
                ..Default::default()
            },
        )
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr intraday_candles: {}", e),
        })?;

    let today = Utc::now()
        .with_timezone(&Seoul)
        .format("%Y%m%d")
        .to_string();

    Ok(resp
        .output2
        .iter()
        .filter_map(|b| {
            let date_str = if b.stck_bsop_date.is_empty() {
                today.as_str()
            } else {
                b.stck_bsop_date.as_str()
            };
            let dt_str = format!("{} {}", date_str, b.stck_cntg_hour);

            chrono::NaiveDateTime::parse_from_str(&dt_str, "%Y%m%d %H%M%S")
                .ok()
                .map(|naive| {
                    let dt = Seoul
                        .from_local_datetime(&naive)
                        .unwrap()
                        .with_timezone(&Utc);
                    UnifiedCandleBar {
                        timestamp: dt,
                        open: b.stck_oprc.parse().unwrap_or(Decimal::ZERO),
                        high: b.stck_hgpr.parse().unwrap_or(Decimal::ZERO),
                        low: b.stck_lwpr.parse().unwrap_or(Decimal::ZERO),
                        close: b.stck_prpr.parse().unwrap_or(Decimal::ZERO),
                        volume: b.cntg_vol.to_string().parse().unwrap_or(0),
                    }
                })
        })
        .collect())
}

async fn kr_current_price(base: &KrMarketBase, symbol: &str) -> Result<Decimal, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .stock()
        .quotations()
        .domestic_stock_v1_quotations_inquire_price(DomesticStockV1QuotationsInquirePriceRequest {
            fid_cond_mrkt_div_code: "J".to_string(),
            fid_input_iscd: symbol.to_string(),
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr current_price: {}", e),
        })?;

    resp.output
        .as_ref()
        .map(|o| {
            o.stck_prpr.parse().map_err(|e| BotError::ApiError {
                msg: format!("parse stck_prpr: {}", e),
            })
        })
        .unwrap_or(Ok(Decimal::ZERO))
}

async fn kr_volume_ranking(base: &KrMarketBase, count: u32) -> Result<Vec<String>, BotError> {
    base.throttler.wait().await;
    let resp = base
        .client
        .stock()
        .quotations()
        .domestic_stock_v1_quotations_volume_rank(DomesticStockV1QuotationsVolumeRankRequest {
            fid_cond_mrkt_div_code: "J".to_string(),
            fid_cond_scr_div_code: "20171".to_string(),
            fid_input_iscd: "0000".to_string(),
            fid_div_cls_code: "0".to_string(),
            fid_blng_cls_code: "0".to_string(),
            fid_trgt_cls_code: "0".to_string(),
            fid_trgt_exls_cls_code: "0".to_string(),
            fid_input_price_1: "".to_string(),
            fid_input_price_2: "".to_string(),
            fid_vol_cnt: "".to_string(),
            ..Default::default()
        })
        .await
        .map_err(BotError::from)?;

    Ok(resp
        .output
        .iter()
        .take(count as usize)
        .filter_map(|i| {
            let s = i.mksc_shrn_iscd.trim();
            if s.is_empty() {
                None
            } else {
                Some(s.to_string())
            }
        })
        .collect())
}

async fn kr_is_holiday(base: &KrMarketBase) -> Result<bool, BotError> {
    base.throttler.wait().await;
    let today = Utc::now()
        .with_timezone(&Seoul)
        .format("%Y%m%d")
        .to_string();
    let resp = base
        .client
        .stock()
        .quotations()
        .domestic_stock_v1_quotations_chk_holiday(DomesticStockV1QuotationsChkHolidayRequest {
            bass_dt: today,
            ctx_area_fk: "".to_string(),
            ctx_area_nk: "".to_string(),
        })
        .await
        .map_err(BotError::from)?;

    Ok(resp.output.iter().any(|h| h.bzdy_yn == "N"))
}

fn kr_market_timing() -> MarketTiming {
    let now = Utc::now().with_timezone(&Seoul);
    let hour = now.hour();
    let minute = now.minute();
    let total_mins = (hour * 60 + minute) as i64;

    let open_mins = 9 * 60;
    let close_mins = 15 * 60 + 30;

    let weekday = now.weekday();
    let is_weekend = matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun);

    let is_open = !is_weekend && total_mins >= open_mins && total_mins < close_mins;

    let mins_since_open = if is_open { total_mins - open_mins } else { -1 };
    let mins_until_close = if is_open { close_mins - total_mins } else { -1 };
    let mins_until_open = if is_weekend {
        // 주말일 경우 월요일 09:00까지 (단순화: 실제로는 요일 계산 필요하나 로그용으로 적절히 처리)
        let days_to_mon = match weekday {
            chrono::Weekday::Sat => 2,
            chrono::Weekday::Sun => 1,
            _ => 0,
        };
        (days_to_mon * 1440) + (open_mins - total_mins)
    } else if total_mins < open_mins {
        open_mins - total_mins
    } else {
        // 오늘 장 종료 후 내일 아침까지
        (1440 - total_mins) + open_mins
    };

    MarketTiming {
        is_open,
        mins_since_open,
        mins_until_close,
        mins_until_open,
        is_holiday: false,
    }
}
