//! Korean market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use chrono_tz::Asia::Seoul;
use kis_api::models::*;
use kis_api::KisClient;
use rust_decimal::Decimal;

/// Base logic for Korean market shared by Real and VTS adapters.
struct KrMarketBase {
    client: KisClient,
    cano: String,
    acnt_prdt_cd: String,
}

impl KrMarketBase {
    fn new(client: KisClient) -> Self {
        Self {
            cano: std::env::var("KIS_ACCOUNT_NO").unwrap_or_default(),
            acnt_prdt_cd: std::env::var("KIS_ACCOUNT_CD").unwrap_or_else(|_| "01".to_string()),
            client,
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
    pub fn new(client: KisClient) -> Self {
        Self {
            base: KrMarketBase::new(client),
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
}

/// Korean VTS Market Adapter.
pub struct KrVtsAdapter {
    base: KrMarketBase,
}

impl KrVtsAdapter {
    pub fn new(client: KisClient) -> Self {
        Self {
            base: KrMarketBase::new(client),
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
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Internal Functions
// ─────────────────────────────────────────────────────────────────────────────

async fn kr_place_order(
    base: &KrMarketBase,
    req: UnifiedOrderRequest,
    adapter: &impl MarketAdapter,
) -> Result<UnifiedOrderResult, BotError> {
    let adjusted_price = req
        .price
        .map(|p| adapter.adjust_aggressive_price(p, req.side, req.strength));

    let resp = base
        .client
        .stock()
        .trading()
        .order_cash(OrderCashRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            pdno: req.symbol.clone(),
            ord_dvsn: "00".to_string(), // 지정가
            ord_qty: req.qty.to_string(),
            ord_unpr: adjusted_price.map(|p| p.to_string()).unwrap_or_default(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("order_cash: {}", e),
        })?;

    let order_no = resp["output"]["KRX_FWDG_ORD_ORGNO"]
        .as_str()
        .unwrap_or("")
        .to_string();

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
    base.client
        .stock()
        .trading()
        .order_rvsecncl(OrderRvsecnclRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            orgn_odno: order.order_no.clone(),
            rvse_cncl_dvsn_cd: "02".to_string(), // 취소
            ord_qty: order.remaining_qty.to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("order_rvsecncl: {}", e),
        })?;
    Ok(true)
}

async fn kr_unfilled_orders(base: &KrMarketBase) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
    let resp = base
        .client
        .stock()
        .trading()
        .inquire_psbl_rvsecncl(InquirePsblRvsecnclRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_psbl_rvsecncl: {}", e),
        })?;

    Ok(resp["output"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|o| UnifiedUnfilledOrder {
            order_no: o["odno"].as_str().unwrap_or("").to_string(),
            symbol: o["pdno"].as_str().unwrap_or("").to_string(),
            side: match o["sll_buy_dvsn_cd"].as_str().unwrap_or("") {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: o["ord_qty"].as_str().unwrap_or("0").parse().unwrap_or(0),
            remaining_qty: o["psbl_qty"].as_str().unwrap_or("0").parse().unwrap_or(0),
            price: o["ord_unpr"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            exchange_code: match o["mksc_shrn_iscd"].as_str().unwrap_or("") {
                s if s.starts_with("KSQ") => Some("Q".to_string()),
                _ => Some("J".to_string()),
            },
        })
        .collect())
}

async fn kr_order_history(
    base: &KrMarketBase,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
    let resp = base
        .client
        .stock()
        .trading()
        .inquire_daily_ccld(InquireDailyCcldRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            inqr_strt_dt: start_date.to_string(),
            inqr_end_dt: end_date.to_string(),
            sll_buy_dvsn_cd: "00".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_daily_ccld: {}", e),
        })?;

    Ok(resp["output1"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|h| UnifiedOrderHistoryItem {
            order_no: h["odno"].as_str().unwrap_or("").to_string(),
            symbol: h["pdno"].as_str().unwrap_or("").to_string(),
            side: match h["sll_buy_dvsn_cd"].as_str().unwrap_or("") {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: h["ord_qty"].as_str().unwrap_or("0").parse().unwrap_or(0),
            filled_qty: h["tot_ccld_qty"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            price: Decimal::ZERO,
            filled_price: h["avg_prvs"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            status: String::new(),
        })
        .collect())
}

async fn kr_balance(base: &KrMarketBase) -> Result<UnifiedBalance, BotError> {
    let resp = base
        .client
        .stock()
        .trading()
        .inquire_balance(InquireBalanceRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_balance: {}", e),
        })?;

    let positions = resp["output1"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|item| UnifiedPosition {
            symbol: item["pdno"].as_str().unwrap_or("").to_string(),
            name: Some(item["prdt_name"].as_str().unwrap_or("").to_string()),
            qty: item["hldg_qty"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            avg_price: item["pchs_avg_pric"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            current_price: item["prpr"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            unrealized_pnl: item["evlu_pfls_amt"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            pnl_pct: item["evlu_pfls_rt"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0.0),
        })
        .collect();

    let cash = resp["output2"][0]["dnca_tot_amt"]
        .as_str()
        .unwrap_or("0")
        .parse()
        .unwrap_or(Decimal::ZERO);

    Ok(UnifiedBalance {
        total_equity: cash,
        available_cash: cash,
        positions,
    })
}

async fn kr_daily_chart(
    base: &KrMarketBase,
    symbol: &str,
    _days: u32,
) -> Result<Vec<UnifiedDailyBar>, BotError> {
    let resp = base
        .client
        .stock()
        .quotations()
        .inquire_daily_itemchartprice(InquireDailyItemchartpriceRequest {
            fid_cond_mrkt_div_code: "J".to_string(),
            fid_input_iscd: symbol.to_string(),
            fid_period_div_code: "D".to_string(),
            fid_org_adj_prc: "1".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr daily_chart: {}", e),
        })?;

    Ok(resp["output2"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|b| {
            chrono::NaiveDate::parse_from_str(b["stck_bsop_date"].as_str()?, "%Y%m%d")
                .ok()
                .map(|date| UnifiedDailyBar {
                    date,
                    open: b["stck_oprc"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    high: b["stck_hgpr"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    low: b["stck_lwpr"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    close: b["stck_clpr"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    volume: b["acml_vol"].as_str().unwrap_or("0").parse().unwrap_or(0),
                })
        })
        .collect())
}

async fn kr_volume_ranking(base: &KrMarketBase, count: u32) -> Result<Vec<String>, BotError> {
    let resp = base
        .client
        .stock()
        .quotations()
        .volume_rank(VolumeRankNextRequest {
            fid_cond_mrkt_div_code: "J".to_string(),
            fid_cond_scr_div_code: "20171".to_string(),
            fid_input_iscd: "0000".to_string(),
            fid_div_cls_code: "0".to_string(),
            fid_blng_cls_code: "0".to_string(),
            fid_trgt_cls_code: "111111111".to_string(),
            fid_trgt_exls_cls_code: "0000000000".to_string(),
            fid_vol_cnt: count.to_string(),
            fid_input_price_1: "".to_string(),
            fid_input_price_2: "".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("kr volume_rank: {}", e),
        })?;

    Ok(resp["output"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|i| i["mksc_shrn_iscd"].as_str().map(|s| s.to_string()))
        .collect())
}

async fn kr_is_holiday(_base: &KrMarketBase) -> Result<bool, BotError> {
    // TODO: KIS 국내 휴일조회 API 연동 필요
    Ok(false)
}

async fn kr_intraday_candles(
    _base: &KrMarketBase,
    symbol: &str,
    _interval_mins: u32,
) -> Result<Vec<UnifiedCandleBar>, BotError> {
    tracing::warn!("intraday_candles not implemented for KR: {}", symbol);
    Ok(vec![])
}

async fn kr_current_price(_base: &KrMarketBase, _symbol: &str) -> Result<Decimal, BotError> {
    Err(BotError::ApiError {
        msg: "current_price not implemented for KR".to_string(),
    })
}

fn kr_market_timing() -> MarketTiming {
    let now = Utc::now().with_timezone(&Seoul);
    let hour = now.hour();
    let minute = now.minute();
    let total_mins = (hour * 60 + minute) as i64;

    let open_mins = 9 * 60;
    let close_mins = 15 * 60 + 30;

    let is_open = total_mins >= open_mins && total_mins < close_mins;
    let mins_since_open = if total_mins >= open_mins {
        total_mins - open_mins
    } else {
        i64::MAX
    };
    let mins_until_close = if total_mins < close_mins {
        close_mins - total_mins
    } else {
        i64::MAX
    };

    MarketTiming {
        is_open,
        mins_since_open,
        mins_until_close,
        is_holiday: false,
    }
}
