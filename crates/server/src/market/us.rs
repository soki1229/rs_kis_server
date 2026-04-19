//! US market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Datelike, Timelike, Utc};
use chrono_tz::America::New_York;
use kis_api::models::*;
use kis_api::KisClient;
use rust_decimal::Decimal;

/// Base logic for US market shared by Real and VTS adapters.
struct UsMarketBase {
    client: KisClient,
    cano: String,
    acnt_prdt_cd: String,
    fx_spread_pct: Decimal,
}

impl UsMarketBase {
    fn new(client: KisClient, cano: String, acnt_prdt_cd: String) -> Self {
        Self {
            client,
            cano,
            acnt_prdt_cd,
            fx_spread_pct: Decimal::new(5, 3),
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
    pub fn new(client: KisClient) -> Self {
        Self {
            base: UsMarketBase::new(
                client,
                std::env::var("KIS_ACCOUNT_NO").unwrap_or_default(),
                std::env::var("KIS_ACCOUNT_CD").unwrap_or_else(|_| "01".to_string()),
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

    fn fx_spread_pct(&self) -> Decimal {
        self.base.fx_spread_pct
    }
}

/// US VTS Market Adapter.
pub struct UsVtsAdapter {
    base: UsMarketBase,
}

impl UsVtsAdapter {
    pub fn new(client: KisClient) -> Self {
        let cano = std::env::var("KIS_VTS_ACCOUNT_NO")
            .or_else(|_| std::env::var("KIS_ACCOUNT_NO"))
            .unwrap_or_default();
        let acnt_prdt_cd = std::env::var("KIS_VTS_ACCOUNT_CD")
            .or_else(|_| std::env::var("KIS_ACCOUNT_CD"))
            .unwrap_or_else(|_| "01".to_string());
        Self {
            base: UsMarketBase::new(client, cano, acnt_prdt_cd),
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

    fn fx_spread_pct(&self) -> Decimal {
        self.base.fx_spread_pct
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
    let exchange =
        UsMarketBase::exchange_from_hint(req.metadata.exchange_hint.as_deref(), &req.symbol);
    let adjusted_price = req
        .price
        .map(|p| adapter.adjust_aggressive_price(p, req.side, req.strength));

    let resp = base
        .client
        .overseas()
        .trading()
        .order_next(OrderNextRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ovrs_excg_cd: exchange.to_string(),
            pdno: req.symbol.clone(),
            ord_qty: req.qty.to_string(),
            ovrs_ord_unpr: adjusted_price.map(|p| p.to_string()).unwrap_or_default(),
            ord_dvsn: "00".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas order: {}", e),
        })?;

    let order_no = resp["output"]["odno"].as_str().unwrap_or("").to_string();

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
    base.client
        .overseas()
        .trading()
        .order_rvsecncl_next(OrderRvsecnclNextNextRequest {
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
            ovrs_ord_unpr: "0".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas cancel: {}", e),
        })?;
    Ok(true)
}

async fn us_unfilled_orders(base: &UsMarketBase) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
    let resp = base
        .client
        .overseas()
        .trading()
        .inquire_nccs(InquireNccsRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ovrs_excg_cd: "NASD".to_string(),
            sort_sqn: "DS".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_nccs: {}", e),
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
            qty: o["ft_ord_qty"].as_str().unwrap_or("0").parse().unwrap_or(0),
            remaining_qty: o["ft_ccld_qty"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            price: o["ft_ord_unpr3"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            exchange_code: Some(o["ovrs_excg_cd"].as_str().unwrap_or("NASD").to_string()),
        })
        .collect())
}

async fn us_order_history(
    base: &UsMarketBase,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
    let resp = base
        .client
        .overseas()
        .trading()
        .inquire_ccnl_next(InquireCcnlNextNextRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            pdno: "%".to_string(),
            ord_strt_dt: start_date.to_string(),
            ord_end_dt: end_date.to_string(),
            sll_buy_dvsn: "00".to_string(),
            ccld_nccs_dvsn: "00".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("inquire_ccnl_next: {}", e),
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
            qty: h["ft_ord_qty"].as_str().unwrap_or("0").parse().unwrap_or(0),
            filled_qty: h["ft_ccld_qty"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(0),
            price: Decimal::ZERO,
            filled_price: h["ft_ccld_unpr3"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            status: String::new(),
        })
        .collect())
}

async fn us_balance(base: &UsMarketBase) -> Result<UnifiedBalance, BotError> {
    let resp = base
        .client
        .overseas()
        .trading()
        .inquire_balance_next(InquireBalanceNextNextNextRequest {
            cano: base.cano.clone(),
            acnt_prdt_cd: base.acnt_prdt_cd.clone(),
            ovrs_excg_cd: "NASD".to_string(),
            tr_crcy_cd: "USD".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("overseas balance: {}", e),
        })?;

    let positions = resp["output1"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .map(|item| UnifiedPosition {
            symbol: item["ovrs_pdno"].as_str().unwrap_or("").to_string(),
            name: Some(item["ovrs_item_name"].as_str().unwrap_or("").to_string()),
            qty: item["ovrs_cblc_qty"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            avg_price: item["pchs_avg_pric"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            current_price: item["now_pric2"]
                .as_str()
                .unwrap_or("0")
                .parse()
                .unwrap_or(Decimal::ZERO),
            unrealized_pnl: item["frcr_evlu_pfls_amt"]
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

    let cash = resp["output2"][0]["frcr_dncl_amt_2"]
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

async fn us_daily_chart(
    base: &UsMarketBase,
    symbol: &str,
    _days: u32,
) -> Result<Vec<UnifiedDailyBar>, BotError> {
    let resp = base
        .client
        .overseas()
        .quotations()
        .dailyprice(DailypriceRequest {
            excd: UsMarketBase::exchange_from_hint(None, symbol),
            symb: symbol.to_string(),
            gubn: "0".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("us dailyprice: {}", e),
        })?;

    Ok(resp["output2"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .filter_map(|b| {
            chrono::NaiveDate::parse_from_str(b["xymd"].as_str()?, "%Y%m%d")
                .ok()
                .map(|date| UnifiedDailyBar {
                    date,
                    open: b["open"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    high: b["high"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    low: b["low"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    close: b["clos"]
                        .as_str()
                        .unwrap_or("0")
                        .parse()
                        .unwrap_or(Decimal::ZERO),
                    volume: b["tvol"].as_str().unwrap_or("0").parse().unwrap_or(0),
                })
        })
        .collect())
}

async fn us_volume_ranking(base: &UsMarketBase, count: u32) -> Result<Vec<String>, BotError> {
    let resp = base
        .client
        .overseas()
        .ranking()
        .trade_vol(TradeVolRequest {
            excd: "NASD".to_string(),
            nday: "0".to_string(),
            vol_rang: "0".to_string(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("us trade_vol: {}", e),
        })?;

    Ok(resp["output"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .take(count as usize)
        .filter_map(|i| i["symb"].as_str().map(|s| s.to_string()))
        .collect())
}

async fn us_is_holiday(base: &UsMarketBase) -> Result<bool, BotError> {
    let today = Utc::now()
        .with_timezone(&New_York)
        .format("%Y%m%d")
        .to_string();
    let resp = base
        .client
        .overseas()
        .quotations()
        .countries_holiday(CountriesHolidayRequest {
            trad_dt: today.clone(),
            ..Default::default()
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("countries_holiday: {}", e),
        })?;

    Ok(resp["output"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .iter()
        .any(|h| h["bzdy_yn"].as_str().unwrap_or("Y") == "N"))
}

async fn us_intraday_candles(
    _base: &UsMarketBase,
    symbol: &str,
    _interval_mins: u32,
) -> Result<Vec<UnifiedCandleBar>, BotError> {
    tracing::warn!("intraday_candles not implemented for US: {}", symbol);
    Ok(vec![])
}

async fn us_current_price(_base: &UsMarketBase, _symbol: &str) -> Result<Decimal, BotError> {
    Err(BotError::ApiError {
        msg: "current_price not implemented for US market".to_string(),
    })
}

fn us_market_timing() -> MarketTiming {
    let now = Utc::now().with_timezone(&New_York);
    let hour = now.hour();
    let minute = now.minute();
    let total_mins = (hour * 60 + minute) as i64;

    let open_mins = 9 * 60 + 30;
    let close_mins = 16 * 60;

    let weekday = now.weekday();
    let is_weekend = matches!(weekday, chrono::Weekday::Sat | chrono::Weekday::Sun);

    let is_open = !is_weekend && total_mins >= open_mins && total_mins < close_mins;
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
