//! Korean market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use chrono_tz::Asia::Seoul;
use kis_api::{
    ChartPeriod, DomesticCancelOrderRequest, DomesticDailyChartRequest, DomesticExchange,
    DomesticOrderHistoryRequest, DomesticPlaceOrderRequest, KisDomesticApi,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;

/// Base logic for Korean market shared by Real and VTS adapters.
struct KrMarketBase {
    client: Arc<dyn KisDomesticApi>,
}

impl KrMarketBase {
    fn new(client: Arc<dyn KisDomesticApi>) -> Self {
        Self { client }
    }

    #[allow(dead_code)]
    fn exchange_from_code(code: Option<&str>) -> DomesticExchange {
        match code {
            Some("Q") => DomesticExchange::KOSDAQ,
            _ => DomesticExchange::KOSPI,
        }
    }

    #[allow(dead_code)]
    fn exchange_to_code(exchange: &str) -> Option<String> {
        match exchange {
            "KSQ" => Some("Q".to_string()),
            _ => Some("J".to_string()),
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
        match self
            .client
            .domestic_order_history(DomesticOrderHistoryRequest {
                start_date: today.clone(),
                end_date: today,
            })
            .await
        {
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
    pub fn new(client: Arc<dyn KisDomesticApi>) -> Self {
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
        // Implementation delegated to shared logic if possible, or copied/specialized
        kr_place_order(&self.base.client, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        kr_cancel_order(&self.base.client, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        kr_unfilled_orders(&self.base.client).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        kr_order_history(&self.base.client, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        // Real: No balance fallback unless explicit error
        match self.base.client.domestic_unfilled_orders().await {
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
        kr_balance(&self.base.client).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        kr_daily_chart(&self.base.client, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        kr_intraday_candles(&self.base.client, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        kr_current_price(&self.base.client, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        kr_volume_ranking(&self.base.client, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        kr_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        kr_is_holiday(&self.base.client).await
    }
}

/// Korean VTS Market Adapter.
pub struct KrVtsAdapter {
    base: KrMarketBase,
}

impl KrVtsAdapter {
    pub fn new(client: Arc<dyn KisDomesticApi>) -> Self {
        Self {
            base: KrMarketBase::new(client),
        }
    }

    /// VTS fallback: detect fill by checking balance when unfilled_orders API fails.
    async fn try_balance_fallback(
        &self,
        symbol: &str,
        expected_qty: u64,
    ) -> Option<(u64, Decimal)> {
        match self.base.client.domestic_balance().await {
            Ok(balance) => {
                if let Some(holding) = balance.items.iter().find(|h| h.symbol == symbol) {
                    let qty = holding.qty.to_u64().unwrap_or(0);
                    if qty >= expected_qty {
                        tracing::info!(
                            "KrVtsAdapter: balance fallback found {} with qty={}, avg_price={}",
                            symbol,
                            qty,
                            holding.avg_price
                        );
                        return Some((qty, holding.avg_price));
                    }
                }
                None
            }
            Err(e) => {
                tracing::warn!(
                    "KrVtsAdapter: balance fallback failed for {}: {}",
                    symbol,
                    e
                );
                None
            }
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
        kr_place_order(&self.base.client, req, self).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        kr_cancel_order(&self.base.client, order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        kr_unfilled_orders(&self.base.client).await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        kr_order_history(&self.base.client, start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        // VTS: Aggressive balance fallback
        match self.base.client.domestic_unfilled_orders().await {
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
            Err(e) => {
                tracing::warn!(
                    "KrVtsAdapter: unfilled_orders error: {} — trying balance fallback",
                    e
                );
                if let Some((filled_qty, filled_price)) =
                    self.try_balance_fallback(symbol, expected_qty).await
                {
                    return Ok(PollOutcome::Filled {
                        filled_qty,
                        filled_price,
                    });
                }
                Err(BotError::ApiError {
                    msg: format!("VTS poll failed: {}", e),
                })
            }
        }
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        kr_balance(&self.base.client).await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        kr_daily_chart(&self.base.client, symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        kr_intraday_candles(&self.base.client, symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        kr_current_price(&self.base.client, symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        kr_volume_ranking(&self.base.client, count).await
    }

    fn market_timing(&self) -> MarketTiming {
        kr_market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        kr_is_holiday(&self.base.client).await
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Shared Internal Functions to minimize duplication
// ─────────────────────────────────────────────────────────────────────────────

async fn kr_place_order(
    client: &Arc<dyn KisDomesticApi>,
    req: UnifiedOrderRequest,
    adapter: &impl MarketAdapter,
) -> Result<UnifiedOrderResult, BotError> {
    let exchange = match req.metadata.exchange_code.as_deref() {
        Some("Q") => DomesticExchange::KOSDAQ,
        _ => DomesticExchange::KOSPI,
    };

    let adjusted_price = req
        .price
        .map(|p| adapter.adjust_aggressive_price(p, req.side, req.strength));

    let place_req = DomesticPlaceOrderRequest {
        symbol: req.symbol.clone(),
        exchange,
        side: match req.side {
            UnifiedSide::Buy => kis_api::OrderSide::Buy,
            UnifiedSide::Sell => kis_api::OrderSide::Sell,
        },
        order_type: kis_api::DomesticOrderType::Limit,
        qty: req.qty as u32,
        price: adjusted_price,
    };

    let response =
        client
            .domestic_place_order(place_req)
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("domestic_place_order failed: {}", e),
            })?;

    Ok(UnifiedOrderResult {
        internal_id: uuid::Uuid::new_v4().to_string(),
        broker_id: response.order_no,
        symbol: req.symbol,
        side: req.side,
        qty: req.qty,
        price: adjusted_price,
    })
}

async fn kr_cancel_order(
    client: &Arc<dyn KisDomesticApi>,
    order: &UnifiedUnfilledOrder,
) -> Result<bool, BotError> {
    let exchange = match order.exchange_code.as_deref() {
        Some("Q") => DomesticExchange::KOSDAQ,
        _ => DomesticExchange::KOSPI,
    };
    let cancel_req = DomesticCancelOrderRequest {
        symbol: order.symbol.clone(),
        exchange,
        original_order_no: order.order_no.clone(),
        qty: order.remaining_qty as u32,
        price: Some(order.price),
    };

    client
        .domestic_cancel_order(cancel_req)
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_cancel_order failed: {}", e),
        })?;

    Ok(true)
}

async fn kr_unfilled_orders(
    client: &Arc<dyn KisDomesticApi>,
) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
    let orders = client
        .domestic_unfilled_orders()
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_unfilled_orders failed: {}", e),
        })?;

    Ok(orders
        .into_iter()
        .map(|o| UnifiedUnfilledOrder {
            order_no: o.order_no,
            symbol: o.symbol,
            side: match o.side_cd.as_str() {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: o.qty as u64,
            remaining_qty: o.remaining_qty as u64,
            price: o.price,
            exchange_code: match o.exchange.as_str() {
                "KSQ" => Some("Q".to_string()),
                _ => Some("J".to_string()),
            },
        })
        .collect())
}

async fn kr_order_history(
    client: &Arc<dyn KisDomesticApi>,
    start_date: &str,
    end_date: &str,
) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
    let history = client
        .domestic_order_history(DomesticOrderHistoryRequest {
            start_date: start_date.to_string(),
            end_date: end_date.to_string(),
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_order_history failed: {}", e),
        })?;

    Ok(history
        .into_iter()
        .map(|h| UnifiedOrderHistoryItem {
            order_no: h.order_no,
            symbol: h.symbol,
            side: match h.side_cd.as_str() {
                "02" => UnifiedSide::Buy,
                _ => UnifiedSide::Sell,
            },
            qty: h.qty,
            filled_qty: h.filled_qty,
            price: Decimal::ZERO,
            filled_price: h.filled_price,
            status: String::new(),
        })
        .collect())
}

async fn kr_balance(client: &Arc<dyn KisDomesticApi>) -> Result<UnifiedBalance, BotError> {
    let balance = client
        .domestic_balance()
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_balance failed: {}", e),
        })?;

    let positions = balance
        .items
        .into_iter()
        .map(|item| UnifiedPosition {
            symbol: item.symbol,
            name: Some(item.name),
            qty: item.qty,
            avg_price: item.avg_price,
            current_price: item.eval_amount / item.qty.max(Decimal::ONE),
            unrealized_pnl: item.unrealized_pnl,
            pnl_pct: item.pnl_rate.to_f64().unwrap_or(0.0),
        })
        .collect();

    Ok(UnifiedBalance {
        total_equity: balance.summary.available_cash, // Use cash as base equity for now
        available_cash: balance.summary.available_cash,
        positions,
    })
}

async fn kr_daily_chart(
    client: &Arc<dyn KisDomesticApi>,
    symbol: &str,
    _days: u32,
) -> Result<Vec<UnifiedDailyBar>, BotError> {
    let bars = client
        .domestic_daily_chart(DomesticDailyChartRequest {
            symbol: symbol.to_string(),
            period: ChartPeriod::Daily,
            adj_price: true,
            exchange: DomesticExchange::KOSPI,
            start_date: None,
            end_date: None,
        })
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_daily_chart failed: {}", e),
        })?;

    Ok(bars
        .into_iter()
        .filter_map(|b| {
            chrono::NaiveDate::parse_from_str(&b.date, "%Y%m%d")
                .ok()
                .map(|date| UnifiedDailyBar {
                    date,
                    open: b.open,
                    high: b.high,
                    low: b.low,
                    close: b.close,
                    volume: b.volume.to_u64().unwrap_or(0),
                })
        })
        .collect())
}

async fn kr_intraday_candles(
    _client: &Arc<dyn KisDomesticApi>,
    symbol: &str,
    _interval_mins: u32,
) -> Result<Vec<UnifiedCandleBar>, BotError> {
    tracing::warn!("intraday_candles not implemented for KR: {}", symbol);
    Ok(vec![])
}

async fn kr_current_price(
    _client: &Arc<dyn KisDomesticApi>,
    _symbol: &str,
) -> Result<Decimal, BotError> {
    Err(BotError::ApiError {
        msg: "current_price not implemented for KR market".to_string(),
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

async fn kr_is_holiday(client: &Arc<dyn KisDomesticApi>) -> Result<bool, BotError> {
    let today = Utc::now()
        .with_timezone(&Seoul)
        .format("%Y%m%d")
        .to_string();
    let holidays = client
        .domestic_holidays(&today)
        .await
        .map_err(|e| BotError::ApiError {
            msg: format!("domestic_holidays failed: {}", e),
        })?;

    Ok(!holidays.is_empty())
}

async fn kr_volume_ranking(
    client: &Arc<dyn KisDomesticApi>,
    count: u32,
) -> Result<Vec<String>, BotError> {
    let (kospi_count, kosdaq_count) = if count >= 20 {
        (count * 3 / 4, count / 4)
    } else {
        (count, 0)
    };

    let mut symbols = match client
        .domestic_volume_ranking(&DomesticExchange::KOSPI, kospi_count)
        .await
    {
        Ok(items) => items.into_iter().map(|i| i.symbol).collect::<Vec<_>>(),
        Err(e) => {
            return Err(BotError::ApiError {
                msg: format!("KOSPI volume_ranking failed: {}", e),
            })
        }
    };

    if kosdaq_count > 0 {
        match client
            .domestic_volume_ranking(&DomesticExchange::KOSDAQ, kosdaq_count)
            .await
        {
            Ok(items) => {
                symbols.extend(items.into_iter().map(|i| i.symbol));
            }
            Err(e) => {
                tracing::warn!("KOSDAQ volume_ranking failed (non-fatal): {}", e);
            }
        }
    }

    Ok(symbols)
}
