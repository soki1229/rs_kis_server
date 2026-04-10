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

/// Korean market adapter wrapping KisDomesticApi.
pub struct KrMarketAdapter {
    client: Arc<dyn KisDomesticApi>,
}

impl KrMarketAdapter {
    pub fn new(client: Arc<dyn KisDomesticApi>) -> Self {
        Self { client }
    }

    fn exchange_from_code(code: Option<&str>) -> DomesticExchange {
        match code {
            Some("Q") => DomesticExchange::KOSDAQ,
            _ => DomesticExchange::KOSPI,
        }
    }

    fn exchange_to_code(exchange: &str) -> Option<String> {
        match exchange {
            "KSQ" => Some("Q".to_string()),
            _ => Some("J".to_string()),
        }
    }

    /// VTS fallback: detect fill by checking balance when unfilled_orders API fails.
    async fn try_balance_fallback(
        &self,
        symbol: &str,
        expected_qty: u64,
    ) -> Option<(u64, Decimal)> {
        match self.client.domestic_balance().await {
            Ok(balance) => {
                if let Some(holding) = balance.items.iter().find(|h| h.symbol == symbol) {
                    let qty = holding.qty.to_u64().unwrap_or(0);
                    if qty >= expected_qty {
                        tracing::info!(
                            "VTS balance fallback: found {} with qty={}, avg_price={}",
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
                    "KrMarketAdapter: balance fallback failed for {}: {}",
                    symbol,
                    e
                );
                None
            }
        }
    }

    /// Confirm fill from order history when order disappears from unfilled list.
    async fn confirm_fill_from_history(
        &self,
        broker_order_no: &str,
        submitted_qty: u64,
    ) -> PollOutcome {
        let today = Utc::now().format("%Y%m%d").to_string();
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
                    "KrMarketAdapter: order_history error for {}: {}",
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

#[async_trait]
impl MarketAdapter for KrMarketAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::Kr
    }

    fn name(&self) -> &'static str {
        "KR"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        let exchange = Self::exchange_from_code(req.metadata.exchange_code.as_deref());

        // Apply aggressive limit pricing
        let adjusted_price = req
            .price
            .map(|p| self.adjust_aggressive_price(p, req.side, req.strength));

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

        let response = self
            .client
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

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        let exchange = Self::exchange_from_code(order.exchange_code.as_deref());
        let cancel_req = DomesticCancelOrderRequest {
            symbol: order.symbol.clone(),
            exchange,
            original_order_no: order.order_no.clone(),
            qty: order.remaining_qty as u32,
            price: Some(order.price),
        };

        self.client
            .domestic_cancel_order(cancel_req)
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("domestic_cancel_order failed: {}", e),
            })?;

        Ok(true)
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        let orders =
            self.client
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
                exchange_code: Self::exchange_to_code(&o.exchange),
            })
            .collect())
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        let history = self
            .client
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
                price: Decimal::ZERO, // Not available in domestic history
                filled_price: h.filled_price,
                status: String::new(),
            })
            .collect())
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match self.client.domestic_unfilled_orders().await {
            Ok(orders) => {
                let still_open = orders.iter().any(|o| o.order_no == broker_order_no);
                if still_open {
                    Ok(PollOutcome::StillOpen)
                } else {
                    Ok(self
                        .confirm_fill_from_history(broker_order_no, expected_qty)
                        .await)
                }
            }
            Err(e) => {
                tracing::warn!(
                    "KrMarketAdapter: unfilled_orders error: {} — trying balance fallback",
                    e
                );
                // VTS fallback
                if let Some((filled_qty, filled_price)) =
                    self.try_balance_fallback(symbol, expected_qty).await
                {
                    return Ok(PollOutcome::Filled {
                        filled_qty,
                        filled_price,
                    });
                }
                Err(BotError::ApiError {
                    msg: format!("unfilled_orders failed and fallback unsuccessful: {}", e),
                })
            }
        }
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        let balance = self
            .client
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
            total_equity: balance.summary.purchase_amount,
            available_cash: balance.summary.purchase_amount,
            positions,
        })
    }

    async fn daily_chart(
        &self,
        symbol: &str,
        _days: u32,
    ) -> Result<Vec<UnifiedDailyBar>, BotError> {
        let bars = self
            .client
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

    async fn intraday_candles(
        &self,
        _symbol: &str,
        _interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        // domestic_candles not available in current API
        Ok(vec![])
    }

    async fn current_price(&self, _symbol: &str) -> Result<Decimal, BotError> {
        // Not directly available — would need separate price API
        Err(BotError::ApiError {
            msg: "current_price not implemented for KR market".to_string(),
        })
    }

    fn market_timing(&self) -> MarketTiming {
        let now = Utc::now().with_timezone(&Seoul);
        let hour = now.hour();
        let minute = now.minute();
        let total_mins = (hour * 60 + minute) as i64;

        // KR market: 09:00 - 15:30 KST
        let open_mins = 9 * 60; // 09:00
        let close_mins = 15 * 60 + 30; // 15:30

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
            is_holiday: false, // Checked separately via is_holiday()
        }
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        let today = Utc::now()
            .with_timezone(&Seoul)
            .format("%Y%m%d")
            .to_string();
        let holidays =
            self.client
                .domestic_holidays(&today)
                .await
                .map_err(|e| BotError::ApiError {
                    msg: format!("domestic_holidays failed: {}", e),
                })?;

        Ok(!holidays.is_empty())
    }

    fn fx_spread_pct(&self) -> Decimal {
        Decimal::ZERO // No FX for domestic market
    }
}
