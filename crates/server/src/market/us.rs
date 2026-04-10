//! US market adapter implementation.

use super::adapter::MarketAdapter;
use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use chrono::{Datelike, Timelike, Utc};
use chrono_tz::America::New_York;
use kis_api::{
    CancelKind, CancelOrderRequest, ChartPeriod, DailyChartRequest, Exchange, KisApi,
    OrderHistoryRequest, OrderSide, OrderType, PlaceOrderRequest,
};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;

/// US market adapter wrapping KisApi.
pub struct UsMarketAdapter {
    client: Arc<dyn KisApi>,
    fx_spread_pct: Decimal,
}

impl UsMarketAdapter {
    pub fn new(client: Arc<dyn KisApi>) -> Self {
        Self {
            client,
            fx_spread_pct: Decimal::new(5, 3), // 0.5% default
        }
    }

    pub fn with_fx_spread(mut self, pct: Decimal) -> Self {
        self.fx_spread_pct = pct;
        self
    }

    fn exchange_from_hint(hint: Option<&str>, symbol: &str) -> Exchange {
        match hint {
            Some("NYSE") => Exchange::NYSE,
            Some("AMEX") => Exchange::AMEX,
            Some("NASD") | Some("NASDAQ") => Exchange::NASD,
            _ => {
                // Default heuristic based on symbol
                if symbol.len() <= 3 {
                    Exchange::NYSE
                } else {
                    Exchange::NASD
                }
            }
        }
    }

    fn exchange_code_to_exchange(code: Option<&str>) -> Exchange {
        match code {
            Some("NYS") | Some("NYSE") => Exchange::NYSE,
            Some("AMS") | Some("AMEX") => Exchange::AMEX,
            _ => Exchange::NASD,
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
            .order_history(OrderHistoryRequest {
                start_date: today.clone(),
                end_date: today,
                exchange_cd: String::new(),
            })
            .await
        {
            Ok(history) => {
                if let Some(h) = history.iter().find(|h| h.order_no == broker_order_no) {
                    let filled_qty = h.filled_qty.to_u64().unwrap_or(0);
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
                    "UsMarketAdapter: order_history error for {}: {}",
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
impl MarketAdapter for UsMarketAdapter {
    fn market_id(&self) -> MarketId {
        MarketId::Us
    }

    fn name(&self) -> &'static str {
        "US"
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        let exchange = Self::exchange_from_hint(req.metadata.exchange_hint.as_deref(), &req.symbol);

        // Apply aggressive limit pricing
        let adjusted_price = req
            .price
            .map(|p| self.adjust_aggressive_price(p, req.side, req.strength));

        let place_req = PlaceOrderRequest {
            symbol: req.symbol.clone(),
            exchange,
            side: match req.side {
                UnifiedSide::Buy => OrderSide::Buy,
                UnifiedSide::Sell => OrderSide::Sell,
            },
            order_type: OrderType::Limit,
            qty: Decimal::from(req.qty),
            price: adjusted_price,
        };

        let response =
            self.client
                .place_order(place_req)
                .await
                .map_err(|e| BotError::ApiError {
                    msg: format!("place_order failed: {}", e),
                })?;

        Ok(UnifiedOrderResult {
            internal_id: uuid::Uuid::new_v4().to_string(),
            broker_id: response.order_org_no,
            symbol: req.symbol,
            side: req.side,
            qty: req.qty,
            price: adjusted_price,
        })
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        let exchange = Self::exchange_code_to_exchange(order.exchange_code.as_deref());
        let cancel_req = CancelOrderRequest {
            symbol: order.symbol.clone(),
            exchange,
            original_order_id: order.order_no.clone(),
            kind: CancelKind::Cancel,
            qty: Decimal::from(order.remaining_qty),
            price: None,
        };

        self.client
            .cancel_order(cancel_req)
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("cancel_order failed: {}", e),
            })?;

        Ok(true)
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        let orders = self
            .client
            .unfilled_orders()
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("unfilled_orders failed: {}", e),
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
                qty: o.qty.to_u64().unwrap_or(0),
                remaining_qty: o.remaining_qty.to_u64().unwrap_or(0),
                price: o.price,
                exchange_code: Some(o.exchange.clone()),
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
            .order_history(OrderHistoryRequest {
                start_date: start_date.to_string(),
                end_date: end_date.to_string(),
                exchange_cd: String::new(),
            })
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("order_history failed: {}", e),
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
                qty: h.qty.to_u32().unwrap_or(0),
                filled_qty: h.filled_qty.to_u32().unwrap_or(0),
                price: Decimal::ZERO,
                filled_price: h.filled_price,
                status: String::new(),
            })
            .collect())
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        _symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        match self.client.unfilled_orders().await {
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
            Err(e) => Err(BotError::ApiError {
                msg: format!("unfilled_orders failed: {}", e),
            }),
        }
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        let balance = self
            .client
            .balance()
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("balance failed: {}", e),
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
            .daily_chart(DailyChartRequest {
                symbol: symbol.to_string(),
                period: ChartPeriod::Daily,
                adj_price: true,
                exchange: Exchange::NASD,
            })
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("daily_chart failed: {}", e),
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
        // Not implemented in current KisApi
        Ok(vec![])
    }

    async fn current_price(&self, _symbol: &str) -> Result<Decimal, BotError> {
        // Not directly available — would need separate price API
        Err(BotError::ApiError {
            msg: "current_price not implemented for US market".to_string(),
        })
    }

    fn market_timing(&self) -> MarketTiming {
        let now = Utc::now().with_timezone(&New_York);
        let hour = now.hour();
        let minute = now.minute();
        let total_mins = (hour * 60 + minute) as i64;

        // US market: 09:30 - 16:00 ET
        let open_mins = 9 * 60 + 30; // 09:30
        let close_mins = 16 * 60; // 16:00

        // Check if it's a weekend
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

    async fn is_holiday(&self) -> Result<bool, BotError> {
        let holidays = self
            .client
            .holidays("USA")
            .await
            .map_err(|e| BotError::ApiError {
                msg: format!("holidays failed: {}", e),
            })?;

        let today = Utc::now()
            .with_timezone(&New_York)
            .format("%Y%m%d")
            .to_string();
        Ok(holidays.iter().any(|h| h.date == today))
    }

    fn fx_spread_pct(&self) -> Decimal {
        self.fx_spread_pct
    }
}
