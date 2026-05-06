//! MarketAdapter trait — the unified port for all market operations.

use super::types::*;
use crate::error::BotError;
use async_trait::async_trait;
use rust_decimal::Decimal;
use std::sync::Arc;

/// Unified market adapter trait.
///
/// All market-specific operations (order placement, polling, data fetching)
/// are abstracted behind this interface. Strategy and pipeline code should
/// only interact with markets through this trait.
#[async_trait]
pub trait MarketAdapter: Send + Sync + 'static {
    /// Market identifier
    fn market_id(&self) -> MarketId;

    /// Human-readable market name
    fn name(&self) -> &'static str;

    // ─────────────────────────────────────────────────────────────────────────
    // Order Operations
    // ─────────────────────────────────────────────────────────────────────────

    /// Place an order and return the result with broker order ID.
    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError>;

    /// Cancel an unfilled order.
    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError>;

    /// Get all currently unfilled orders.
    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError>;

    /// Get order history for a date range.
    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError>;

    /// Poll order status and return the outcome.
    /// Implementations should handle market-specific polling logic (e.g., VTS fallback for KR).
    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError>;

    // ─────────────────────────────────────────────────────────────────────────
    // Balance & Position
    // ─────────────────────────────────────────────────────────────────────────

    /// Get current balance and positions.
    async fn balance(&self) -> Result<UnifiedBalance, BotError>;

    // ─────────────────────────────────────────────────────────────────────────
    // Market Data
    // ─────────────────────────────────────────────────────────────────────────

    /// Fetch daily OHLCV bars for a symbol.
    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError>;

    /// Fetch intraday candle bars.
    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError>;

    /// Get current price for a symbol.
    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError>;

    /// Get top N symbols by trading volume.
    /// Returns symbol strings only (ticker codes).
    /// Default implementation returns empty vec (adapters may override).
    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        let _ = count;
        Ok(vec![])
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Market Timing
    // ─────────────────────────────────────────────────────────────────────────

    /// Get current market timing information.
    fn market_timing(&self) -> MarketTiming;

    /// Check if today is a trading holiday.
    async fn is_holiday(&self) -> Result<bool, BotError>;

    /// 지정 날짜의 장 개장 UTC 시각 (스케줄러용)
    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>>;

    /// 지정 날짜의 장 마감 UTC 시각 (스케줄러용)
    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>>;

    /// 시장 현지 시간 기준 오늘 날짜 (KR: Seoul, US: New_York)
    fn local_today(&self) -> chrono::NaiveDate;

    // ─────────────────────────────────────────────────────────────────────────
    // Cost Adjustments
    // ─────────────────────────────────────────────────────────────────────────

    /// Get FX spread percentage for profit target adjustment.
    /// Returns 0.0 for markets without FX considerations (e.g., KR).
    fn fx_spread_pct(&self) -> Decimal {
        Decimal::ZERO
    }

    /// Get market-specific WebSocket subscription key (e.g., "DNASNVDA" for US Real, "005930" for KR).
    fn get_ws_key(&self, symbol: &str) -> String {
        symbol.to_string()
    }

    /// Pre-flight: 매수 가능 수량 확인 (API 기반).
    /// qty=0 반환 시 주문 불가. API 실패 시 원래 qty 반환 (soft check).
    async fn check_buy_orderable(&self, _symbol: &str, _price: Decimal, qty: u64) -> u64 {
        qty
    }

    /// Pre-flight: 매도 보유 수량 확인 (잔고 API 기반).
    /// qty=0 반환 시 포지션 없음. API 실패 시 원래 qty 반환 (soft check).
    async fn check_sell_orderable(&self, _symbol: &str, qty: u64) -> u64 {
        qty
    }

    /// Apply aggressive limit pricing adjustment to a base price.
    /// Default: +0.2% for strength >= 0.85 on buy orders.
    fn adjust_aggressive_price(
        &self,
        base_price: Decimal,
        side: UnifiedSide,
        strength: Option<f64>,
    ) -> Decimal {
        match (side, strength) {
            (UnifiedSide::Buy, Some(s)) if s >= 0.85 => {
                let bump = Decimal::new(2, 3); // 0.2%
                base_price * (Decimal::ONE + bump)
            }
            _ => base_price,
        }
    }
}

/// Extension trait for Arc<dyn MarketAdapter>
#[async_trait]
impl MarketAdapter for Arc<dyn MarketAdapter> {
    fn market_id(&self) -> MarketId {
        (**self).market_id()
    }

    fn name(&self) -> &'static str {
        (**self).name()
    }

    async fn place_order(&self, req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        (**self).place_order(req).await
    }

    async fn cancel_order(&self, order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        (**self).cancel_order(order).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        (**self).unfilled_orders().await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        (**self).order_history(start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        (**self)
            .poll_order_status(broker_order_no, symbol, expected_qty)
            .await
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        (**self).balance().await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        (**self).daily_chart(symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        (**self).intraday_candles(symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        (**self).current_price(symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        (**self).volume_ranking(count).await
    }

    fn market_timing(&self) -> MarketTiming {
        (**self).market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        (**self).is_holiday().await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        (**self).market_open_utc(date)
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        (**self).market_close_utc(date)
    }

    fn local_today(&self) -> chrono::NaiveDate {
        (**self).local_today()
    }

    fn fx_spread_pct(&self) -> Decimal {
        (**self).fx_spread_pct()
    }

    fn get_ws_key(&self, symbol: &str) -> String {
        (**self).get_ws_key(symbol)
    }

    async fn check_buy_orderable(&self, symbol: &str, price: Decimal, qty: u64) -> u64 {
        (**self).check_buy_orderable(symbol, price, qty).await
    }

    async fn check_sell_orderable(&self, symbol: &str, qty: u64) -> u64 {
        (**self).check_sell_orderable(symbol, qty).await
    }

    fn adjust_aggressive_price(
        &self,
        base_price: Decimal,
        side: UnifiedSide,
        strength: Option<f64>,
    ) -> Decimal {
        (**self).adjust_aggressive_price(base_price, side, strength)
    }
}

/// A wrapper adapter that blocks all order placement and cancellation.
/// Used when bot.execution_enabled = 0.
pub struct ReadOnlyAdapter {
    inner: Arc<dyn MarketAdapter>,
}

impl ReadOnlyAdapter {
    pub fn new(inner: Arc<dyn MarketAdapter>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl MarketAdapter for ReadOnlyAdapter {
    fn market_id(&self) -> MarketId {
        self.inner.market_id()
    }

    fn name(&self) -> &'static str {
        self.inner.name()
    }

    async fn place_order(&self, _req: UnifiedOrderRequest) -> Result<UnifiedOrderResult, BotError> {
        tracing::warn!("ReadOnlyAdapter: order placement blocked (Evaluation Mode)");
        // Return a dummy error or a specific variant to signal block
        Err(BotError::ApiError {
            msg: "Order placement blocked in Evaluation Mode".to_string(),
        })
    }

    async fn cancel_order(&self, _order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
        tracing::warn!("ReadOnlyAdapter: order cancellation blocked (Evaluation Mode)");
        Ok(false)
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
        self.inner.unfilled_orders().await
    }

    async fn order_history(
        &self,
        start_date: &str,
        end_date: &str,
    ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
        self.inner.order_history(start_date, end_date).await
    }

    async fn poll_order_status(
        &self,
        broker_order_no: &str,
        symbol: &str,
        expected_qty: u64,
    ) -> Result<PollOutcome, BotError> {
        self.inner
            .poll_order_status(broker_order_no, symbol, expected_qty)
            .await
    }

    async fn balance(&self) -> Result<UnifiedBalance, BotError> {
        self.inner.balance().await
    }

    async fn daily_chart(&self, symbol: &str, days: u32) -> Result<Vec<UnifiedDailyBar>, BotError> {
        self.inner.daily_chart(symbol, days).await
    }

    async fn intraday_candles(
        &self,
        symbol: &str,
        interval_mins: u32,
    ) -> Result<Vec<UnifiedCandleBar>, BotError> {
        self.inner.intraday_candles(symbol, interval_mins).await
    }

    async fn current_price(&self, symbol: &str) -> Result<Decimal, BotError> {
        self.inner.current_price(symbol).await
    }

    async fn volume_ranking(&self, count: u32) -> Result<Vec<String>, BotError> {
        self.inner.volume_ranking(count).await
    }

    fn market_timing(&self) -> MarketTiming {
        self.inner.market_timing()
    }

    async fn is_holiday(&self) -> Result<bool, BotError> {
        self.inner.is_holiday().await
    }

    fn market_open_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        self.inner.market_open_utc(date)
    }

    fn market_close_utc(&self, date: chrono::NaiveDate) -> Option<chrono::DateTime<chrono::Utc>> {
        self.inner.market_close_utc(date)
    }

    fn local_today(&self) -> chrono::NaiveDate {
        self.inner.local_today()
    }

    fn fx_spread_pct(&self) -> Decimal {
        self.inner.fx_spread_pct()
    }

    fn get_ws_key(&self, symbol: &str) -> String {
        self.inner.get_ws_key(symbol)
    }

    async fn check_buy_orderable(&self, symbol: &str, price: Decimal, qty: u64) -> u64 {
        self.inner.check_buy_orderable(symbol, price, qty).await
    }

    async fn check_sell_orderable(&self, symbol: &str, qty: u64) -> u64 {
        self.inner.check_sell_orderable(symbol, qty).await
    }
}
