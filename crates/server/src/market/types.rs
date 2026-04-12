//! Unified domain models for market-agnostic trading operations.

use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

/// Market identifier
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum MarketId {
    Kr,
    KrVts,
    Us,
    UsVts,
}

impl MarketId {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Kr => "KR",
            Self::KrVts => "KR-VTS",
            Self::Us => "US",
            Self::UsVts => "US-VTS",
        }
    }

    pub fn is_vts(&self) -> bool {
        matches!(self, Self::KrVts | Self::UsVts)
    }

    pub fn is_kr(&self) -> bool {
        matches!(self, Self::Kr | Self::KrVts)
    }

    pub fn is_us(&self) -> bool {
        matches!(self, Self::Us | Self::UsVts)
    }
}

impl std::fmt::Display for MarketId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.label())
    }
}

/// Unified order request — market-agnostic representation
#[derive(Debug, Clone)]
pub struct UnifiedOrderRequest {
    pub symbol: String,
    pub side: UnifiedSide,
    pub qty: u64,
    pub price: Option<Decimal>,
    pub atr: Option<Decimal>,
    /// Signal strength for aggressive limit pricing (0.0-1.0)
    pub strength: Option<f64>,
    /// Market-specific metadata (e.g., exchange code for KR)
    pub metadata: OrderMetadata,
}

/// Order side
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum UnifiedSide {
    Buy,
    Sell,
}

/// Market-specific order metadata
#[derive(Debug, Clone, Default)]
pub struct OrderMetadata {
    /// KR: "J" (KOSPI) or "Q" (KOSDAQ)
    pub exchange_code: Option<String>,
    /// US: Exchange hint (e.g., "NASD", "NYSE")
    pub exchange_hint: Option<String>,
}

/// Unified order result after placement
#[derive(Debug, Clone)]
pub struct UnifiedOrderResult {
    pub internal_id: String,
    pub broker_id: String,
    pub symbol: String,
    pub side: UnifiedSide,
    pub qty: u64,
    pub price: Option<Decimal>,
}

/// Unified fill information
#[derive(Debug, Clone)]
pub struct UnifiedFill {
    pub order_id: String,
    pub symbol: String,
    pub filled_qty: u64,
    pub filled_price: Decimal,
    pub metadata: OrderMetadata,
}

/// Poll result for order status checking
#[derive(Debug, Clone)]
pub enum PollOutcome {
    Filled {
        filled_qty: u64,
        filled_price: Decimal,
    },
    PartialFilled {
        filled_qty: u64,
        filled_price: Decimal,
    },
    StillOpen,
    Cancelled,
    Failed {
        reason: String,
    },
}

/// Unified unfilled order representation
#[derive(Debug, Clone)]
pub struct UnifiedUnfilledOrder {
    pub order_no: String,
    pub symbol: String,
    pub side: UnifiedSide,
    pub qty: u64,
    pub remaining_qty: u64,
    pub price: Decimal,
    pub exchange_code: Option<String>,
}

/// Unified position/holding
#[derive(Debug, Clone)]
pub struct UnifiedPosition {
    pub symbol: String,
    pub name: Option<String>,
    pub qty: Decimal,
    pub avg_price: Decimal,
    pub current_price: Decimal,
    pub unrealized_pnl: Decimal,
    pub pnl_pct: f64,
}

/// Unified balance response
#[derive(Debug, Clone)]
pub struct UnifiedBalance {
    pub total_equity: Decimal,
    pub available_cash: Decimal,
    pub positions: Vec<UnifiedPosition>,
}

/// Unified daily OHLCV bar
#[derive(Debug, Clone)]
pub struct UnifiedDailyBar {
    pub date: chrono::NaiveDate,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
}

/// Unified candle bar (intraday)
#[derive(Debug, Clone)]
pub struct UnifiedCandleBar {
    pub timestamp: DateTime<Utc>,
    pub open: Decimal,
    pub high: Decimal,
    pub low: Decimal,
    pub close: Decimal,
    pub volume: u64,
}

/// Unified order history item
#[derive(Debug, Clone)]
pub struct UnifiedOrderHistoryItem {
    pub order_no: String,
    pub symbol: String,
    pub side: UnifiedSide,
    pub qty: u32,
    pub filled_qty: u32,
    pub price: Decimal,
    pub filled_price: Decimal,
    pub status: String,
}

/// Market timing information
#[derive(Debug, Clone)]
pub struct MarketTiming {
    pub is_open: bool,
    pub mins_since_open: i64,
    pub mins_until_close: i64,
    pub is_holiday: bool,
}

// Conversion helpers from legacy types
impl From<crate::types::Side> for UnifiedSide {
    fn from(side: crate::types::Side) -> Self {
        match side {
            crate::types::Side::Buy => UnifiedSide::Buy,
            crate::types::Side::Sell => UnifiedSide::Sell,
        }
    }
}

impl From<UnifiedSide> for crate::types::Side {
    fn from(side: UnifiedSide) -> Self {
        match side {
            UnifiedSide::Buy => crate::types::Side::Buy,
            UnifiedSide::Sell => crate::types::Side::Sell,
        }
    }
}

impl From<crate::types::OrderRequest> for UnifiedOrderRequest {
    fn from(req: crate::types::OrderRequest) -> Self {
        Self {
            symbol: req.symbol,
            side: req.side.into(),
            qty: req.qty,
            price: req.price,
            atr: req.atr,
            strength: req.strength,
            metadata: OrderMetadata {
                exchange_code: req.exchange_code,
                exchange_hint: None,
            },
        }
    }
}

impl From<UnifiedFill> for crate::types::FillInfo {
    fn from(fill: UnifiedFill) -> Self {
        Self {
            order_id: fill.order_id,
            symbol: fill.symbol,
            filled_qty: fill.filled_qty,
            filled_price: fill.filled_price,
            exchange_code: fill.metadata.exchange_code,
            atr: None,
        }
    }
}
