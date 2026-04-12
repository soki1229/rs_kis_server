use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

// ── Market ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Market {
    Kr,
    Us,
}

impl Market {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Kr => "KR",
            Self::Us => "US",
        }
    }
}

// ── Market Regime ─────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MarketRegime {
    Trending,
    Volatile,
    Quiet,
}

impl std::fmt::Display for MarketRegime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Trending => write!(f, "Trending"),
            Self::Volatile => write!(f, "Volatile"),
            Self::Quiet => write!(f, "Quiet"),
        }
    }
}

// ── Watchlist ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct WatchlistSet {
    pub stable: Vec<String>,
    pub aggressive: Vec<String>,
}

impl WatchlistSet {
    pub fn all_unique(&self) -> Vec<String> {
        let mut seen = std::collections::HashSet::new();
        let mut result = Vec::new();
        for s in self.stable.iter().chain(self.aggressive.iter()) {
            if seen.insert(s.clone()) {
                result.push(s.clone());
            }
        }
        result
    }
}

// ── Position ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub name: Option<String>,
    pub qty: i64,
    pub avg_price: Decimal,
    #[serde(default)]
    pub current_price: Decimal,
    #[serde(default)]
    pub unrealized_pnl: Decimal,
    #[serde(default)]
    pub pnl_pct: f64,
    #[serde(default)]
    pub stop_price: Decimal,
    #[serde(default)]
    pub trailing_stop: Option<Decimal>,
    #[serde(default)]
    pub profit_target_1: Decimal,
    #[serde(default)]
    pub profit_target_2: Decimal,
    #[serde(default)]
    pub regime: String,
}

// ── Order ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub side: Side,
    pub qty: u64,
    pub price: Option<Decimal>,
    pub atr: Option<Decimal>,
    /// KIS exchange code: "J" = KOSPI, "Q" = KOSDAQ. None for US orders.
    pub exchange_code: Option<String>,
    /// Signal strength (0.0-1.0) for aggressive limit pricing
    #[serde(default)]
    pub strength: Option<f64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OrderResult {
    pub order_id: String,
    pub symbol: String,
    pub side: Side,
    pub qty: u64,
    pub price: Decimal,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FillInfo {
    pub order_id: String,
    pub symbol: String,
    pub filled_qty: u64,
    pub filled_price: Decimal,
    pub atr: Option<Decimal>,
    /// KIS exchange code: "J"=KOSPI, "Q"=KOSDAQ. None for US orders.
    pub exchange_code: Option<String>,
}


// ── PnL ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PnLReport {
    pub realized: Decimal,
    pub unrealized: Decimal,
    pub date: chrono::NaiveDate,
}

// ── Signal ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warning,
    Critical,
}

// ── Market Tick / Quote ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub price: Decimal,
    pub volume: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub bid_price: Decimal,
    pub ask_price: Decimal,
    pub bid_qty: u64,
    pub ask_qty: u64,
}

// ── Kill Switch ────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KillSwitchMode {
    Soft,
    Hard,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KillSwitchFile {
    pub mode: KillSwitchMode,
    pub reason: String,
    pub triggered_at: chrono::DateTime<chrono::FixedOffset>,
    pub details: String,
}

// ── Order State Machine ───────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    PendingSubmit,
    Submitted,
    PartiallyFilled { filled_qty: u64 },
    FullyFilled,
    CancelledPartial { filled_qty: u64 },
    Cancelled,
    Failed { reason: String },
}

// ── Bot Commands / Events ─────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum BotCommand {
    Start,
    Stop,
    LiquidateAll,
    Pause,
    ForceOrder(OrderRequest),
    SetRiskLimit(Decimal),
    QueryStatus,
    KillHard,
    KillSoft,
    KillClear,
}

#[derive(Debug, Clone)]
pub enum BotEvent {
    OrderPlaced(OrderResult),
    OrderFilled(FillInfo),
    PositionChanged(Position),
    DailyPnL(PnLReport),
    Alert { level: AlertLevel, msg: String },
}
