use chrono::{DateTime, FixedOffset};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

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

/// 레짐 계산에 필요한 시장 지표 스냅샷 (CandleBar 배열에서 파이프라인이 계산)
#[derive(Debug, Clone)]
pub struct RegimeInput {
    pub ma5: f64,
    pub ma20: f64,
    pub daily_change_pct: f64,
    pub volume_ratio: f64,
}

// ── Bot State ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BotState {
    #[default]
    Idle,
    RecoveryCheck,
    Active,
    EntryPaused,
    EntryPausedLlmOutage,
    SoftBlocked,
    Suspended,
    HardBlocked,
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
    pub triggered_at: DateTime<FixedOffset>,
    pub details: String,
}

// ── Order ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Market {
    Us,
    Kr,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum OrderState {
    PendingSubmit,
    Submitted,
    PartiallyFilled { filled_qty: Decimal },
    FullyFilled,
    CancelledPartial { filled_qty: Decimal },
    Cancelled,
    Failed { reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderRequest {
    pub symbol: String,
    pub market: Market,
    pub side: Side,
    pub quantity: Decimal,
    pub price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderResult {
    pub broker_order_id: Option<String>,
    pub state: OrderState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FillInfo {
    pub filled_qty: Decimal,
    pub avg_price: Decimal,
}

// ── Position ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub symbol: String,
    pub market: Market,
    pub quantity: Decimal,
    pub avg_price: Decimal,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PnLReport {
    pub realized_pnl: Decimal,
    pub unrealized_pnl: Decimal,
    pub daily_r: f64,
}

// ── Signal ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Direction {
    Long,
    Short,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuleSignal {
    pub direction: Direction,
    pub strength: f64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum LlmVerdict {
    Enter,
    Watch,
    Block,
}

// ── Market Tick / Quote ───────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    pub symbol: String,
    pub price: Decimal,
    pub volume: Decimal,
    pub timestamp: DateTime<FixedOffset>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    pub symbol: String,
    pub bid: Decimal,
    pub ask: Decimal,
}

// ── Watchlist ─────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WatchlistSet {
    pub us: Vec<String>,
    pub kr: Vec<String>,
}

// ── Alerts ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum AlertLevel {
    Info,
    Warn,
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AlertEvent {
    Info { message: String },
    Warn { message: String },
    Critical { message: String },
}

// ── Bot Commands / Events (previously in domain crate) ────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BotCommand {
    Start,
    Stop,
    PauseEntry,
    ResumeEntry,
    GetStatus,
    KillSwitch { mode: KillSwitchMode, reason: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BotEvent {
    StateChanged { new_state: BotState },
    OrderPlaced(OrderRequest),
    OrderFilled(FillInfo),
    PositionUpdated(Vec<Position>),
    PnLUpdated(PnLReport),
    Alert(AlertEvent),
    WatchlistUpdated(WatchlistSet),
}
