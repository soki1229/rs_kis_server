use crate::types::{Position, WatchlistPatch};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};
use tokio::sync::{watch, Notify};

#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub enum BotState {
    #[default]
    Idle,
    Active,
    EntryPaused,          // 진입 차단, 보유 포지션 청산 계속
    EntryPausedLlmOutage, // LLM 연속 실패로 인한 진입 차단
    Suspended,            // 신규 주문 전체 차단
    HardBlocked,          // KillSwitch 활성화 상태
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MarketLiveState {
    pub positions: Vec<Position>,
    pub daily_pnl_r: f64,
    pub regime: String, // crate::MarketRegime의 Display 문자열
    /// position task가 마지막으로 publish한 시각 (None = 초기값 그대로)
    pub last_updated: Option<chrono::DateTime<chrono::Utc>>,
    /// 사용 가능 잔고 (KR: 원화, US: USD). None = 미조회
    pub available_cash: Option<rust_decimal::Decimal>,
    /// 오늘(KST) 실현 손익 합계
    pub realized_today: f64,
    /// 이번달(KST) 실현 손익 합계
    pub realized_month: f64,
    /// 전체 실현 손익 합계
    pub realized_total: f64,
    /// 기준 예수금 (portfolio_config.initial_equity). None = 미설정
    pub initial_equity: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DailyStats {
    pub total_trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub max_drawdown_r: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarketSummary {
    pub bot_state: BotState,
    pub profile: String,
    pub kill_switch: Option<String>,
    pub stats: DailyStats,
}

impl MarketSummary {
    pub fn new() -> Self {
        Self {
            bot_state: BotState::default(),
            profile: "default".into(),
            kill_switch: None,
            stats: DailyStats::default(),
        }
    }
}

impl Default for MarketSummary {
    fn default() -> Self {
        Self::new()
    }
}

// ── Phase 1: Construction ──────────────────────────────────────────────────
//
// `PipelineConfig` owns the watch::Sender that must be transferred to
// PositionTask exactly once. Because it is not Clone and `build()` consumes
// `self`, the compiler enforces single use — the previous `Option::take +
// expect` runtime panic is replaced by a compile-time ownership check.

/// Pipeline construction handle. Create one per market (KR / US) in main.rs,
/// clone out any `summary` / `live_state_rx` references you need, then call
/// `build()` to obtain the PositionTask sender and the shareable handles.
pub struct PipelineConfig {
    live_state_tx: watch::Sender<MarketLiveState>,
    /// Share this with Telegram/REST/ControlTask before calling build().
    pub live_state_rx: watch::Receiver<MarketLiveState>,
    /// Share this with ControlTask/ExecutionTask/SignalTask before calling build().
    pub summary: Arc<RwLock<MarketSummary>>,
    pub activity: crate::shared::activity::ActivityLog,
    /// /status 즉시 갱신 신호 — Telegram이 notify, PositionTask가 balance 재조회 후 publish
    pub refresh_notify: Arc<Notify>,
    /// 런타임 워치리스트 패치 — REST가 쓰고 scheduler가 읽어 반영
    pub watchlist_patch: Arc<RwLock<WatchlistPatch>>,
}

impl PipelineConfig {
    pub fn new(activity: crate::shared::activity::ActivityLog) -> Self {
        let (tx, rx) = watch::channel(MarketLiveState::default());
        Self {
            live_state_tx: tx,
            live_state_rx: rx,
            summary: Arc::new(RwLock::new(MarketSummary::new())),
            activity,
            refresh_notify: Arc::new(Notify::new()),
            watchlist_patch: Arc::new(RwLock::new(WatchlistPatch::default())),
        }
    }

    /// Consumes `self` — compiler enforces exactly-once.
    /// Returns the `Sender` for PositionTask and shareable `PipelineState`.
    pub fn build(self) -> (watch::Sender<MarketLiveState>, PipelineState) {
        let state = PipelineState {
            live_state_rx: self.live_state_rx,
            summary: self.summary,
            activity: self.activity,
            refresh_notify: self.refresh_notify,
            watchlist_patch: self.watchlist_patch,
        };
        (self.live_state_tx, state)
    }
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self::new(crate::shared::activity::ActivityLog::new())
    }
}

// ── Phase 2: Shared Handles ────────────────────────────────────────────────
//
// `PipelineState` no longer contains the Sender; it is freely Clone-able and
// can be shared with REST, Telegram, and ControlTask without ordering concerns.

/// Shareable pipeline state handles. Clone freely — there is no hidden
/// `Option<Sender>` that becomes `None` after a single clone.
#[derive(Clone)]
pub struct PipelineState {
    /// PositionTask live state reader — REST/Telegram read from this.
    pub live_state_rx: watch::Receiver<MarketLiveState>,
    /// Low-frequency summary — ControlTask writes, REST/Telegram read.
    pub summary: Arc<RwLock<MarketSummary>>,
    pub activity: crate::shared::activity::ActivityLog,
    /// /status 즉시 갱신 신호 — Telegram이 notify, PositionTask가 balance 재조회 후 publish
    pub refresh_notify: Arc<Notify>,
    /// 런타임 워치리스트 패치 — REST가 쓰고 scheduler가 읽어 반영
    pub watchlist_patch: Arc<RwLock<WatchlistPatch>>,
}

impl PipelineState {
    /// Convenience constructor for tests that do not need the PositionTask sender.
    #[cfg(test)]
    pub fn new_for_test() -> Self {
        PipelineConfig::new(crate::shared::activity::ActivityLog::new())
            .build()
            .1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bot_state_default_is_idle() {
        assert_eq!(BotState::default(), BotState::Idle);
    }

    #[test]
    fn market_summary_new_is_idle() {
        let s = MarketSummary::new();
        assert_eq!(s.bot_state, BotState::Idle);
        assert!(s.kill_switch.is_none());
    }

    #[test]
    fn market_live_state_default() {
        let s = MarketLiveState::default();
        assert!(s.positions.is_empty());
        assert_eq!(s.daily_pnl_r, 0.0);
    }

    #[test]
    fn pipeline_config_build_is_single_use() {
        let config = PipelineConfig::new(crate::shared::activity::ActivityLog::new());
        // Clone out the parts we need before consuming
        let summary = config.summary.clone();
        let live_rx = config.live_state_rx.clone();

        // build() consumes config — calling it a second time would be a compile error
        let (tx, state) = config.build();

        // Sender works
        tx.send(MarketLiveState::default()).unwrap();

        // PipelineState is Clone-able
        let state2 = state.clone();

        // All shared parts point to the same underlying data
        assert!(Arc::ptr_eq(&summary, &state2.summary));
        assert!(Arc::ptr_eq(&state.summary, &state2.summary));
        drop((live_rx, tx, state, state2));
    }

    #[test]
    fn pipeline_state_clone_shares_summary() {
        let state = PipelineState::new_for_test();
        let cloned = state.clone();
        // Write via one handle, read via the other
        state.summary.write().unwrap().profile = "conservative".into();
        assert_eq!(cloned.summary.read().unwrap().profile, "conservative");
    }
}
