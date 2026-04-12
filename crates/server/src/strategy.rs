use crate::pipeline::QuoteSnapshot;
use crate::regime::RegimeInput;
use crate::types::MarketRegime;
use async_trait::async_trait;
use kis_api::{KisApi, KisDomesticApi};
use rust_decimal::Decimal;
use std::sync::Arc;

// ── Discovery ─────────────────────────────────────────────────────────────

/// 워치리스트 구성 전략. 어떤 종목을 감시할지 결정한다.
#[async_trait]
pub trait DiscoveryStrategy: Send + Sync {
    /// US 워치리스트 빌드
    async fn build_us_watchlist(&self, client: Arc<dyn KisApi>) -> Vec<String>;
    /// KR 워치리스트 빌드
    async fn build_kr_watchlist(&self, client: Arc<dyn KisDomesticApi>) -> Vec<String>;
}

// ── Regime ────────────────────────────────────────────────────────────────

/// 시장 레짐 분류 전략. RegimeInput을 받아 현재 레짐을 반환한다.
/// 파이프라인이 CandleBar → RegimeInput 계산을 담당하고, 이 trait은 분류만 한다.
pub trait RegimeStrategy: Send + Sync {
    fn classify(&self, input: &RegimeInput) -> MarketRegime;
}

// ── Signal ────────────────────────────────────────────────────────────────

/// 시그널 평가에 필요한 컨텍스트.
#[derive(Debug, Clone)]
pub struct SignalContext {
    pub symbol: String,
    /// "US" | "KR"
    pub market: String,
    /// 완성된 캔들 (최신 순)
    pub candles: Vec<kis_api::CandleBar>,
    /// 현재 호가 정보
    pub quote: Option<QuoteSnapshot>,
    /// 현재 틱 가격
    pub current_price: Decimal,
    /// 당일 최고가 (rolling)
    pub rolling_high: Decimal,
    /// 현재 계좌 잔고 (USD 또는 KRW)
    pub account_balance: Decimal,
    /// 현재 시장 레짐
    pub regime: MarketRegime,
    /// config에서 설정한 최소 setup score (기본값: 60)
    pub setup_score_min: u32,
    /// config에서 설정한 regime filter 활성화 여부
    pub regime_filter: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Direction {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LlmVerdict {
    Enter,
    Watch,
    Block,
}

/// 시그널 평가 결과
#[derive(Debug, Clone)]
pub struct TradeSignal {
    pub symbol: String,
    pub direction: Direction,
    pub strength: f64,
    pub llm_verdict: Option<LlmVerdict>,
    pub entry_price: Decimal,
    pub quantity: Decimal,
    /// 진입 시점의 ATR(14) 값. 손절/익절가 계산의 기준이 됨.
    /// 만약 데이터 부족 등으로 ATR 계산이 불가능한 경우 evaluate()는 None을 반환해야 함.
    pub atr: Decimal,
    /// Signal 레벨에서 계산된 setup score (Qualification 중복 계산 방지용)
    pub setup_score: Option<i32>,
    /// 시장 레짐 (Risk sizing에서 사용)
    pub regime: Option<MarketRegime>,
}

/// 시그널 전략. 틱/캔들 컨텍스트를 받아 진입 신호를 생성한다.
#[async_trait]
pub trait SignalStrategy: Send + Sync {
    /// None 반환 시 진입 없음
    async fn evaluate(&self, ctx: &SignalContext, db: &sqlx::SqlitePool) -> Option<TradeSignal>;
}

// ── Qualification ─────────────────────────────────────────────────────────

/// 진입 후보에 대한 최종 심사 입력
#[derive(Debug, Clone)]
pub struct SignalCandidate {
    pub signal: TradeSignal,
    pub regime: MarketRegime,
    /// Signal 레벨에서 계산된 setup score (있으면 Qualification에서 재계산 안 함)
    pub setup_score: Option<i32>,
}

/// 심사 결과
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QualResult {
    /// 진입 허용
    Pass,
    /// 진입 차단
    Block { reason: String },
}

/// 진입 자격 심사 전략
pub trait QualificationStrategy: Send + Sync {
    fn qualify(&self, candidate: &SignalCandidate) -> QualResult;
}

// ── Risk ──────────────────────────────────────────────────────────────────

/// 포지션 사이징에 필요한 포트폴리오 스냅샷
#[derive(Debug, Clone)]
pub struct Portfolio {
    pub balance: Decimal,
    pub open_position_count: u32,
    pub daily_pnl_r: f64,
}

/// 리스크 전략. 진입 허용된 신호의 포지션 크기를 결정한다.
pub trait RiskStrategy: Send + Sync {
    /// 반환값: 주문 수량 (0이면 주문 안 함)
    fn size(&self, signal: &TradeSignal, portfolio: &Portfolio) -> Decimal;
}

// ── Bundle ────────────────────────────────────────────────────────────────

/// 프레임워크에 주입되는 전략 구현체 묶음
pub struct StrategyBundle {
    pub discovery: Box<dyn DiscoveryStrategy>,
    pub regime: Box<dyn RegimeStrategy>,
    pub signal: Box<dyn SignalStrategy>,
    pub qualification: Box<dyn QualificationStrategy>,
    pub risk: Box<dyn RiskStrategy>,
}
