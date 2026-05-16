use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ProfileName {
    Default,
    Conservative,
    Aggressive,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StrategyProfile {
    pub id: String,
    pub allocation_pct: Decimal,
    pub setup_score_min: u32,
    #[serde(default = "default_true")]
    pub regime_filter: bool,
    #[serde(default)]
    pub aggressive_mode: bool,
    /// 목표 보유 일수. swing=1~5일, position=7일+. 초과 시 강제 청산.
    #[serde(default = "default_holding_days")]
    pub holding_days_target: u32,
}

fn default_holding_days() -> u32 {
    5
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum AccountKind {
    Real,
    Vts,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct BotConfig {
    /// 0: Evaluation mode (no orders), 1: Execution mode
    #[serde(default = "default_zero")]
    pub execution_enabled: u32,
}

fn default_zero() -> u32 {
    0
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketConfig {
    pub watchlist: Vec<String>,
    pub dynamic_watchlist_size: usize,
    pub db_path: String,
    pub kill_switch_path: String,
    /// Which account to use for trading
    #[serde(default = "default_vts_account")]
    pub trading_account: AccountKind,
    /// Which provider to use for market data
    #[serde(default = "default_real_account")]
    pub data_provider: AccountKind,
    #[serde(default = "default_watchlist_refresh_interval")]
    pub watchlist_refresh_interval_secs: u64,
    #[serde(default = "default_strategies")]
    pub strategies: Vec<StrategyProfile>,
    /// Enable generic pipeline for this market (phased rollout).
    /// When true, uses MarketAdapter-based generic tasks instead of legacy market-specific tasks.
    #[serde(default)]
    pub use_generic_pipeline: bool,
    /// Volume ranking에서 제외할 종목 코드 목록 (레버리지/인버스 ETF, 매매불가 종목 등)
    #[serde(default)]
    pub symbol_blacklist: Vec<String>,
}

fn default_vts_account() -> AccountKind {
    AccountKind::Vts
}

fn default_real_account() -> AccountKind {
    AccountKind::Real
}

fn default_strategies() -> Vec<StrategyProfile> {
    use rust_decimal_macros::dec;
    vec![StrategyProfile {
        id: "stable".into(),
        allocation_pct: dec!(1.0),
        setup_score_min: 60,
        regime_filter: true,
        aggressive_mode: false,
        holding_days_target: 5,
    }]
}

fn default_watchlist_refresh_interval() -> u64 {
    600
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TwapConfig {
    #[serde(default = "default_twap_threshold")]
    pub threshold_qty: u64,
    #[serde(default = "default_twap_slices")]
    pub slice_count: u32,
    #[serde(default = "default_twap_delay")]
    pub delay_secs_per_slice: u64,
}

fn default_twap_threshold() -> u64 {
    100
}
fn default_twap_slices() -> u32 {
    5
}
fn default_twap_delay() -> u64 {
    30
}

impl Default for TwapConfig {
    fn default() -> Self {
        Self {
            threshold_qty: 100,
            slice_count: 5,
            delay_secs_per_slice: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RiskConfig {
    pub max_open_positions: u32,
    pub daily_loss_limit_pct: Decimal,
    pub consecutive_loss_limit: u32,
    pub pending_order_limit_pct: Decimal,
    pub risk_per_trade_pct: Decimal,
    pub atr_stop_multiplier: Decimal,
    pub max_position_pct: Decimal,
    #[serde(default = "default_weekly_loss_limit_pct")]
    pub weekly_loss_limit_pct: Decimal,
    #[serde(default = "default_total_drawdown_hard_stop_pct")]
    pub total_drawdown_hard_stop_pct: Decimal,
    #[serde(default)]
    pub earnings_blackout_symbols: Vec<String>,
    #[serde(default = "default_entry_blackout_open_mins")]
    pub entry_blackout_open_mins: i64,
    #[serde(default = "default_mdd_soft_kill_pct")]
    pub mdd_soft_kill_pct: Decimal,
    #[serde(default = "default_mdd_hard_kill_pct")]
    pub mdd_hard_kill_pct: Decimal,
    #[serde(default)]
    pub twap: TwapConfig,
}

fn default_weekly_loss_limit_pct() -> Decimal {
    rust_decimal_macros::dec!(0.05)
}

fn default_total_drawdown_hard_stop_pct() -> Decimal {
    rust_decimal_macros::dec!(0.15)
}

fn default_entry_blackout_open_mins() -> i64 {
    15
}

fn default_mdd_soft_kill_pct() -> Decimal {
    rust_decimal_macros::dec!(0.10)
}

fn default_mdd_hard_kill_pct() -> Decimal {
    rust_decimal_macros::dec!(0.15)
}

impl Default for RiskConfig {
    fn default() -> Self {
        use rust_decimal_macros::dec;
        Self {
            max_open_positions: 5,
            daily_loss_limit_pct: dec!(0.02),
            consecutive_loss_limit: 3,
            pending_order_limit_pct: dec!(0.30),
            risk_per_trade_pct: dec!(0.005),
            atr_stop_multiplier: dec!(1.5),
            max_position_pct: dec!(0.10),
            weekly_loss_limit_pct: dec!(0.05),
            total_drawdown_hard_stop_pct: dec!(0.15),
            earnings_blackout_symbols: vec![],
            entry_blackout_open_mins: 15,
            mdd_soft_kill_pct: dec!(0.10),
            mdd_hard_kill_pct: dec!(0.15),
            twap: TwapConfig::default(),
        }
    }
}

impl RiskConfig {
    pub fn validate(&self) -> anyhow::Result<()> {
        use rust_decimal_macros::dec;

        if self.risk_per_trade_pct <= Decimal::ZERO || self.risk_per_trade_pct > dec!(0.05) {
            anyhow::bail!(
                "risk_per_trade_pct={} out of range (0, 0.05]",
                self.risk_per_trade_pct
            );
        }
        if self.max_position_pct <= Decimal::ZERO || self.max_position_pct > dec!(0.30) {
            anyhow::bail!(
                "max_position_pct={} out of range (0, 0.30]",
                self.max_position_pct
            );
        }
        if self.daily_loss_limit_pct <= Decimal::ZERO || self.daily_loss_limit_pct > dec!(0.10) {
            anyhow::bail!(
                "daily_loss_limit_pct={} out of range (0, 0.10]",
                self.daily_loss_limit_pct
            );
        }
        if self.atr_stop_multiplier <= dec!(0.5) || self.atr_stop_multiplier > dec!(5.0) {
            anyhow::bail!(
                "atr_stop_multiplier={} out of range (0.5, 5.0]",
                self.atr_stop_multiplier
            );
        }
        if self.max_open_positions == 0 || self.max_open_positions > 20 {
            anyhow::bail!(
                "max_open_positions={} out of range [1, 20]",
                self.max_open_positions
            );
        }
        if self.pending_order_limit_pct <= Decimal::ZERO
            || self.pending_order_limit_pct > Decimal::ONE
        {
            anyhow::bail!(
                "pending_order_limit_pct={} out of range (0, 1.0]",
                self.pending_order_limit_pct
            );
        }
        if self.weekly_loss_limit_pct <= Decimal::ZERO || self.weekly_loss_limit_pct > dec!(0.20) {
            anyhow::bail!(
                "weekly_loss_limit_pct={} out of range (0, 0.20]",
                self.weekly_loss_limit_pct
            );
        }
        if self.total_drawdown_hard_stop_pct <= Decimal::ZERO
            || self.total_drawdown_hard_stop_pct > dec!(0.50)
        {
            anyhow::bail!(
                "total_drawdown_hard_stop_pct={} out of range (0, 0.50]",
                self.total_drawdown_hard_stop_pct
            );
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SignalConfig {
    pub setup_score_min: u32,
    pub llm_score_threshold: u32,
    pub llm_strength_threshold: f64,
    #[serde(default = "default_llm_model")]
    pub llm_model: String,
    #[serde(default = "default_llm_circuit_breaker")]
    pub llm_circuit_breaker_threshold: u32,
    #[serde(default = "default_llm_timeout")]
    pub llm_timeout_secs: u64,
    #[serde(default = "default_signal_cooldown")]
    pub signal_cooldown_secs: u64,
    #[serde(default = "default_true")]
    pub llm_enabled: bool,
    #[serde(default = "default_candle_interval")]
    pub candle_interval_secs: u64,
    #[serde(default = "default_volume_ratio_threshold")]
    pub volume_ratio_threshold: f64,
    #[serde(default = "default_recent_volume_ratio_threshold")]
    pub recent_volume_ratio_threshold: f64,
}

fn default_llm_model() -> String {
    "claude-haiku-20240307".to_string()
}

fn default_llm_circuit_breaker() -> u32 {
    5
}

fn default_llm_timeout() -> u64 {
    10
}

fn default_signal_cooldown() -> u64 {
    60
}

fn default_candle_interval() -> u64 {
    30
}

fn default_volume_ratio_threshold() -> f64 {
    1.5
}

fn default_recent_volume_ratio_threshold() -> f64 {
    1.2
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            setup_score_min: 50,
            llm_score_threshold: 80,
            llm_strength_threshold: 0.50,
            llm_model: default_llm_model(),
            llm_circuit_breaker_threshold: default_llm_circuit_breaker(),
            llm_timeout_secs: default_llm_timeout(),
            signal_cooldown_secs: default_signal_cooldown(),
            llm_enabled: true,
            candle_interval_secs: default_candle_interval(),
            volume_ratio_threshold: default_volume_ratio_threshold(),
            recent_volume_ratio_threshold: default_recent_volume_ratio_threshold(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PositionConfig {
    pub stop_atr_multiplier: Decimal,
    pub profit_target_1_atr: Decimal,
    pub profit_target_2_atr: Decimal,
    pub trailing_atr_trending: Decimal,
    pub trailing_atr_volatile: Decimal,
    pub partial_exit_pct: Decimal,
    pub entry_blackout_close_mins: u32,
    pub limit_price_atr_cushion: Decimal,
    /// US market FX spread compensation (e.g., 0.005 = 0.5%)
    #[serde(default = "default_us_fx_spread_pct")]
    pub us_fx_spread_pct: Decimal,
    /// 장 마감 시 모든 포지션 강제 청산 여부.
    /// true = 데이 트레이딩 (매일 청산), false = 스윙 트레이딩 (trailing stop/목표가 청산).
    #[serde(default)]
    pub eod_liquidation: bool,
}

fn default_us_fx_spread_pct() -> Decimal {
    rust_decimal_macros::dec!(0.005)
}

impl Default for PositionConfig {
    fn default() -> Self {
        use rust_decimal_macros::dec;
        Self {
            stop_atr_multiplier: dec!(2.0),
            profit_target_1_atr: dec!(2.0),
            profit_target_2_atr: dec!(4.0),
            trailing_atr_trending: dec!(1.5),
            trailing_atr_volatile: dec!(2.0),
            partial_exit_pct: dec!(0.5),
            entry_blackout_close_mins: 15,
            limit_price_atr_cushion: dec!(0.10),
            us_fx_spread_pct: dec!(0.005),
            eod_liquidation: false,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TunableConfig {
    pub signal: SignalConfig,
    pub position: PositionConfig,
}

impl TunableConfig {
    pub fn from_server_config(cfg: &ServerConfig) -> Self {
        Self {
            signal: cfg.signal.clone(),
            position: cfg.position.clone(),
        }
    }

    pub fn apply(&mut self, param: &str, new_value: &str) -> bool {
        match param {
            "setup_score_min" => {
                if let Ok(v) = new_value.parse::<u32>() {
                    self.signal.setup_score_min = v;
                    return true;
                }
            }
            "stop_atr_multiplier" => {
                if let Ok(v) = new_value.parse::<Decimal>() {
                    self.position.stop_atr_multiplier = v;
                    return true;
                }
            }
            "profit_target_1_atr" => {
                if let Ok(v) = new_value.parse::<Decimal>() {
                    self.position.profit_target_1_atr = v;
                    return true;
                }
            }
            "profit_target_2_atr" => {
                if let Ok(v) = new_value.parse::<Decimal>() {
                    self.position.profit_target_2_atr = v;
                    return true;
                }
            }
            "trailing_atr_trending" => {
                if let Ok(v) = new_value.parse::<Decimal>() {
                    self.position.trailing_atr_trending = v;
                    return true;
                }
            }
            "trailing_atr_volatile" => {
                if let Ok(v) = new_value.parse::<Decimal>() {
                    self.position.trailing_atr_volatile = v;
                    return true;
                }
            }
            _ => {
                tracing::warn!("TunableConfig: unknown parameter '{}'", param);
            }
        }
        false
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TokenCacheConfig {
    /// 실전 토큰 캐시 파일 경로
    #[serde(default = "default_real_token_cache_path")]
    pub real_path: String,
    /// 실전 웹소켓 키 캐시 파일 경로
    #[serde(default = "default_real_approval_cache_path")]
    pub real_approval_path: String,
    /// 모의투자 토큰 캐시 파일 경로
    #[serde(default = "default_vts_token_cache_path")]
    pub vts_path: String,
    /// 모의투자 웹소켓 키 캐시 파일 경로
    #[serde(default = "default_vts_approval_cache_path")]
    pub vts_approval_path: String,
}

fn default_real_token_cache_path() -> String {
    "~/.config/kis/token_real.json".to_string()
}
fn default_real_approval_cache_path() -> String {
    "~/.config/kis/approval_real.json".to_string()
}
fn default_vts_token_cache_path() -> String {
    "~/.config/kis/token_vts.json".to_string()
}
fn default_vts_approval_cache_path() -> String {
    "~/.config/kis/approval_vts.json".to_string()
}

impl Default for TokenCacheConfig {
    fn default() -> Self {
        Self {
            real_path: default_real_token_cache_path(),
            real_approval_path: default_real_approval_cache_path(),
            vts_path: default_vts_token_cache_path(),
            vts_approval_path: default_vts_approval_cache_path(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct ServerConfig {
    pub rest_port: u16,
    #[serde(default)]
    pub bot: BotConfig,
    pub kr: MarketConfig,
    pub us: MarketConfig,
    #[serde(default)]
    pub risk: RiskConfig,
    #[serde(default)]
    pub signal: SignalConfig,
    #[serde(default)]
    pub position: PositionConfig,
    #[serde(default)]
    pub token_cache: TokenCacheConfig,
    #[serde(default = "default_env_file")]
    pub env_file: String,
    #[serde(default)]
    pub integrations: IntegrationConfig,
}

fn default_env_file() -> String {
    ".env".to_string()
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct IntegrationConfig {
    #[serde(default)]
    pub telegram: TelegramConfig,
    #[serde(default)]
    pub notion: NotionConfig,
    #[serde(default)]
    pub llm: LlmConfig,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct TelegramConfig {
    #[serde(default)]
    pub alert_chat_id: Option<i64>,
    #[serde(default)]
    pub monitor_chat_id: Option<i64>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct NotionConfig {
    #[serde(default)]
    pub page_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LlmConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
}

fn default_true() -> bool {
    true
}

impl Default for LlmConfig {
    fn default() -> Self {
        Self { enabled: true }
    }
}

#[derive(Debug)]
pub struct Secrets {
    pub alert_bot_token: String,
    pub alert_chat_id: i64,
    pub monitor_bot_token: String,
    pub monitor_chat_id: i64,
    pub rest_admin_token: String,
    pub notion_token: String,
    pub notion_page_id: String,
    // KIS API Keys
    pub vts_app_key: String,
    pub vts_app_secret: String,
    pub real_app_key: String,
    pub real_app_secret: String,
}

impl Secrets {
    pub fn from_env(overrides: &IntegrationConfig) -> anyhow::Result<Self> {
        let alert_chat_id: i64 = match overrides.telegram.alert_chat_id {
            Some(id) => id,
            None => std::env::var("TELEGRAM_ALERT_CHAT_ID")?.parse()?,
        };
        let monitor_chat_id: i64 = match overrides.telegram.monitor_chat_id {
            Some(id) => id,
            None => std::env::var("TELEGRAM_MONITOR_CHAT_ID")?.parse()?,
        };
        let notion_page_id = match &overrides.notion.page_id {
            Some(id) => id.clone(),
            None => std::env::var("NOTION_PAGE_ID")?,
        };

        // KIS Keys logic:
        // 1. VTS Key: KIS_VTS_APP_KEY > VTS_APP_KEY > KIS_APP_KEY (Existing legacy rule)
        let vts_app_key = std::env::var("KIS_VTS_APP_KEY")
            .or_else(|_| std::env::var("VTS_APP_KEY"))
            .or_else(|_| std::env::var("KIS_APP_KEY"))?;
        let vts_app_secret = std::env::var("KIS_VTS_APP_SECRET")
            .or_else(|_| std::env::var("VTS_APP_SECRET"))
            .or_else(|_| std::env::var("KIS_APP_SECRET"))?;

        // 2. Real Key: KIS_REAL_APP_KEY > KIS_APP_KEY (Default to KIS_APP_KEY)
        let real_app_key = std::env::var("KIS_REAL_APP_KEY")
            .or_else(|_| std::env::var("KIS_APP_KEY"))
            .unwrap_or_else(|_| vts_app_key.clone());
        let real_app_secret = std::env::var("KIS_REAL_APP_SECRET")
            .or_else(|_| std::env::var("KIS_APP_SECRET"))
            .unwrap_or_else(|_| vts_app_secret.clone());

        Ok(Self {
            alert_bot_token: std::env::var("TELEGRAM_ALERT_BOT_TOKEN")?,
            alert_chat_id,
            monitor_bot_token: std::env::var("TELEGRAM_MONITOR_BOT_TOKEN")?,
            monitor_chat_id,
            rest_admin_token: std::env::var("REST_ADMIN_TOKEN")?,
            notion_token: std::env::var("NOTION_TOKEN")?,
            notion_page_id,
            vts_app_key,
            vts_app_secret,
            real_app_key,
            real_app_secret,
        })
    }
}

impl ServerConfig {
    pub fn from_file(path: &str) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("failed to read config file '{}': {}", path, e))?;
        let mut cfg: Self = toml::from_str(&content)?;

        cfg.risk.validate()?;

        match dotenvy::from_filename(&cfg.env_file) {
            Ok(_) => tracing::info!("loaded env_file: {}", cfg.env_file),
            Err(e) if e.not_found() => {
                tracing::warn!(
                    "env_file '{}' not found, using process environment",
                    cfg.env_file
                );
            }
            Err(e) => {
                return Err(anyhow::anyhow!(
                    "failed to load env_file '{}': {}",
                    cfg.env_file,
                    e
                ));
            }
        }

        if !cfg.integrations.llm.enabled {
            cfg.signal.llm_enabled = false;
        }

        Ok(cfg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_config_has_defaults() {
        let mc = MarketConfig {
            watchlist: vec!["NVDA".into()],
            dynamic_watchlist_size: 10,
            db_path: "/tmp/test.db".into(),
            kill_switch_path: "/tmp/ks.json".into(),
            trading_account: AccountKind::Vts,
            data_provider: AccountKind::Real,
            watchlist_refresh_interval_secs: default_watchlist_refresh_interval(),
            strategies: default_strategies(),
            use_generic_pipeline: false,
            symbol_blacklist: vec![],
        };
        assert_eq!(mc.watchlist.len(), 1);
        assert_eq!(mc.dynamic_watchlist_size, 10);
        assert_eq!(mc.watchlist_refresh_interval_secs, 600);
        assert_eq!(mc.trading_account, AccountKind::Vts);
        assert_eq!(mc.data_provider, AccountKind::Real);
        assert!(!mc.use_generic_pipeline);
    }

    #[test]
    fn risk_config_default_passes_validation() {
        RiskConfig::default().validate().unwrap();
    }

    #[test]
    fn risk_config_rejects_pct_over_one() {
        use rust_decimal_macros::dec;
        let cfg = RiskConfig {
            daily_loss_limit_pct: dec!(2.0),
            ..Default::default()
        };
        assert!(cfg.validate().is_err());
    }
}
