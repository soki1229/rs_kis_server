use crate::types::KillSwitchMode;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum BotError {
    #[error("Config error: {0}")]
    Config(String),

    #[error("Database error: {0}")]
    Db(#[from] sqlx::Error),

    #[error("Database migration error: {0}")]
    Migration(#[from] sqlx::migrate::MigrateError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Kill switch active: {mode:?}")]
    KillSwitchActive { mode: KillSwitchMode },

    #[error("Recovery check failed: {reason}")]
    RecoveryFailed { reason: String },

    #[error("LLM error: {0}")]
    Llm(String),

    #[error("Risk guard blocked: {reason}")]
    RiskBlocked { reason: String },

    #[error("API error: {msg}")]
    ApiError { msg: String },

    #[error("이 API는 모의투자(VTS) 환경에서 지원되지 않습니다.")]
    UnsupportedInVts,
}

impl BotError {
    pub fn handle_vts_error<T: Default>(self, label: &str) -> Result<T, Self> {
        if matches!(self, BotError::UnsupportedInVts) {
            tracing::warn!(
                "{} is not supported in VTS, skipping with default value",
                label
            );
            Ok(T::default())
        } else {
            Err(self)
        }
    }
}

impl From<kis_api::error::KisError> for BotError {
    fn from(e: kis_api::error::KisError) -> Self {
        match e {
            kis_api::error::KisError::NotSupportedInVts => BotError::UnsupportedInVts,
            _ => BotError::ApiError { msg: e.to_string() },
        }
    }
}
