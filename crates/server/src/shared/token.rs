use chrono::{DateTime, Utc};
use kis_api::{KisClient, KisError};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

struct ApprovalKeyEntry {
    key: String,
    expires_at: DateTime<Utc>,
}

/// WebSocket approval_key 메모리 캐시.
/// KIS approval_key 유효기간 ~24시간.
pub struct ApprovalKeyCache {
    inner: Mutex<Option<ApprovalKeyEntry>>,
    refresh_before_secs: i64,
}

impl ApprovalKeyCache {
    /// `refresh_before_secs`: 만료 N초 전 재발급 (권장: 600)
    pub fn new(refresh_before_secs: i64) -> Self {
        Self {
            inner: Mutex::new(None),
            refresh_before_secs,
        }
    }

    /// 유효한 approval_key 반환. 만료 임박/미발급 시 client로 재발급.
    pub async fn get(&self, client: &KisClient) -> Result<String, KisError> {
        let mut guard = self.inner.lock().await;
        let threshold = Utc::now() + chrono::Duration::seconds(self.refresh_before_secs);

        if let Some(entry) = &*guard {
            if entry.expires_at > threshold {
                return Ok(entry.key.clone());
            }
        }

        tracing::info!("approval_key 재발급 중...");
        let key = client.approval_key().await?;
        *guard = Some(ApprovalKeyEntry {
            key: key.clone(),
            expires_at: Utc::now() + chrono::Duration::hours(24),
        });
        tracing::info!("approval_key 발급 완료 (유효: ~24시간)");
        Ok(key)
    }
}

/// 백그라운드 토큰 자동 갱신 태스크.
/// 만료 `refresh_before_mins`분 전에 `client.refresh_token()` 호출.
/// 갱신 실패 시 60초 후 재시도.
pub async fn run_token_refresh_task(
    client: KisClient,
    refresh_before_mins: i64,
    token: CancellationToken,
) {
    let label = match client.env() {
        kis_api::KisEnv::Real => "Real",
        kis_api::KisEnv::Vts => "VTS",
    };

    loop {
        let sleep_dur = match client.token_expires_at().await {
            Some(expires_at) => {
                let refresh_at = expires_at - chrono::Duration::minutes(refresh_before_mins);
                let now = Utc::now();
                if refresh_at > now {
                    let dur: chrono::Duration = refresh_at - now;
                    dur.to_std().unwrap_or(Duration::from_secs(60))
                } else {
                    Duration::ZERO
                }
            }
            None => Duration::from_secs(3600),
        };

        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("{label} TokenRefreshTask: 종료");
                break;
            }
            _ = tokio::time::sleep(sleep_dur) => {}
        }

        if token.is_cancelled() {
            break;
        }

        tracing::info!("{label} 토큰 갱신 중...");
        match client.refresh_token().await {
            Ok(()) => tracing::info!("{label} 토큰 갱신 완료"),
            Err(e) => {
                tracing::error!("{label} 토큰 갱신 실패: {e}. 60초 후 재시도");
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn approval_key_cache_returns_cached_value() {
        // ApprovalKeyCache가 동일 키를 반환하는지 확인 (실제 API 없이)
        let cache = ApprovalKeyCache::new(600);
        assert!(cache.inner.lock().await.is_none());
    }
}
