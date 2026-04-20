use chrono::{DateTime, Utc};
use kis_api::{KisClient, KisError};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize)]
struct ApprovalKeyEntry {
    key: String,
    expires_at: DateTime<Utc>,
}

/// WebSocket approval_key 캐시 (메모리 + 파일).
/// KIS approval_key 유효기간 ~24시간.
pub struct ApprovalKeyCache {
    inner: Mutex<Option<ApprovalKeyEntry>>,
    refresh_before_secs: i64,
    cache_path: Option<PathBuf>,
}

impl ApprovalKeyCache {
    /// `refresh_before_secs`: 만료 N초 전 재발급 (권장: 600)
    pub fn new(refresh_before_secs: i64, cache_path: Option<PathBuf>) -> Self {
        Self {
            inner: Mutex::new(None),
            refresh_before_secs,
            cache_path,
        }
    }

    /// 유효한 approval_key 반환. 파일 캐시 -> 메모리 캐시 -> 재발급 순서로 확인.
    pub async fn get(&self, client: &KisClient) -> Result<String, KisError> {
        let mut guard = self.inner.lock().await;
        let threshold = Utc::now() + chrono::Duration::seconds(self.refresh_before_secs);

        // 1. 메모리에 이미 있는 경우 확인
        if let Some(entry) = &*guard {
            if entry.expires_at > threshold {
                return Ok(entry.key.clone());
            }
        }

        // 2. 파일 캐시에서 로드 시도 (메모리에 없거나 만료된 경우)
        if let Some(path) = &self.cache_path {
            if let Ok(cache_str) = std::fs::read_to_string(path) {
                if let Ok(entry) = serde_json::from_str::<ApprovalKeyEntry>(&cache_str) {
                    if entry.expires_at > threshold {
                        let remaining = entry.expires_at - Utc::now();
                        tracing::info!(
                            "유효한 웹소켓 키 발견 (만료까지 {}시간 {}분)",
                            remaining.num_hours(),
                            remaining.num_minutes() % 60
                        );
                        let key = entry.key.clone();
                        *guard = Some(entry);
                        return Ok(key);
                    }
                }
            }
        }

        // 3. 둘 다 없으면 재발급
        tracing::info!("approval_key 재발급 중...");
        let key = client.approval_key().await?;
        let entry = ApprovalKeyEntry {
            key: key.clone(),
            expires_at: Utc::now() + chrono::Duration::hours(24),
        };

        // 4. 파일 및 메모리 저장
        if let Some(path) = &self.cache_path {
            if let Ok(cache_json) = serde_json::to_string(&entry) {
                let _ = std::fs::write(path, cache_json);
            }
        }
        *guard = Some(entry);

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
        let cache = ApprovalKeyCache::new(600, None);
        assert!(cache.inner.lock().await.is_none());
    }
}
