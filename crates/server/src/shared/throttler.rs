use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// KIS API 전역 슬로틀러.
/// AppKey 단위로 적용되는 KIS의 TPS 제한을 준수하기 위해 모든 어댑터가 이 객체를 공유합니다.
pub struct KisThrottler {
    last_call: Mutex<Instant>,
    throttle_ms: u64,
}

impl KisThrottler {
    pub fn new(throttle_ms: u64) -> Self {
        Self {
            last_call: Mutex::new(Instant::now() - Duration::from_secs(10)),
            throttle_ms,
        }
    }

    /// API 호출 전 대기. 물리적 TPS 제한 준수 보장.
    pub async fn wait(&self) {
        let mut last = self.last_call.lock().await;
        let elapsed = last.elapsed();
        let wait_dur = Duration::from_millis(self.throttle_ms);

        if elapsed < wait_dur {
            tokio::time::sleep(wait_dur - elapsed).await;
        }
        *last = Instant::now();
    }
}
