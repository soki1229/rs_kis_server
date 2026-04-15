use crate::error::BotError;
use crate::types::{KillSwitchFile, KillSwitchMode};
use chrono::{TimeZone, Utc};

pub struct KillSwitch {
    path: String,
}

impl KillSwitch {
    pub fn new(path: String) -> Self {
        Self { path }
    }

    /// 파일 존재 시 파싱. None = Kill Switch 비활성.
    #[allow(dead_code)]
    pub fn current_mode(&self) -> Option<KillSwitchMode> {
        let content = std::fs::read_to_string(&self.path).ok()?;
        let file: KillSwitchFile = serde_json::from_str(&content).ok()?;
        Some(file.mode)
    }

    /// Kill Switch 발동. 파일에 JSON으로 기록.
    pub fn activate(
        &self,
        mode: KillSwitchMode,
        reason: &str,
        details: &str,
    ) -> Result<(), BotError> {
        let kst = chrono::FixedOffset::east_opt(9 * 3600)
            .unwrap()
            .from_utc_datetime(&Utc::now().naive_utc());
        let file = KillSwitchFile {
            mode,
            reason: reason.to_string(),
            triggered_at: kst,
            details: details.to_string(),
        };
        let json = serde_json::to_string_pretty(&file).map_err(BotError::Json)?;
        std::fs::write(&self.path, json).map_err(BotError::Io)
    }

    /// Kill Switch 해제. 파일 삭제.
    pub fn clear(&self) -> Result<(), BotError> {
        if std::path::Path::new(&self.path).exists() {
            std::fs::remove_file(&self.path).map_err(BotError::Io)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::KillSwitchMode;

    fn temp_ks_path() -> String {
        let dir = tempfile::tempdir().unwrap();
        let path = dir
            .path()
            .join("test_ks.json")
            .to_str()
            .unwrap()
            .to_string();
        // Leak the tempdir so it isn't deleted during test
        std::mem::forget(dir);
        path
    }

    #[test]
    fn no_file_means_no_kill_switch() {
        let ks = KillSwitch::new(temp_ks_path());
        assert!(ks.current_mode().is_none());
    }

    #[test]
    fn hard_kill_switch_persists() {
        let path = temp_ks_path();
        let ks = KillSwitch::new(path.clone());
        ks.activate(KillSwitchMode::Hard, "test", "details")
            .unwrap();

        let ks2 = KillSwitch::new(path);
        assert_eq!(ks2.current_mode(), Some(KillSwitchMode::Hard));
    }

    #[test]
    fn soft_kill_switch_persists_to_file() {
        let ks = KillSwitch::new(temp_ks_path());
        ks.activate(KillSwitchMode::Soft, "test", "details")
            .unwrap();
        assert_eq!(ks.current_mode(), Some(KillSwitchMode::Soft));
    }

    #[test]
    fn clear_removes_file() {
        let ks = KillSwitch::new(temp_ks_path());
        ks.activate(KillSwitchMode::Hard, "test", "details")
            .unwrap();
        ks.clear().unwrap();
        assert!(ks.current_mode().is_none());
    }

    #[test]
    fn file_content_includes_reason() {
        let path = temp_ks_path();
        let ks = KillSwitch::new(path.clone());
        ks.activate(KillSwitchMode::Hard, "test reason", "details")
            .unwrap();

        let content = std::fs::read_to_string(path).unwrap();
        assert!(content.contains("test reason"));
        assert!(content.contains("Hard"));
    }
}
