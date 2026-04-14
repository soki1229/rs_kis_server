use crate::config::ProfileName;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MonitoringInput {
    pub current_profile: ProfileName,
    /// 최근 7일 누적 R (손절 기준 R 배수)
    pub rolling_7d_r: f64,
    /// 최대 드로다운 비율 (예: 0.05 = 5%)
    pub mdd_pct: f64,
    /// 동일 레짐 연속 손실 횟수
    pub regime_consecutive_losses: u32,
    /// 최근 30일 누적 R
    pub rolling_30d_r: f64,
    /// LLM ENTER 승률 - rule-only 승률 (음수 = LLM 부진)
    pub llm_win_rate_vs_rule: f64,
    /// Setup Score 80+ 구간 승률
    pub score_80plus_win_rate: f64,
    /// Conservative 모드로 전환된 후 경과 거래일 수
    pub conservative_days_elapsed: u32,
    /// 현재 연속 손실 횟수
    pub consecutive_losses: u32,
    /// Conservative 전환 이후 7일 R
    pub conservative_7d_r: f64,
}

impl Default for MonitoringInput {
    fn default() -> Self {
        Self {
            current_profile: ProfileName::Default,
            rolling_7d_r: 0.0,
            mdd_pct: 0.0,
            regime_consecutive_losses: 0,
            rolling_30d_r: 0.0,
            llm_win_rate_vs_rule: 0.0,
            score_80plus_win_rate: 0.5,
            conservative_days_elapsed: 0,
            consecutive_losses: 0,
            conservative_7d_r: 0.0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub enum MonitoringDecision {
    /// Default → Conservative 강제 전환
    ForceConservative { reason: String },
    /// Conservative → Default 복귀 허용 (쿨다운 3거래일 + 조건 충족)
    ReturnToDefault,
    /// 동일 레짐 5연속 손실 → 해당 레짐 7일 중단
    SuspendRegime { days: u32 },
    /// WARN 알림만 발송 (행동 변경 없음)
    WarnAlert { message: String },
    /// INFO 알림만 발송
    InfoAlert { message: String },
}

/// 스펙 Section 15 경보 vs 강제 전환 평가. 순수 함수.
/// 하나의 상황에 여러 MonitoringDecision이 동시에 반환될 수 있음.
#[allow(dead_code)]
pub fn evaluate_monitoring(input: &MonitoringInput) -> Vec<MonitoringDecision> {
    let mut decisions = Vec::new();

    // ── 강제 전환 ──────────────────────────────────────────────────────

    if input.current_profile == ProfileName::Default {
        if input.rolling_7d_r < -2.0 {
            decisions.push(MonitoringDecision::ForceConservative {
                reason: format!("7d R = {:.1}R < -2R", input.rolling_7d_r),
            });
        }
        if input.mdd_pct >= 0.05 {
            decisions.push(MonitoringDecision::ForceConservative {
                reason: format!("MDD {:.1}% >= 5%", input.mdd_pct * 100.0),
            });
        }
    }

    if input.regime_consecutive_losses >= 5 {
        decisions.push(MonitoringDecision::SuspendRegime { days: 7 });
    }

    // ── Conservative → Default 복귀 (쿨다운 + 조건 충족) ──────────────

    if input.current_profile == ProfileName::Conservative
        && input.conservative_days_elapsed >= 3
        && input.conservative_7d_r > 1.0
        && input.consecutive_losses == 0
    {
        decisions.push(MonitoringDecision::ReturnToDefault);
    }

    // ── 알림 전용 (행동 변경 없음) ────────────────────────────────────

    if input.rolling_30d_r < -5.0 {
        decisions.push(MonitoringDecision::WarnAlert {
            message: format!(
                "[WARN] 30-day cumulative R below -5R ({:.1}R). Strategy parameter review recommended.",
                input.rolling_30d_r
            ),
        });
    }

    if input.mdd_pct >= 0.10 {
        decisions.push(MonitoringDecision::WarnAlert {
            message: "[WARN] MDD reached -10%. Review strategy before next session.".to_string(),
        });
    }

    if input.llm_win_rate_vs_rule < -0.10 {
        decisions.push(MonitoringDecision::InfoAlert {
            message: "[INFO] LLM ENTER win rate underperforming rule-only by 10%p for 3 weeks. Consider raising setup_score_threshold_llm.".to_string(),
        });
    }

    if input.score_80plus_win_rate < 0.40 {
        decisions.push(MonitoringDecision::InfoAlert {
            message: "[INFO] Score 80+ win rate below 40% for 2 weeks. Check signal thresholds or feature design.".to_string(),
        });
    }

    decisions
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_conservative_when_7d_r_below_minus_2() {
        let input = MonitoringInput {
            rolling_7d_r: -2.1,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(d.iter().any(|x| matches!(x, MonitoringDecision::ForceConservative { .. })));
    }

    #[test]
    fn force_conservative_when_mdd_5pct() {
        let input = MonitoringInput {
            mdd_pct: 0.051,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(d.iter().any(|x| matches!(x, MonitoringDecision::ForceConservative { .. })));
    }

    #[test]
    fn regime_suspend_when_5_consecutive_losses() {
        let input = MonitoringInput {
            regime_consecutive_losses: 5,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(d.iter().any(|x| matches!(x, MonitoringDecision::SuspendRegime { .. })));
    }

    #[test]
    fn return_to_default_after_cooldown_and_recovery() {
        let input = MonitoringInput {
            current_profile: ProfileName::Conservative,
            conservative_days_elapsed: 3,
            conservative_7d_r: 1.1,
            consecutive_losses: 0,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(d.iter().any(|x| matches!(x, MonitoringDecision::ReturnToDefault)));
    }

    #[test]
    fn no_return_to_default_before_cooldown() {
        let input = MonitoringInput {
            current_profile: ProfileName::Conservative,
            conservative_days_elapsed: 2,
            conservative_7d_r: 1.5,
            consecutive_losses: 0,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(!d.iter().any(|x| matches!(x, MonitoringDecision::ReturnToDefault)));
    }

    #[test]
    fn warn_alert_when_30d_r_below_minus_5() {
        let input = MonitoringInput {
            rolling_30d_r: -5.1,
            ..Default::default()
        };
        let d = evaluate_monitoring(&input);
        assert!(d.iter().any(|x| matches!(x, MonitoringDecision::WarnAlert { .. })));
    }

    #[test]
    fn no_action_when_all_clear() {
        let input = MonitoringInput::default();
        let d = evaluate_monitoring(&input);
        assert!(d.is_empty());
    }
}
