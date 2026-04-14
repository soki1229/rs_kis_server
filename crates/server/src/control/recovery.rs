use rust_decimal::Decimal;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryFailureCode {
    BalanceMismatch,
    OrphanedOrder,
    BrokerOrderMissing,
}

#[derive(Debug, Clone)]
pub enum RecoveryOutcome {
    /// 모든 검사 통과 → 정상 기동
    Pass,
    /// UNRECONCILED_FILL: DB 상태만 갱신하고 계속 진행
    AutoFixed { count: usize },
    /// Kill Switch 발동 필요
    Fail {
        code: RecoveryFailureCode,
        detail: String,
    },
}

pub struct RecoveryInput {
    /// DB `positions` 테이블 총 평가액
    pub db_position_total: Decimal,
    /// 브로커 `balance()` 총 평가액
    pub broker_balance_total: Decimal,
    /// DB에 `SUBMITTED` 상태이나 브로커 `unfilled_orders`에 없는 주문 존재
    pub has_orphaned_submitted_orders: bool,
    /// DB는 `SUBMITTED`이나 브로커에서 체결 완료된 주문 수 (WS 누락)
    pub unreconciled_fill_count: usize,
    /// DB에 `broker_order_id` 없는 `SUBMITTED` 주문 존재
    pub has_orders_without_broker_id: bool,
    /// 불일치 허용 임계값 (예: 0.05 = 5%)
    pub mismatch_threshold_pct: Decimal,
}

/// 스펙 Section 9 재시작 복구 절차 (순수 함수 — 실제 API 호출 없음).
pub fn run_recovery_check(input: &RecoveryInput) -> RecoveryOutcome {
    // 1순위: broker_order_id 없는 SUBMITTED 주문
    if input.has_orders_without_broker_id {
        return RecoveryOutcome::Fail {
            code: RecoveryFailureCode::BrokerOrderMissing,
            detail: "SUBMITTED orders found without broker_order_id".to_string(),
        };
    }

    // 2순위: 잔고 불일치
    if !input.broker_balance_total.is_zero() {
        let diff = (input.db_position_total - input.broker_balance_total).abs();
        let pct = diff / input.broker_balance_total;
        if pct > input.mismatch_threshold_pct {
            return RecoveryOutcome::Fail {
                code: RecoveryFailureCode::BalanceMismatch,
                detail: format!(
                    "db={} broker={} diff={:.1}%",
                    input.db_position_total,
                    input.broker_balance_total,
                    pct * Decimal::ONE_HUNDRED
                ),
            };
        }
    }

    // 3순위: orphaned order
    if input.has_orphaned_submitted_orders {
        return RecoveryOutcome::Fail {
            code: RecoveryFailureCode::OrphanedOrder,
            detail: "SUBMITTED orders not found in broker unfilled_orders".to_string(),
        };
    }

    // UNRECONCILED_FILL: 자동 복구 (Kill Switch 없음)
    if input.unreconciled_fill_count > 0 {
        return RecoveryOutcome::AutoFixed {
            count: input.unreconciled_fill_count,
        };
    }

    RecoveryOutcome::Pass
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    fn make_input(
        db_total: f64,
        broker_total: f64,
        orphaned: bool,
        unreconciled_fills: usize,
    ) -> RecoveryInput {
        RecoveryInput {
            db_position_total: Decimal::try_from(db_total).unwrap(),
            broker_balance_total: Decimal::try_from(broker_total).unwrap(),
            has_orphaned_submitted_orders: orphaned,
            unreconciled_fill_count: unreconciled_fills,
            has_orders_without_broker_id: false,
            mismatch_threshold_pct: dec!(0.05),
        }
    }

    #[test]
    fn clean_state_passes() {
        let input = make_input(1000.0, 1010.0, false, 0);
        let result = run_recovery_check(&input);
        assert!(matches!(result, RecoveryOutcome::Pass));
    }

    #[test]
    fn large_balance_mismatch_fails() {
        let input = make_input(1000.0, 1100.0, false, 0);
        let result = run_recovery_check(&input);
        assert!(matches!(
            result,
            RecoveryOutcome::Fail {
                code: RecoveryFailureCode::BalanceMismatch,
                ..
            }
        ));
    }

    #[test]
    fn orphaned_order_fails() {
        let input = make_input(1000.0, 1000.0, true, 0);
        let result = run_recovery_check(&input);
        assert!(matches!(
            result,
            RecoveryOutcome::Fail {
                code: RecoveryFailureCode::OrphanedOrder,
                ..
            }
        ));
    }

    #[test]
    fn unreconciled_fill_is_auto_fixed_not_fail() {
        let input = make_input(1000.0, 1000.0, false, 2);
        let result = run_recovery_check(&input);
        assert!(matches!(result, RecoveryOutcome::AutoFixed { count: 2 }));
    }

    #[test]
    fn missing_broker_id_fails() {
        let mut input = make_input(1000.0, 1000.0, false, 0);
        input.has_orders_without_broker_id = true;
        let result = run_recovery_check(&input);
        assert!(matches!(
            result,
            RecoveryOutcome::Fail {
                code: RecoveryFailureCode::BrokerOrderMissing,
                ..
            }
        ));
    }
}
