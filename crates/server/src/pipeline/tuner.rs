use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use sqlx::SqlitePool;
use std::collections::HashMap;

/// Guardrail: allowed ranges for each tunable parameter.
/// Values outside these bounds are rejected.
struct ParamBounds {
    min: f64,
    max: f64,
    step: f64,
}

fn guardrails() -> HashMap<&'static str, ParamBounds> {
    let mut m = HashMap::new();
    m.insert(
        "setup_score_min",
        ParamBounds {
            min: 40.0,
            max: 85.0,
            step: 5.0,
        },
    );
    m.insert(
        "stop_atr_multiplier",
        ParamBounds {
            min: 1.0,
            max: 4.0,
            step: 0.25,
        },
    );
    m.insert(
        "profit_target_1_atr",
        ParamBounds {
            min: 1.0,
            max: 5.0,
            step: 0.5,
        },
    );
    m.insert(
        "profit_target_2_atr",
        ParamBounds {
            min: 2.0,
            max: 8.0,
            step: 0.5,
        },
    );
    m.insert(
        "trailing_atr_trending",
        ParamBounds {
            min: 0.5,
            max: 3.0,
            step: 0.25,
        },
    );
    m.insert(
        "trailing_atr_volatile",
        ParamBounds {
            min: 1.0,
            max: 4.0,
            step: 0.25,
        },
    );
    m
}

/// Minimum number of trades required before tuner will propose any changes.
const MIN_TRADES_FOR_TUNING: u32 = 10;
/// Minimum observation days required.
const MIN_DAYS_FOR_TUNING: u32 = 5;
/// Cooldown: same parameter cannot be changed again within this many calendar days.
const COOLDOWN_DAYS: i64 = 3;

/// A single proposed parameter adjustment.
#[derive(Debug, Clone)]
pub struct ParamAdjustment {
    pub parameter: String,
    pub old_value: String,
    pub new_value: String,
    pub reason: String,
}

/// Stats window for tuner analysis.
struct TunerStats {
    days: u32,
    total_pnl: Decimal,
    losing_days: u32,
    total_trades: u32,
    score_buckets: Vec<(String, u32, Decimal)>,
}

/// Analyze recent N days of trading data and propose parameter adjustments.
/// Works purely from SQLite — no LLM required.
/// Returns at most ONE adjustment per invocation (one-change-per-day policy).
pub async fn analyze_and_propose(
    db: &SqlitePool,
    lookback_days: u32,
    signal_cfg: &crate::config::SignalConfig,
    position_cfg: &crate::config::PositionConfig,
) -> Vec<ParamAdjustment> {
    ensure_param_changes_table(db).await;

    let stats = match collect_tuner_stats(db, lookback_days).await {
        Some(s) => s,
        None => return vec![],
    };

    if stats.days < MIN_DAYS_FOR_TUNING || stats.total_trades < MIN_TRADES_FOR_TUNING {
        tracing::info!(
            "Tuner: insufficient data ({}d / {} trades, need {}d / {} trades) — skipping",
            stats.days,
            stats.total_trades,
            MIN_DAYS_FOR_TUNING,
            MIN_TRADES_FOR_TUNING
        );
        return vec![];
    }

    let guards = guardrails();

    // Evaluate rules in priority order; return first valid proposal (one-change-per-day).
    let candidates: Vec<Option<ParamAdjustment>> = vec![
        evaluate_rule1(&stats, signal_cfg, &guards),
        evaluate_rule2(&stats, position_cfg, &guards),
        evaluate_rule3(&stats, signal_cfg, &guards),
    ];

    for candidate in candidates.into_iter().flatten() {
        if check_cooldown(db, &candidate.parameter).await {
            return vec![candidate];
        } else {
            tracing::info!(
                "Tuner: skipping '{}' — cooldown active (changed within {}d)",
                candidate.parameter,
                COOLDOWN_DAYS
            );
        }
    }

    vec![]
}

/// Rule 1: If low-score entries (60-69) make up >= 40% of all entries AND
/// overall PnL is negative, raise setup_score_min by one step.
fn evaluate_rule1(
    stats: &TunerStats,
    signal_cfg: &crate::config::SignalConfig,
    guards: &HashMap<&str, ParamBounds>,
) -> Option<ParamAdjustment> {
    let total_bucket_count: u32 = stats.score_buckets.iter().map(|(_, c, _)| c).sum();
    let (_, low_count, _) = stats
        .score_buckets
        .iter()
        .find(|(label, _, _)| label == "60-69")?;

    let low_ratio = if total_bucket_count > 0 {
        *low_count as f64 / total_bucket_count as f64
    } else {
        return None;
    };

    if *low_count < 3 || low_ratio < 0.4 || stats.total_pnl >= Decimal::ZERO {
        return None;
    }

    let bounds = guards.get("setup_score_min")?;
    let current = signal_cfg.setup_score_min as f64;
    let proposed = (current + bounds.step).min(bounds.max);
    if (proposed - current).abs() < f64::EPSILON {
        return None;
    }

    Some(ParamAdjustment {
        parameter: "setup_score_min".into(),
        old_value: format!("{}", signal_cfg.setup_score_min),
        new_value: format!("{}", proposed as u32),
        reason: format!(
            "Score 60-69 entries: {}/{} ({:.0}%) with overall PnL {} — raising threshold",
            low_count,
            total_bucket_count,
            low_ratio * 100.0,
            stats.total_pnl
        ),
    })
}

/// Rule 2: If losing_days >= 60% of window AND total_pnl < 0,
/// tighten stop_atr_multiplier by one step.
fn evaluate_rule2(
    stats: &TunerStats,
    position_cfg: &crate::config::PositionConfig,
    guards: &HashMap<&str, ParamBounds>,
) -> Option<ParamAdjustment> {
    if stats.days < 3 {
        return None;
    }
    let losing_ratio = stats.losing_days as f64 / stats.days as f64;
    if losing_ratio < 0.6 || stats.total_pnl >= Decimal::ZERO {
        return None;
    }

    let bounds = guards.get("stop_atr_multiplier")?;
    let current = position_cfg.stop_atr_multiplier.to_f64().unwrap_or(2.0);
    let proposed = (current - bounds.step).max(bounds.min);
    if (proposed - current).abs() < f64::EPSILON {
        return None;
    }

    Some(ParamAdjustment {
        parameter: "stop_atr_multiplier".into(),
        old_value: position_cfg.stop_atr_multiplier.to_string(),
        new_value: format!("{:.2}", proposed),
        reason: format!(
            "{}/{} losing days, total PnL {} — tightening stop",
            stats.losing_days, stats.days, stats.total_pnl
        ),
    })
}

/// Rule 3: If total_pnl > 0, winning ratio >= 60%, AND setup_score_min is above guardrail min,
/// relax it by one step to capture more opportunities.
fn evaluate_rule3(
    stats: &TunerStats,
    signal_cfg: &crate::config::SignalConfig,
    guards: &HashMap<&str, ParamBounds>,
) -> Option<ParamAdjustment> {
    if stats.total_pnl <= Decimal::ZERO || stats.days < MIN_DAYS_FOR_TUNING {
        return None;
    }
    let winning_days = stats.days - stats.losing_days;
    let win_ratio = winning_days as f64 / stats.days as f64;
    let min_bound = guards
        .get("setup_score_min")
        .map(|b| b.min as u32)
        .unwrap_or(40);
    if win_ratio < 0.6 || signal_cfg.setup_score_min <= min_bound {
        return None;
    }

    let bounds = guards.get("setup_score_min")?;
    let current = signal_cfg.setup_score_min as f64;
    let proposed = (current - bounds.step).max(bounds.min);
    if (proposed - current).abs() < f64::EPSILON {
        return None;
    }

    Some(ParamAdjustment {
        parameter: "setup_score_min".into(),
        old_value: format!("{}", signal_cfg.setup_score_min),
        new_value: format!("{}", proposed as u32),
        reason: format!(
            "{:.0}% winning days with positive PnL {} — relaxing threshold to capture more",
            win_ratio * 100.0,
            stats.total_pnl
        ),
    })
}

async fn ensure_param_changes_table(db: &SqlitePool) {
    let _ = sqlx::query(
        "CREATE TABLE IF NOT EXISTS param_changes (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            market          TEXT NOT NULL,
            mode            TEXT NOT NULL,
            parameter       TEXT NOT NULL,
            old_value       TEXT NOT NULL,
            new_value       TEXT NOT NULL,
            reason          TEXT NOT NULL,
            source          TEXT NOT NULL,
            status          TEXT NOT NULL,
            session_pnl_before TEXT,
            created_at      TEXT NOT NULL
        )",
    )
    .execute(db)
    .await;
}

/// Check if a parameter has NOT been changed within the cooldown window.
/// Returns true if it is safe to change (no recent change).
async fn check_cooldown(db: &SqlitePool, parameter: &str) -> bool {
    let cutoff = (chrono::Utc::now() - chrono::Duration::days(COOLDOWN_DAYS)).to_rfc3339();

    let recent: Option<i64> = sqlx::query_scalar(
        "SELECT COUNT(*) FROM param_changes WHERE parameter = ? AND status IN ('applied', 'proposed') AND created_at > ?",
    )
    .bind(parameter)
    .bind(&cutoff)
    .fetch_optional(db)
    .await
    .ok()
    .flatten();

    recent.unwrap_or(0) == 0
}

/// Apply a guardrail check: ensure the value is within allowed bounds.
pub fn validate_adjustment(param: &str, value: f64) -> bool {
    let guards = guardrails();
    match guards.get(param) {
        Some(bounds) => value >= bounds.min && value <= bounds.max,
        None => true,
    }
}

/// Persist a param change record to the local SQLite DB.
/// `status`: "proposed" | "applied" | "rejected"
pub async fn record_param_change(
    db: &SqlitePool,
    market: &str,
    mode: &str,
    adj: &ParamAdjustment,
    source: &str,
    status: &str,
    session_pnl: Option<Decimal>,
) {
    if let Err(e) = sqlx::query(
        "INSERT INTO param_changes (market, mode, parameter, old_value, new_value, reason, source, status, session_pnl_before, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    .bind(market)
    .bind(mode)
    .bind(&adj.parameter)
    .bind(&adj.old_value)
    .bind(&adj.new_value)
    .bind(&adj.reason)
    .bind(source)
    .bind(status)
    .bind(session_pnl.map(|p| p.to_string()))
    .bind(chrono::Utc::now().to_rfc3339())
    .execute(db)
    .await {
        tracing::warn!("Failed to record param change: {}", e);
    }
}

async fn collect_tuner_stats(db: &SqlitePool, lookback_days: u32) -> Option<TunerStats> {
    let rows = sqlx::query(
        r#"
        SELECT date, pnl, trade_count
        FROM session_stats
        ORDER BY date DESC
        LIMIT ?
        "#,
    )
    .bind(lookback_days)
    .fetch_all(db)
    .await
    .ok()?;

    if rows.is_empty() {
        return None;
    }

    use sqlx::Row;

    let mut total_pnl = Decimal::ZERO;
    let mut losing_days = 0u32;
    let mut total_trades = 0u32;

    for row in &rows {
        let pnl: Decimal = row
            .try_get::<String, _>("pnl")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(Decimal::ZERO);
        let trades: i64 = row.try_get("trade_count").unwrap_or(0);
        total_pnl += pnl;
        total_trades += trades as u32;
        if pnl < Decimal::ZERO {
            losing_days += 1;
        }
    }

    let min_date = rows
        .last()
        .and_then(|r| r.try_get::<String, _>("date").ok())
        .unwrap_or_default();

    let bucket_rows = sqlx::query(
        r#"
        SELECT
            CASE
                WHEN setup_score BETWEEN 60 AND 69 THEN '60-69'
                WHEN setup_score BETWEEN 70 AND 79 THEN '70-79'
                WHEN setup_score >= 80 THEN '80+'
                ELSE 'other'
            END as bucket,
            COUNT(*) as cnt
        FROM signal_log
        WHERE action = 'enter'
          AND DATE(logged_at) >= ?
        GROUP BY bucket
        "#,
    )
    .bind(&min_date)
    .fetch_all(db)
    .await
    .unwrap_or_default();

    let score_buckets: Vec<(String, u32, Decimal)> = bucket_rows
        .iter()
        .map(|r| {
            let bucket: String = r.try_get("bucket").unwrap_or_default();
            let cnt: i64 = r.try_get("cnt").unwrap_or(0);
            (bucket, cnt as u32, Decimal::ZERO)
        })
        .collect();

    Some(TunerStats {
        days: rows.len() as u32,
        total_pnl,
        losing_days,
        total_trades,
        score_buckets,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guardrail_validates_in_range() {
        assert!(validate_adjustment("setup_score_min", 65.0));
        assert!(validate_adjustment("setup_score_min", 40.0));
        assert!(validate_adjustment("setup_score_min", 85.0));
    }

    #[test]
    fn guardrail_rejects_out_of_range() {
        assert!(!validate_adjustment("setup_score_min", 90.0));
        assert!(!validate_adjustment("setup_score_min", 35.0));
        assert!(!validate_adjustment("stop_atr_multiplier", 0.5));
        assert!(!validate_adjustment("stop_atr_multiplier", 5.0));
    }

    #[test]
    fn guardrail_allows_unknown_param() {
        assert!(validate_adjustment("unknown_param", 999.0));
    }
}
