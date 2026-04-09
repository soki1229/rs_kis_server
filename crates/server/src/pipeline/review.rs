use crate::monitoring::alert::AlertRouter;
use reqwest::Client;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use sqlx::{Row, SqlitePool};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[derive(Debug, Clone, Default, Serialize)]
pub struct DailyTradeStats {
    pub date: String,
    pub total_trades: u32,
    pub wins: u32,
    pub losses: u32,
    pub win_rate: f64,
    pub total_pnl: Decimal,
    pub avg_win_pnl: Decimal,
    pub avg_loss_pnl: Decimal,
    pub max_win: Decimal,
    pub max_loss: Decimal,
    pub profit_factor: f64,
    /// True if individual trade PnL is not available (aggregate only)
    pub aggregate_only: bool,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct SignalStats {
    pub total_signals: u32,
    pub avg_setup_score: f64,
    pub score_60_69_count: u32,
    pub score_60_69_win_rate: f64,
    pub score_70_79_count: u32,
    pub score_70_79_win_rate: f64,
    pub score_80_plus_count: u32,
    pub score_80_plus_win_rate: f64,
    pub llm_enter_count: u32,
    pub llm_enter_win_rate: f64,
    pub llm_block_count: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct DailyReviewReport {
    pub market: String,
    pub date: String,
    pub trade_stats: DailyTradeStats,
    pub signal_stats: SignalStats,
    pub llm_analysis: Option<String>,
    pub suggestions: Vec<String>,
}

pub async fn collect_daily_trade_stats(
    db_pool: &SqlitePool,
    date: &str,
) -> Result<DailyTradeStats, sqlx::Error> {
    let mut stats = DailyTradeStats {
        date: date.to_string(),
        ..Default::default()
    };

    let rows = sqlx::query(
        r#"
        SELECT 
            o.symbol,
            o.side,
            CAST(o.qty AS REAL) as qty,
            CAST(o.price AS REAL) as entry_price,
            CAST(p.entry_price AS REAL) as pos_entry,
            ss.pnl
        FROM orders o
        LEFT JOIN positions p ON o.id = p.order_id
        LEFT JOIN session_stats ss ON DATE(o.submitted_at) = ss.date
        WHERE DATE(o.submitted_at) = ?
          AND o.state = 'FullyFilled'
        ORDER BY o.submitted_at
        "#,
    )
    .bind(date)
    .fetch_all(db_pool)
    .await?;

    if rows.is_empty() {
        return Ok(stats);
    }

    let session_pnl: Option<String> =
        sqlx::query_scalar("SELECT pnl FROM session_stats WHERE date = ?")
            .bind(date)
            .fetch_optional(db_pool)
            .await?;

    stats.total_pnl = session_pnl
        .and_then(|s| s.parse().ok())
        .unwrap_or(Decimal::ZERO);

    let trade_results = sqlx::query(
        r#"
        SELECT 
            sl.symbol,
            sl.action,
            sl.setup_score,
            sl.llm_verdict,
            COALESCE(
                (SELECT CAST(p.entry_price AS REAL) FROM positions p WHERE p.symbol = sl.symbol LIMIT 1),
                0
            ) as entry_price
        FROM signal_log sl
        WHERE DATE(sl.logged_at) = ?
          AND sl.action = 'enter'
        "#,
    )
    .bind(date)
    .fetch_all(db_pool)
    .await?;

    // Individual trade PnL tracking not supported in current schema.
    // We only have aggregate PnL from session_stats.
    stats.total_trades = trade_results.len() as u32;
    stats.aggregate_only = true;

    // Without per-trade PnL, we cannot determine individual wins/losses.
    // Set to 0 and win_rate to 0 to indicate "unknown" rather than falsely reporting.
    stats.wins = 0;
    stats.losses = 0;
    stats.win_rate = 0.0;

    let mut win_pnls: Vec<Decimal> = Vec::new();
    let mut loss_pnls: Vec<Decimal> = Vec::new();

    // Categorize aggregate PnL for profit factor calculation
    if stats.total_pnl > Decimal::ZERO && stats.total_trades > 0 {
        win_pnls.push(stats.total_pnl);
    } else if stats.total_pnl < Decimal::ZERO && stats.total_trades > 0 {
        loss_pnls.push(stats.total_pnl);
    }

    if !win_pnls.is_empty() {
        let sum: Decimal = win_pnls.iter().sum();
        stats.avg_win_pnl = sum / Decimal::from(win_pnls.len() as u32);
        stats.max_win = win_pnls.iter().max().cloned().unwrap_or(Decimal::ZERO);
    }

    if !loss_pnls.is_empty() {
        let sum: Decimal = loss_pnls.iter().sum();
        stats.avg_loss_pnl = sum / Decimal::from(loss_pnls.len() as u32);
        stats.max_loss = loss_pnls.iter().min().cloned().unwrap_or(Decimal::ZERO);
    }

    let total_wins: Decimal = win_pnls.iter().sum();
    let total_losses: Decimal = loss_pnls.iter().map(|x| x.abs()).sum();
    stats.profit_factor = if total_losses > Decimal::ZERO {
        (total_wins / total_losses)
            .to_string()
            .parse()
            .unwrap_or(0.0)
    } else if total_wins > Decimal::ZERO {
        f64::INFINITY
    } else {
        0.0
    };

    Ok(stats)
}

pub async fn collect_signal_stats(
    db_pool: &SqlitePool,
    date: &str,
) -> Result<SignalStats, sqlx::Error> {
    let mut stats = SignalStats::default();

    let rows = sqlx::query(
        r#"
        SELECT 
            setup_score,
            llm_verdict,
            action
        FROM signal_log sl
        WHERE DATE(sl.logged_at) = ?
        "#,
    )
    .bind(date)
    .fetch_all(db_pool)
    .await?;

    if rows.is_empty() {
        return Ok(stats);
    }

    let mut score_sum = 0i64;
    let mut score_60_69 = (0u32, 0u32);
    let mut score_70_79 = (0u32, 0u32);
    let mut score_80_plus = (0u32, 0u32);
    let mut llm_enter = (0u32, 0u32);
    let mut llm_block = 0u32;

    for row in &rows {
        let score: i32 = row.try_get("setup_score").unwrap_or(0);
        let verdict: Option<String> = row.try_get("llm_verdict").ok().flatten();
        let action: String = row.try_get("action").unwrap_or_default();

        stats.total_signals += 1;
        score_sum += score as i64;

        if action == "enter" {
            match score {
                60..=69 => {
                    score_60_69.0 += 1;
                }
                70..=79 => {
                    score_70_79.0 += 1;
                }
                80..=100 => {
                    score_80_plus.0 += 1;
                }
                _ => {}
            }

            if let Some(v) = &verdict {
                if v.to_lowercase() == "enter" {
                    llm_enter.0 += 1;
                }
            }
        }

        if let Some(v) = &verdict {
            if v.to_lowercase() == "block" {
                llm_block += 1;
            }
        }
    }

    stats.avg_setup_score = if stats.total_signals > 0 {
        score_sum as f64 / stats.total_signals as f64
    } else {
        0.0
    };

    stats.score_60_69_count = score_60_69.0;
    // Win rate tracking requires per-trade PnL (not available in current schema)
    // These remain 0.0 until trade_results table is implemented
    stats.score_60_69_win_rate = 0.0;

    stats.score_70_79_count = score_70_79.0;
    stats.score_70_79_win_rate = 0.0;

    stats.score_80_plus_count = score_80_plus.0;
    stats.score_80_plus_win_rate = 0.0;

    stats.llm_enter_count = llm_enter.0;
    stats.llm_enter_win_rate = 0.0;
    stats.llm_block_count = llm_block;

    Ok(stats)
}

pub async fn generate_llm_analysis(
    api_key: &str,
    model: &str,
    report: &DailyReviewReport,
) -> Result<(String, Vec<String>), anyhow::Error> {
    let client = Client::new();

    let ts = &report.trade_stats;
    let ss = &report.signal_stats;

    // Build win/loss description based on data availability
    let win_loss_info = if ts.aggregate_only {
        format!(
            "- 총 거래: {}건 (개별 거래 PnL 미추적으로 승/패 구분 불가)",
            ts.total_trades
        )
    } else {
        format!(
            "- 총 거래: {}건 (승: {}, 패: {})\n- 승률: {:.1}%",
            ts.total_trades,
            ts.wins,
            ts.losses,
            ts.win_rate * 100.0
        )
    };

    let prompt = format!(
        r#"당신은 전문 트레이딩 코치입니다. 오늘의 자동매매 봇 거래 결과를 분석하고 피드백을 제공하세요.

## 오늘의 거래 데이터 ({market} - {date})

### 거래 통계
{win_loss_info}
- 총 PnL: {total_pnl}
- Profit Factor: {pf:.2}

### 신호 분석
- 총 신호: {total_signals}건
- 평균 Setup Score: {avg_score:.1}
- Score 60-69: {s60}건
- Score 70-79: {s70}건
- Score 80+: {s80}건
- LLM ENTER: {llm_enter}건
- LLM BLOCK: {llm_block}건

**참고**: 현재 DB 스키마에서는 개별 거래 PnL을 추적하지 않으므로, 승률 및 신호별 승률 데이터는 제공되지 않습니다. 일일 총 PnL과 신호 건수를 기반으로 분석해주세요.

## 요청사항
다음 형식으로 JSON 응답하세요:
{{
  "analysis": "오늘 거래에 대한 2-3문장 요약 분석 (총 PnL 부호와 거래 건수 기반)",
  "good_points": ["잘한 점 1", "잘한 점 2"],
  "improve_points": ["개선할 점 1", "개선할 점 2"],
  "suggestions": [
    "구체적 제안 1 (예: setup_score_min을 65로 상향 권장)",
    "구체적 제안 2"
  ]
}}

거래가 없으면 "거래 없음" 으로 간단히 응답하세요."#,
        market = report.market,
        date = report.date,
        win_loss_info = win_loss_info,
        total_pnl = ts.total_pnl,
        pf = ts.profit_factor,
        total_signals = ss.total_signals,
        avg_score = ss.avg_setup_score,
        s60 = ss.score_60_69_count,
        s70 = ss.score_70_79_count,
        s80 = ss.score_80_plus_count,
        llm_enter = ss.llm_enter_count,
        llm_block = ss.llm_block_count,
    );

    let body = serde_json::json!({
        "model": model,
        "max_tokens": 500,
        "messages": [{"role": "user", "content": prompt}]
    });

    let resp = client
        .post("https://api.anthropic.com/v1/messages")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .header("content-type", "application/json")
        .timeout(Duration::from_secs(30))
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("LLM API error {}: {}", status, text));
    }

    let raw: serde_json::Value = resp.json().await?;
    let content = raw["content"][0]["text"]
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("no content in LLM response"))?;

    #[derive(Deserialize)]
    struct LlmReviewResponse {
        analysis: String,
        #[serde(default)]
        suggestions: Vec<String>,
    }

    match serde_json::from_str::<LlmReviewResponse>(content) {
        Ok(parsed) => Ok((parsed.analysis, parsed.suggestions)),
        Err(_) => Ok((content.to_string(), vec![])),
    }
}

pub fn format_telegram_report(report: &DailyReviewReport, mode: &str) -> String {
    let ts = &report.trade_stats;
    let ss = &report.signal_stats;

    let pnl_emoji = if ts.total_pnl > Decimal::ZERO {
        "📈"
    } else if ts.total_pnl < Decimal::ZERO {
        "📉"
    } else {
        "➖"
    };

    // When aggregate_only=true, per-trade win/loss is not tracked
    let win_loss_display = if ts.aggregate_only && ts.total_trades > 0 {
        format!("총 거래: {} (개별 PnL 미지원)", ts.total_trades)
    } else {
        format!(
            "총 거래: {} (✅{} / ❌{})",
            ts.total_trades, ts.wins, ts.losses
        )
    };

    let win_rate_display = if ts.aggregate_only && ts.total_trades > 0 {
        "승률: N/A (개별 거래 PnL 필요)".to_string()
    } else {
        format!("승률: {:.1}%", ts.win_rate * 100.0)
    };

    // Signal stats win rates are also unavailable without per-trade PnL tracking
    let signal_win_rate_note = if ts.aggregate_only && ts.total_trades > 0 {
        "\n⚠️ 신호별 승률 미지원 (개별 거래 PnL 추적 필요)"
    } else {
        ""
    };

    let mut msg = format!(
        r#"📊 *일일 리포트* [{market}|{mode}] ⚠️ 참고용
📅 {date}

{pnl_emoji} *거래 요약*
• {win_loss}
• {win_rate}
• PnL: {pnl}
• Profit Factor: {pf:.2}

📡 *신호 분석*
• 평균 Score: {avg_score:.0}
• 80+ 건수: {s80}건
• 70-79 건수: {s70}건
• 60-69 건수: {s60}건
• LLM ENTER: {llm}건 / BLOCK: {llm_block}건{signal_note}"#,
        market = report.market,
        mode = mode,
        date = report.date,
        pnl_emoji = pnl_emoji,
        win_loss = win_loss_display,
        win_rate = win_rate_display,
        pnl = ts.total_pnl,
        pf = ts.profit_factor,
        avg_score = ss.avg_setup_score,
        s80 = ss.score_80_plus_count,
        s70 = ss.score_70_79_count,
        s60 = ss.score_60_69_count,
        llm = ss.llm_enter_count,
        llm_block = ss.llm_block_count,
        signal_note = signal_win_rate_note,
    );

    if let Some(analysis) = &report.llm_analysis {
        msg.push_str(&format!(
            "\n\n🤖 *AI 분석* (의사결정 보조 참고치)\n{}",
            analysis
        ));
    }

    if !report.suggestions.is_empty() {
        msg.push_str("\n\n💡 *개선 제안* (dry_run 검증 후 적용 권장)");
        for (i, s) in report.suggestions.iter().enumerate() {
            msg.push_str(&format!("\n{}. {}", i + 1, s));
        }
    }

    msg.push_str("\n\n─────────────────────────\n📌 본 리포트는 참고용이며, 실거래 의사결정 전 dry_run 검증을 권장합니다.");

    msg
}

pub async fn run_daily_review(
    market: &str,
    db_pool: &SqlitePool,
    _alert: &AlertRouter,
    summary_alert: &AlertRouter,
    api_key: Option<&str>,
    model: &str,
    dry_run: bool,
) -> Result<DailyReviewReport, anyhow::Error> {
    let date = chrono::Local::now().format("%Y-%m-%d").to_string();

    tracing::info!("DailyReview[{}]: collecting stats for {}", market, date);

    let trade_stats = collect_daily_trade_stats(db_pool, &date).await?;
    let signal_stats = collect_signal_stats(db_pool, &date).await?;

    let mut report = DailyReviewReport {
        market: market.to_string(),
        date: date.clone(),
        trade_stats,
        signal_stats,
        llm_analysis: None,
        suggestions: vec![],
    };

    if let Some(key) = api_key {
        if !key.is_empty() && report.trade_stats.total_trades > 0 {
            match generate_llm_analysis(key, model, &report).await {
                Ok((analysis, suggestions)) => {
                    report.llm_analysis = Some(analysis);
                    report.suggestions = suggestions;
                }
                Err(e) => {
                    tracing::warn!("DailyReview[{}]: LLM analysis failed: {}", market, e);
                }
            }
        }
    }

    let mode = if dry_run { "VTS" } else { "LIVE" };
    let telegram_msg = format_telegram_report(&report, mode);
    summary_alert.info(telegram_msg);

    tracing::info!(
        "DailyReview[{}]: report generated — {} trades, {:.1}% win rate",
        market,
        report.trade_stats.total_trades,
        report.trade_stats.win_rate * 100.0
    );

    Ok(report)
}

/// Post-review pipeline: Notion upload + tuner parameter adjustment.
/// Called after run_daily_review completes.
#[allow(clippy::too_many_arguments)]
pub async fn run_post_review(
    market: &str,
    report: &DailyReviewReport,
    db_pool: &SqlitePool,
    _alert: &AlertRouter,
    summary_alert: &AlertRouter,
    notion: Option<&std::sync::Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    tunable_tx: Option<&std::sync::Arc<tokio::sync::watch::Sender<crate::config::TunableConfig>>>,
    signal_cfg: &crate::config::SignalConfig,
    position_cfg: &crate::config::PositionConfig,
    dry_run: bool,
) {
    let mode = if dry_run { "VTS" } else { "LIVE" };
    // 1. Upload daily report to Notion
    if let Some(nc) = notion {
        let ts = &report.trade_stats;
        let ss = &report.signal_stats;
        let row = crate::notion::DailyReportRow {
            date: format!("{} [{}]", report.date, market),
            market: market.to_string(),
            mode: mode.to_string(),
            total_trades: ts.total_trades as f64,
            pnl: ts.total_pnl.to_string().parse().unwrap_or(0.0),
            profit_factor: ts.profit_factor,
            avg_score: ss.avg_setup_score,
            llm_enter: ss.llm_enter_count as f64,
            llm_block: ss.llm_block_count as f64,
            score_80_plus: ss.score_80_plus_count as f64,
            score_70_79: ss.score_70_79_count as f64,
            score_60_69: ss.score_60_69_count as f64,
            ai_analysis: report.llm_analysis.clone().unwrap_or_default(),
            suggestions: report.suggestions.join("; "),
        };
        let client = nc.read().await;
        if let Err(e) = client.add_daily_report(&row).await {
            tracing::warn!("Notion daily report upload failed: {}", e);
        }
    }

    // 2. Run tuner (stats-based parameter adjustment — no LLM required)
    let adjustments = super::tuner::analyze_and_propose(db_pool, 5, signal_cfg, position_cfg).await;

    if adjustments.is_empty() {
        tracing::info!("Tuner[{}]: no adjustments proposed", market);
        return;
    }

    // LIVE: proposal-only (record but do not auto-apply; human reviews via Notion/Telegram).
    // VTS:  auto-apply validated adjustments for rapid experimentation.
    let auto_apply = dry_run;

    let action_label = if auto_apply {
        "자동 적용"
    } else {
        "조정 제안"
    };
    let mut tg_msg = format!("🔧 *파라미터 {}* [{}|{}]\n", action_label, market, mode);

    for adj in &adjustments {
        let valid =
            super::tuner::validate_adjustment(&adj.parameter, adj.new_value.parse().unwrap_or(0.0));

        let status = if !valid {
            "rejected"
        } else if auto_apply {
            "applied"
        } else {
            "proposed"
        };

        // Record to SQLite (with mode + status)
        super::tuner::record_param_change(db_pool, market, mode, adj, "stats_rule", status, None)
            .await;

        // Upload to Notion
        if let Some(nc) = notion {
            let row = crate::notion::ParamChangeRow {
                timestamp: chrono::Utc::now().to_rfc3339(),
                market: market.to_string(),
                mode: mode.to_string(),
                parameter: adj.parameter.clone(),
                old_value: adj.old_value.clone(),
                new_value: adj.new_value.clone(),
                reason: adj.reason.clone(),
                source: "stats_rule".to_string(),
                applied: status == "applied",
            };
            let client = nc.read().await;
            if let Err(e) = client.add_param_change(&row).await {
                tracing::warn!("Notion param change upload failed: {}", e);
            }
        }

        if !valid {
            tg_msg.push_str(&format!(
                "• `{}`: {} → {} ❌ (guardrail 범위 초과)\n",
                adj.parameter, adj.old_value, adj.new_value
            ));
            tracing::warn!(
                "Tuner[{}]: rejected {} = {} (guardrail violation)",
                market,
                adj.parameter,
                adj.new_value
            );
        } else if auto_apply {
            // VTS: apply to runtime config via watch channel
            if let Some(tx) = tunable_tx {
                let mut current = (**tx).borrow().clone();
                if current.apply(&adj.parameter, &adj.new_value) {
                    let _ = tx.send(current);
                    tg_msg.push_str(&format!(
                        "• `{}`: {} → {} ✅ (적용됨)\n  _{}_\n",
                        adj.parameter, adj.old_value, adj.new_value, adj.reason
                    ));
                    tracing::info!(
                        "Tuner[{}]: applied {} = {} (was {})",
                        market,
                        adj.parameter,
                        adj.new_value,
                        adj.old_value
                    );
                }
            }
        } else {
            // LIVE: proposal-only — notify but do not apply
            tg_msg.push_str(&format!(
                "• `{}`: {} → {} 📋 (제안)\n  _{}_\n",
                adj.parameter, adj.old_value, adj.new_value, adj.reason
            ));
            tracing::info!(
                "Tuner[{}]: proposed {} = {} (current {}) — awaiting manual approval",
                market,
                adj.parameter,
                adj.new_value,
                adj.old_value
            );
        }
    }

    summary_alert.info(tg_msg);
}

/// Standalone daily review task (for future use as independent scheduled task).
/// Currently, daily review is triggered from handle_eod in position.rs.
#[allow(dead_code)]
pub async fn run_daily_review_task(
    market: String,
    _db_pool: SqlitePool,
    _alert: AlertRouter,
    _api_key: Option<String>,
    _model: String,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("DailyReviewTask[{}]: shutting down", market);
                return;
            }
            _ = tokio::time::sleep(Duration::from_secs(60)) => {
                // Placeholder: actual trigger is from EOD
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn format_telegram_report_includes_all_sections() {
        let report = DailyReviewReport {
            market: "KR".to_string(),
            date: "2026-04-01".to_string(),
            trade_stats: DailyTradeStats {
                date: "2026-04-01".to_string(),
                total_trades: 5,
                wins: 0,
                losses: 0,
                win_rate: 0.0,
                total_pnl: Decimal::from(45000),
                avg_win_pnl: Decimal::from(25000),
                avg_loss_pnl: Decimal::from(-15000),
                max_win: Decimal::from(40000),
                max_loss: Decimal::from(-20000),
                profit_factor: 1.67,
                aggregate_only: true,
            },
            signal_stats: SignalStats {
                total_signals: 8,
                avg_setup_score: 72.5,
                score_60_69_count: 2,
                score_60_69_win_rate: 0.0,
                score_70_79_count: 3,
                score_70_79_win_rate: 0.0,
                score_80_plus_count: 3,
                score_80_plus_win_rate: 0.0,
                llm_enter_count: 4,
                llm_enter_win_rate: 0.0,
                llm_block_count: 1,
            },
            llm_analysis: Some("오늘 거래는 양호했습니다.".to_string()),
            suggestions: vec!["setup_score_min을 65로 상향 검토".to_string()],
        };

        let msg = format_telegram_report(&report, "VTS");

        assert!(msg.contains("[KR|VTS]"));
        assert!(msg.contains("2026-04-01"));
        assert!(msg.contains("개별 PnL 미지원")); // aggregate_only message
        assert!(msg.contains("45000"));
        assert!(msg.contains("AI 분석"));
        assert!(msg.contains("오늘 거래는 양호했습니다"));
        assert!(msg.contains("개선 제안"));
        assert!(msg.contains("setup_score_min"));
    }

    #[test]
    fn format_telegram_report_handles_no_trades() {
        let report = DailyReviewReport {
            market: "US".to_string(),
            date: "2026-04-01".to_string(),
            trade_stats: DailyTradeStats::default(),
            signal_stats: SignalStats::default(),
            llm_analysis: None,
            suggestions: vec![],
        };

        let msg = format_telegram_report(&report, "LIVE");
        assert!(msg.contains("[US|LIVE]"));
        assert!(msg.contains("총 거래: 0"));
    }

    #[tokio::test]
    async fn collect_stats_empty_db() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let stats = collect_daily_trade_stats(&pool, "2026-04-01")
            .await
            .unwrap();
        assert_eq!(stats.total_trades, 0);

        let signal_stats = collect_signal_stats(&pool, "2026-04-01").await.unwrap();
        assert_eq!(signal_stats.total_signals, 0);
    }
}
