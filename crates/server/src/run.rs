use crate::config::{Secrets, ServerConfig};
use crate::market::MarketAdapter;
use crate::pipeline;
use crate::shared;
use crate::state;
use crate::strategy::StrategyBundle;
use crate::types::BotCommand;

use chrono_tz::{America, Asia};
use kis_api::{KisApi, KisClient, KisConfig, KisDomesticClient};
use std::sync::{atomic::AtomicU32, Arc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Framework entry point. Call this from your binary after assembling a StrategyBundle.
pub async fn run(cfg: ServerConfig, strategies: StrategyBundle) -> anyhow::Result<()> {
    let regime_strategy: Arc<dyn crate::strategy::RegimeStrategy> = Arc::from(strategies.regime);
    let discovery_strategy: Arc<dyn crate::strategy::DiscoveryStrategy> =
        Arc::from(strategies.discovery);
    let signal_strategy: Arc<dyn crate::strategy::SignalStrategy> = Arc::from(strategies.signal);
    let qual_strategy: Arc<dyn crate::strategy::QualificationStrategy> =
        Arc::from(strategies.qualification);
    let risk_strategy: Arc<dyn crate::strategy::RiskStrategy> = Arc::from(strategies.risk);

    let secrets = Secrets::from_env(&cfg.integrations)?;

    let kr_effective_dry_run = cfg.kr.dry_run.unwrap_or(cfg.dry_run);
    let us_effective_dry_run = cfg.us.dry_run.unwrap_or(cfg.dry_run);
    
    let llm_available = std::env::var("ANTHROPIC_API_KEY")
        .map(|k| !k.is_empty())
        .unwrap_or(false);
    if !cfg.integrations.llm.enabled {
        tracing::info!("LLM evaluation disabled via config");
    } else if !llm_available {
        tracing::error!(
            "ANTHROPIC_API_KEY not set — LLM evaluation will be skipped in signal tasks"
        );
    }

    let token = CancellationToken::new();
    let activity = shared::activity::ActivityLog::new();

    let notion_client = {
        let mut nc = crate::notion::NotionClient::new(
            secrets.notion_token.clone(),
            secrets.notion_page_id.clone(),
        );
        match nc.bootstrap().await {
            Ok(ids) => {
                tracing::info!("Notion workspace bootstrapped: {:?}", ids);
                Some(Arc::new(tokio::sync::RwLock::new(nc)))
            }
            Err(e) => {
                tracing::warn!("Notion bootstrap failed (non-fatal): {}", e);
                None
            }
        }
    };

    let real_config = KisConfig::from_env()?;
    let vts_config = KisConfig::from_env_vts().unwrap_or_else(|_| {
        tracing::warn!("VTS configuration not found in environment, using Real config as fallback for VTS adapters");
        real_config.clone()
    });

    let kr_real_client = Arc::new(KisDomesticClient::new(real_config.clone()));
    let us_real_client = Arc::new(KisClient::new(real_config));
    let kr_vts_client = Arc::new(KisDomesticClient::new(vts_config.clone()));
    let us_vts_client = Arc::new(KisClient::new(vts_config));

    let adapters = crate::run_generic::MarketAdapters::new(
        kr_real_client,
        us_real_client.clone(),
        kr_vts_client,
        us_vts_client,
    );

    let kr_adapter: Arc<dyn MarketAdapter> = if kr_effective_dry_run {
        adapters.kr_vts.clone()
    } else {
        adapters.kr_real.clone()
    };

    let us_adapter: Arc<dyn MarketAdapter> = if us_effective_dry_run {
        adapters.us_vts.clone()
    } else {
        adapters.us_real.clone()
    };

    tracing::info!("connecting shared WebSocket stream...");
    let shared_stream = us_real_client
        .stream()
        .await
        .expect("failed to establish shared WebSocket stream");
    tracing::info!("shared WebSocket stream established for KR+US tick data");

    let mut kr_pipeline = pipeline::MarketPipeline::new(&cfg.kr.db_path);
    let mut us_pipeline = pipeline::MarketPipeline::new(&cfg.us.db_path);

    let kr_p_config = state::PipelineConfig::new(activity.clone());
    let us_p_config = state::PipelineConfig::new(activity.clone());

    let (kr_live_tx, kr_state) = kr_p_config.build();
    let (us_live_tx, us_state) = us_p_config.build();

    let (kr_cmd_tx, kr_cmd_rx) = tokio::sync::mpsc::channel::<BotCommand>(64);
    let (us_cmd_tx, us_cmd_rx) = tokio::sync::mpsc::channel::<BotCommand>(64);

    // ControlTask (KR)
    let t = token.clone();
    let h_kr_ctrl: JoinHandle<()> = tokio::spawn(shared::control::run_control_task(kr_cmd_rx, kr_pipeline.force_order_tx.clone(), kr_state.summary.clone(), cfg.kr.kill_switch_path.clone(), t));

    // ControlTask (US)
    let t = token.clone();
    let h_us_ctrl: JoinHandle<()> = tokio::spawn(shared::control::run_control_task(us_cmd_rx, us_pipeline.force_order_tx.clone(), us_state.summary.clone(), cfg.us.kill_switch_path.clone(), t));

    let kr_alert = kr_pipeline.alert.clone();
    let us_alert = us_pipeline.alert.clone();
    let kr_summary_alert = kr_pipeline.summary_alert.clone();
    let us_summary_alert = us_pipeline.summary_alert.clone();

    // AlertTask
    let t = token.clone();
    let h_alert: JoinHandle<()> = tokio::spawn(shared::telegram::run_alert_task_with_rx(
        secrets.alert_bot_token.clone(), secrets.alert_chat_id, kr_alert.subscribe(), us_alert.subscribe(), t,
    ));

    // MonitorTask
    let t = token.clone();
    let h_monitor: JoinHandle<()> = tokio::spawn(shared::telegram::run_monitor_task(
        secrets.monitor_bot_token.clone(), secrets.monitor_chat_id, kr_summary_alert.subscribe(), us_summary_alert.subscribe(),
        kr_cmd_tx.clone(), us_cmd_tx.clone(), kr_state.clone(), us_state.clone(), activity.clone(), t,
    ));

    let mode = match (kr_effective_dry_run, us_effective_dry_run) {
        (true, true) => "모의투자(VTS)",
        (true, false) => "국내모의/미국실전",
        (false, true) => "국내실전/미국모의",
        (false, false) => "실전투자(LIVE)",
    };
    kr_summary_alert.info(format!("🚀 KIS Automated Trading Bot 가동 시작 [{}]", mode));

    // RestApiTask
    let t = token.clone();
    let kr_state_rest = kr_state.clone();
    let us_state_rest = us_state.clone();
    let h_rest: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = shared::rest::run_rest_task(cfg.rest_port, kr_state_rest, us_state_rest, kr_cmd_tx, us_cmd_tx, secrets.rest_admin_token.clone(), t).await {
            tracing::error!("RestApiTask error: {}", e);
        }
    });

    // ── KR pipeline ──────────────────────────────────────────────────────
    let kr_db_pool = crate::db::connect(&cfg.kr.db_path).await?;
    let kr_active = kr_adapter.market_timing().is_open;
    
    // Recovery (KR)
    let kr_recovery_failed = if kr_active {
        let kr_broker_order_nos: Option<Vec<String>> = if !kr_effective_dry_run {
            match kr_adapter.unfilled_orders().await {
                Ok(unfilled) => Some(unfilled.into_iter().map(|o| o.order_no).collect()),
                Err(e) => { tracing::warn!("KR unfilled_orders fetch failed: {e} — skipping orphan check"); None }
            }
        } else { None };
        run_market_recovery("KR", &kr_db_pool, kr_broker_order_nos).await
    } else { false };

    if kr_recovery_failed {
        kr_state.summary.write().unwrap().bot_state = state::BotState::HardBlocked;
        kr_summary_alert.critical("🚨 복구 실패 — HardBlocked 상태. 수동 점검 필요!".to_string());
    } else {
        kr_state.summary.write().unwrap().bot_state = state::BotState::Active;
        if kr_active { kr_summary_alert.info("✅ [국내] 시스템 복구 및 무결성 검사 완료".to_string()); }
    }

    let (kr_regime_tx, kr_regime_rx) = crate::regime::regime_channel(crate::types::MarketRegime::Trending);
    let t = token.clone();
    let h_kr_regime: JoinHandle<()> = tokio::spawn(pipeline::run_generic_regime_task(kr_adapter.clone(), regime_strategy.clone(), kr_regime_tx, kr_alert.clone(), t, "069500"));

    if kr_active {
        let kr_wl = pipeline::scheduler::build_kr_watchlist(kr_adapter.as_ref(), &cfg.kr, &kr_alert, &kr_db_pool).await;
        if !kr_wl.all_unique().is_empty() {
            let _ = pipeline::signal::seed_symbols(&kr_wl.all_unique(), kr_adapter.as_ref(), &kr_db_pool, false).await;
            kr_pipeline.watchlist_tx.send(kr_wl).ok();
        }
    }

    let t = token.clone();
    let h_kr_tick: JoinHandle<()> = tokio::spawn(pipeline::tick::run_kr_tick_task(shared_stream.clone(), kr_pipeline.watchlist_rx.clone(), kr_pipeline.tick_tx.clone(), kr_pipeline.tick_pos_tx.clone(), kr_pipeline.quote_tx.clone(), kr_alert.clone(), activity.clone(), kr_db_pool.clone(), t));

    let kr_pending = Arc::new(AtomicU32::new(0));
    let kr_poll_sem = Arc::new(tokio::sync::Semaphore::new(3));

    let t = token.clone();
    let h_kr_signal: JoinHandle<()> = tokio::spawn(pipeline::signal::run_signal_task(
        kr_pipeline.tick_tx.subscribe(), kr_pipeline.quote_rx, kr_pipeline.order_tx.clone(), kr_regime_rx.clone(), kr_db_pool.clone(), kr_adapter.clone(), kr_pipeline.watchlist_rx.clone(),
        kr_state.summary.clone(), cfg.signal.clone(), cfg.kr.strategies.clone(),
        activity.clone(), notion_client.clone(), kr_state.live_state_rx.clone(), signal_strategy.clone(), qual_strategy.clone(), risk_strategy.clone(), t
    ));

    let t = token.clone();
    let h_kr_exec: JoinHandle<()> = tokio::spawn(pipeline::run_generic_execution_task(kr_adapter.clone(), kr_pipeline.order_rx, kr_pipeline.force_order_rx, kr_pipeline.fill_tx.clone(), kr_db_pool.clone(), kr_state.summary.clone(), kr_alert.clone(), t, kr_pending, kr_poll_sem));

    let t = token.clone();
    let h_kr_pos: JoinHandle<()> = tokio::spawn(pipeline::run_generic_position_task(
        kr_adapter.clone(), kr_pipeline.fill_rx, kr_pipeline.tick_pos_rx, kr_pipeline.eod_rx.take().unwrap(), kr_live_tx, kr_pipeline.force_order_tx.clone(), kr_regime_rx, kr_db_pool.clone(),
        t, pipeline::position::market_close_utc(15, 30, Asia::Seoul),
        cfg.position.clone()
    ));

    // ── US pipeline ──────────────────────────────────────────────────────
    let us_db_pool = crate::db::connect(&cfg.us.db_path).await?;
    let us_active = us_adapter.market_timing().is_open;

    // Recovery (US)
    let us_recovery_failed = if us_active {
        let us_broker_order_nos: Option<Vec<String>> = if !us_effective_dry_run {
            match us_adapter.unfilled_orders().await {
                Ok(unfilled) => Some(unfilled.into_iter().map(|o| o.order_no).collect()),
                Err(e) => { tracing::warn!("US unfilled_orders fetch failed: {e} — skipping orphan check"); None }
            }
        } else { None };
        run_market_recovery("US", &us_db_pool, us_broker_order_nos).await
    } else { false };

    if us_recovery_failed {
        us_state.summary.write().unwrap().bot_state = state::BotState::HardBlocked;
        us_summary_alert.critical("🚨 복구 실패 — HardBlocked 상태. 수동 점검 필요!".to_string());
    } else {
        us_state.summary.write().unwrap().bot_state = state::BotState::Active;
        if us_active { us_summary_alert.info("✅ [미국] 시스템 복구 및 무결성 검사 완료".to_string()); }
    }

    let (us_regime_tx, us_regime_rx) = crate::regime::regime_channel(crate::types::MarketRegime::Trending);
    let t = token.clone();
    let h_us_regime: JoinHandle<()> = tokio::spawn(pipeline::run_generic_regime_task(us_adapter.clone(), regime_strategy.clone(), us_regime_tx, us_alert.clone(), t, "QQQ"));

    if us_active {
        let us_wl = pipeline::scheduler::build_watchlist(us_adapter.as_ref(), &cfg.us, &us_alert, &us_db_pool).await;
        if !us_wl.all_unique().is_empty() {
            let _ = pipeline::signal::seed_symbols(&us_wl.all_unique(), us_adapter.as_ref(), &us_db_pool, false).await;
            us_pipeline.watchlist_tx.send(us_wl).ok();
        }
    }

    let t = token.clone();
    let h_us_tick: JoinHandle<()> = tokio::spawn(pipeline::tick::run_us_tick_task(shared_stream.clone(), us_pipeline.watchlist_rx.clone(), us_pipeline.tick_tx.clone(), us_pipeline.tick_pos_tx.clone(), us_pipeline.quote_tx.clone(), us_alert.clone(), activity.clone(), us_db_pool.clone(), t));

    let us_pending = Arc::new(AtomicU32::new(0));
    let us_poll_sem = Arc::new(tokio::sync::Semaphore::new(3));
    let t = token.clone();
    let h_us_signal: JoinHandle<()> = tokio::spawn(pipeline::signal::run_signal_task(
        us_pipeline.tick_tx.subscribe(), us_pipeline.quote_rx, us_pipeline.order_tx.clone(), us_regime_rx.clone(), us_db_pool.clone(), us_adapter.clone(), us_pipeline.watchlist_rx.clone(),
        us_state.summary.clone(), cfg.signal.clone(), cfg.us.strategies.clone(),
        activity.clone(), notion_client.clone(), us_state.live_state_rx.clone(), signal_strategy.clone(), qual_strategy.clone(), risk_strategy.clone(), t
    ));

    let t = token.clone();
    let h_us_exec: JoinHandle<()> = tokio::spawn(pipeline::run_generic_execution_task(us_adapter.clone(), us_pipeline.order_rx, us_pipeline.force_order_rx, us_pipeline.fill_tx.clone(), us_db_pool.clone(), us_state.summary.clone(), us_alert.clone(), t, us_pending, us_poll_sem));

    let t = token.clone();
    let h_us_pos: JoinHandle<()> = tokio::spawn(pipeline::run_generic_position_task(
        us_adapter.clone(), us_pipeline.fill_rx, us_pipeline.tick_pos_rx, us_pipeline.eod_rx.take().unwrap(), us_live_tx, us_pipeline.force_order_tx.clone(), us_regime_rx, us_db_pool.clone(),
        t, pipeline::position::market_close_utc(16, 0, America::New_York),
        cfg.position.clone()
    ));

    // Scheduler (KR)
    let t = token.clone();
    let h_kr_sched: JoinHandle<()> = tokio::spawn(pipeline::scheduler::run_kr_scheduler_task(kr_adapter, discovery_strategy.clone(), kr_pipeline.watchlist_tx, kr_pipeline.eod_tx.unwrap(), cfg.kr.clone(), kr_alert, kr_summary_alert.clone(), activity.clone(), kr_db_pool, t));

    // Scheduler (US)
    let t = token.clone();
    let h_us_sched: JoinHandle<()> = tokio::spawn(pipeline::scheduler::run_scheduler_task(us_adapter, discovery_strategy, us_pipeline.watchlist_tx, us_pipeline.eod_tx.unwrap(), cfg.us.clone(), us_alert, us_summary_alert, activity, us_db_pool, t));

    kr_summary_alert.info(format!("✅ 모든 태스크 준비 완료 [{}] — 장 시작 대기 중", mode));

    tokio::signal::ctrl_c().await?;
    tracing::info!("Received Ctrl+C — initiating graceful shutdown");

    crate::notion::spawn_system_event(&notion_client, crate::notion::SystemEventRow {
        timestamp: chrono::Utc::now().to_rfc3339(), event: "BotShutdown".to_string(), market: "ALL".to_string(), mode: mode.to_string(), detail: "Graceful shutdown initiated (Ctrl+C)".to_string(),
    });

    kr_summary_alert.info("🛑 봇 종료 신호 수신 — Graceful Shutdown 진행 중...".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    token.cancel();

    let _ = h_kr_sched.await; let _ = h_us_sched.await;
    let _ = h_kr_regime.await; let _ = h_us_regime.await;
    let _ = h_kr_tick.await; let _ = h_us_tick.await;
    let _ = h_kr_signal.await; let _ = h_us_signal.await;
    let _ = h_kr_pos.await; let _ = h_us_pos.await;
    let _ = h_kr_exec.await; let _ = h_us_exec.await;
    let _ = h_kr_ctrl.await; let _ = h_us_ctrl.await;
    let _ = h_rest.await; let _ = h_alert.await; let _ = h_monitor.await;

    tracing::info!("Shutdown complete");
    Ok(())
}

async fn run_market_recovery(
    market: &str,
    db_pool: &sqlx::SqlitePool,
    broker_order_nos: Option<Vec<String>>,
) -> bool {
    use crate::control::recovery::{RecoveryInput, RecoveryOutcome};

    let has_orders_without_broker_id: bool = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM orders WHERE state = 'Submitted' AND broker_order_id IS NULL",
    )
    .fetch_one(db_pool)
    .await
    .unwrap_or(0)
        > 0;

    let has_orphaned_submitted_orders = if let Some(broker_ids) = broker_order_nos {
        let broker_set: std::collections::HashSet<&str> =
            broker_ids.iter().map(|s| s.as_str()).collect();
        let db_submitted: Vec<String> = sqlx::query_scalar(
            "SELECT broker_order_id FROM orders WHERE state = 'Submitted' AND broker_order_id IS NOT NULL",
        )
        .fetch_all(db_pool)
        .await
        .unwrap_or_default();
        db_submitted
            .iter()
            .any(|id| !broker_set.contains(id.as_str()))
    } else {
        false
    };

    tracing::info!(
        "{market} recovery input: orphaned={has_orphaned_submitted_orders}, no_broker_id={has_orders_without_broker_id}"
    );

    match crate::control::recovery::run_recovery_check(&RecoveryInput {
        db_position_total: rust_decimal::Decimal::ZERO,
        broker_balance_total: rust_decimal::Decimal::ZERO,
        has_orphaned_submitted_orders,
        unreconciled_fill_count: 0,
        has_orders_without_broker_id,
        mismatch_threshold_pct: rust_decimal_macros::dec!(0.05),
    }) {
        RecoveryOutcome::Fail { code, detail } => {
            tracing::error!("{market} recovery check FAILED: {code:?} — {detail}");
            true
        }
        RecoveryOutcome::AutoFixed { count } => {
            tracing::warn!("{market} recovery: {count} unreconciled fills auto-fixed");
            false
        }
        RecoveryOutcome::Pass => {
            tracing::info!("{market} recovery check passed");
            false
        }
    }
}
