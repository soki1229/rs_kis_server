use crate::config::{Secrets, ServerConfig};
use crate::market::{MarketAdapter, ReadOnlyAdapter};
use crate::pipeline;
use crate::shared;
use crate::state::{BotState, PipelineConfig};
use crate::strategy::StrategyBundle;
use crate::types::Market;

use kis_api::{KisClient, KisEnv};
use std::path::PathBuf;
use std::sync::Arc;
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

    let execution_enabled = cfg.bot.execution_enabled == 1;
    let mode = if execution_enabled {
        "Execution"
    } else {
        "Evaluation"
    };

    if !execution_enabled {
        tracing::info!("BOT MODE: EVALUATION (No actual orders will be placed)");
    } else {
        tracing::info!("BOT MODE: EXECUTION (Orders will be sent to designated accounts)");
    }

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
                let _ = ids;
                tracing::info!("Notion workspace bootstrapped");
                Some(Arc::new(tokio::sync::RwLock::new(nc)))
            }
            Err(e) => {
                tracing::warn!("Notion bootstrap failed (non-fatal): {}", e);
                None
            }
        }
    };

    let vts_cache_path = PathBuf::from(shellexpand::tilde(&cfg.token_cache.vts_path).into_owned());
    if let Some(parent) = vts_cache_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    let real_cache_path =
        PathBuf::from(shellexpand::tilde(&cfg.token_cache.real_path).into_owned());
    if let Some(parent) = real_cache_path.parent() {
        tokio::fs::create_dir_all(parent).await.ok();
    }

    // Initialize Clients
    let vts_client = KisClient::with_cache(
        &secrets.vts_app_key,
        &secrets.vts_app_secret,
        KisEnv::Vts,
        Some(vts_cache_path),
    )
    .await?;

    let has_distinct_real_key = secrets.real_app_key != secrets.vts_app_key;
    let real_client = match KisClient::with_cache(
        &secrets.real_app_key,
        &secrets.real_app_secret,
        KisEnv::Real,
        Some(real_cache_path),
    )
    .await
    {
        Ok(client) => {
            let t = token.clone();
            tokio::spawn(shared::token::run_token_refresh_task(client.clone(), 10, t));
            client
        }
        Err(e) => {
            if has_distinct_real_key {
                return Err(anyhow::anyhow!("Failed to init Real client: {}", e));
            } else {
                tracing::warn!("Real client initialization failed (using VTS key): {}. Falling back to VTS client for everything.", e);
                vts_client.clone()
            }
        }
    };

    // VTS Token Refresh
    {
        let t = token.clone();
        tokio::spawn(shared::token::run_token_refresh_task(
            vts_client.clone(),
            10,
            t,
        ));
    }

    let kr_vts_client = vts_client.clone();
    let us_vts_client = vts_client.clone();
    let kr_real_client = real_client.clone();
    let us_real_client = real_client.clone();

    let real_throttler = Arc::new(shared::throttler::KisThrottler::new(500)); // 2 TPS for Real
    let vts_throttler = Arc::new(shared::throttler::KisThrottler::new(1200)); // VTS 1 TPS + 200ms margin

    let adapters_factory = crate::run_generic::MarketAdapters::new(
        kr_real_client.clone(),
        us_real_client.clone(),
        kr_vts_client.clone(),
        us_vts_client.clone(),
        real_throttler.clone(),
        vts_throttler.clone(),
    );

    // Select WebSocket Client (Shared)
    let prefers_real_data = cfg.kr.data_provider == crate::config::AccountKind::Real
        || cfg.us.data_provider == crate::config::AccountKind::Real;

    let ws_client = if prefers_real_data && real_client.env() == KisEnv::Real {
        &real_client
    } else {
        &vts_client
    };

    let ws_url = ws_client.ws_url();
    let is_vts_ws = ws_client.env() == KisEnv::Vts;

    tracing::info!(
        "connecting shared WebSocket stream ({}) to {}...",
        if is_vts_ws { "VTS" } else { "Real" },
        ws_url
    );

    // Bypass cache and force fetch a fresh approval key for WebSocket
    let ws_approval_key = ws_client
        .approval_key()
        .await
        .expect("WebSocket approval_key 강제 발급 실패");

    tracing::info!("WebSocket approval_key 발급 완료 (Fresh)");

    let shared_stream =
        pipeline::stream::StreamManager::connect(ws_url, ws_approval_key, ws_client.clone(), 4096)
            .await?;
    tracing::info!("WebSocket stream 연결 완료");

    // Construct final adapters with ReadOnly protection if needed
    let mut kr_adapter: Arc<dyn MarketAdapter> = match cfg.kr.trading_account {
        crate::config::AccountKind::Real => adapters_factory.kr_real.clone(),
        crate::config::AccountKind::Vts => adapters_factory.kr_vts.clone(),
    };
    if !execution_enabled {
        kr_adapter = Arc::new(ReadOnlyAdapter::new(kr_adapter));
    }

    let mut us_adapter: Arc<dyn MarketAdapter> = match cfg.us.trading_account {
        crate::config::AccountKind::Real => adapters_factory.us_real.clone(),
        crate::config::AccountKind::Vts => adapters_factory.us_vts.clone(),
    };
    if !execution_enabled {
        us_adapter = Arc::new(ReadOnlyAdapter::new(us_adapter));
    }

    // ── Pipeline State & Channels ──────────────────────────────────────────
    let (kr_live_tx, kr_state) = PipelineConfig::new(activity.clone()).build();
    let (us_live_tx, us_state) = PipelineConfig::new(activity.clone()).build();

    if execution_enabled {
        kr_state.summary.write().unwrap().bot_state = BotState::Active;
        us_state.summary.write().unwrap().bot_state = BotState::Active;
        tracing::info!("execution_enabled=1 → BotState 초기값 Active 설정");
    }

    let mut kr_pipeline = pipeline::MarketPipeline::new(cfg.kr.db_path.clone());
    let mut us_pipeline = pipeline::MarketPipeline::new(cfg.us.db_path.clone());

    // ── Shared Shared Tasks ────────────────────────────────────────────────
    let h_rest = tokio::spawn(shared::rest::run_rest_task(
        cfg.rest_port,
        kr_state.clone(),
        us_state.clone(),
        kr_pipeline.control_tx.clone(),
        us_pipeline.control_tx.clone(),
        secrets.rest_admin_token.clone(),
        token.clone(),
    ));

    let h_alert = tokio::spawn(shared::telegram::run_alert_task_with_rx(
        secrets.alert_bot_token.clone(),
        secrets.alert_chat_id,
        kr_pipeline.alert.subscribe(),
        us_pipeline.alert.subscribe(),
        token.clone(),
    ));

    let h_monitor = tokio::spawn(shared::telegram::run_monitor_task(
        secrets.monitor_bot_token.clone(),
        secrets.monitor_chat_id,
        kr_pipeline.summary_alert.subscribe(),
        us_pipeline.summary_alert.subscribe(),
        kr_pipeline.control_tx.clone(),
        us_pipeline.control_tx.clone(),
        kr_state.clone(),
        us_state.clone(),
        activity.clone(),
        token.clone(),
    ));

    let h_kr_ctrl = tokio::spawn(shared::control::run_control_task(
        kr_pipeline.control_rx,
        kr_pipeline.force_order_tx.clone(),
        kr_state.summary.clone(),
        cfg.kr.kill_switch_path.clone(),
        token.clone(),
    ));

    let h_us_ctrl = tokio::spawn(shared::control::run_control_task(
        us_pipeline.control_rx,
        us_pipeline.force_order_tx.clone(),
        us_state.summary.clone(),
        cfg.us.kill_switch_path.clone(),
        token.clone(),
    ));

    // Pipeline Tasks initialization
    let mut h_kr_regime = None;
    let mut h_us_regime = None;
    let mut h_kr_tick = None;
    let mut h_us_tick = None;
    let mut h_kr_signal = None;
    let mut h_us_signal = None;
    let mut h_kr_pos = None;
    let mut h_us_pos = None;
    let mut h_kr_exec = None;
    let mut h_us_exec = None;

    let kr_alert_router = kr_pipeline.alert.clone();
    let us_alert_router = us_pipeline.alert.clone();

    // ── KR initialization ──────────────────────────────────────────────────
    let kr_db_pool = crate::db::connect(&cfg.kr.db_path).await?;
    let kr_timing = kr_adapter.market_timing();
    let kr_active = kr_timing.is_open;
    if kr_active {
        tracing::info!(
            "KR market status: is_open=true, since_open={}, until_close={}",
            crate::market::MarketTiming::format_mins(kr_timing.mins_since_open),
            crate::market::MarketTiming::format_mins(kr_timing.mins_until_close)
        );
        // CRITICAL: Run recovery and seeding BEFORE starting ticker
        run_market_recovery("KR", &kr_db_pool, kr_adapter.clone()).await;
        let initial_watchlist = pipeline::scheduler::build_watchlist(
            kr_adapter.clone(),
            &discovery_strategy,
            &cfg.kr,
            &kr_alert_router,
            &kr_db_pool,
        )
        .await;
        pipeline::signal::seed_symbols(
            &initial_watchlist.all_unique(),
            kr_adapter.as_ref(),
            &kr_db_pool,
            true,
        )
        .await;
        let _ = kr_pipeline.watchlist_tx.send(initial_watchlist);
    } else {
        tracing::info!(
            "KR market status: is_open=false, until_open={}",
            crate::market::MarketTiming::format_mins(kr_timing.mins_until_open)
        );
        tracing::info!(
            "KR market is closed. Pipeline tasks will be started by scheduler when market opens."
        );
    }

    if cfg.kr.use_generic_pipeline {
        let t = token.clone();
        let (regime_tx, regime_rx) = tokio::sync::watch::channel(crate::types::MarketRegime::Quiet);
        h_kr_regime = Some(tokio::spawn(pipeline::run_generic_regime_task(
            kr_adapter.clone(),
            regime_strategy.clone(),
            regime_tx,
            kr_alert_router.clone(),
            t,
            "069500", // KODEX 200 (KOSPI200 ETF) — KR 시장 벤치마크
            kr_db_pool.clone(),
        )));

        let t = token.clone();
        h_kr_tick = Some(tokio::spawn(pipeline::tick::run_tick_task(
            Market::Kr,
            kr_adapter.clone(),
            shared_stream.clone(),
            kr_pipeline.watchlist_rx.clone(),
            kr_pipeline.tick_tx.clone(),
            kr_pipeline.tick_pos_tx.clone(),
            kr_pipeline.quote_tx.clone(),
            kr_alert_router.clone(),
            activity.clone(),
            kr_db_pool.clone(),
            t,
        )));

        let t = token.clone();
        h_kr_signal = Some(tokio::spawn(pipeline::signal::run_signal_task(
            kr_pipeline.tick_tx.subscribe(),
            kr_pipeline.quote_rx,
            kr_pipeline.order_tx.clone(),
            regime_rx.clone(),
            kr_db_pool.clone(),
            kr_adapter.clone(),
            kr_pipeline.watchlist_rx.clone(),
            kr_state.summary.clone(),
            cfg.signal.clone(),
            cfg.kr.strategies.clone(),
            activity.clone(),
            notion_client.clone(),
            kr_state.live_state_rx.clone(),
            signal_strategy.clone(),
            qual_strategy.clone(),
            risk_strategy.clone(),
            t,
        )));

        let t = token.clone();
        let kr_poll_sem = Arc::new(tokio::sync::Semaphore::new(1));
        let kr_pending = Arc::new(std::sync::atomic::AtomicU32::new(0));

        h_kr_exec = Some(tokio::spawn(pipeline::run_generic_execution_task(
            kr_adapter.clone(),
            kr_pipeline.order_rx,
            kr_pipeline.force_order_rx,
            kr_pipeline.fill_tx.clone(),
            kr_db_pool.clone(),
            kr_state.summary.clone(),
            kr_alert_router.clone(),
            cfg.risk.twap.clone(),
            t,
            kr_pending,
            kr_poll_sem,
        )));

        let t = token.clone();
        h_kr_pos = Some(tokio::spawn(pipeline::run_generic_position_task(
            kr_adapter.clone(),
            kr_pipeline.fill_rx,
            kr_pipeline.tick_pos_rx,
            kr_pipeline.eod_rx.take().expect("eod_rx missing"),
            kr_live_tx,
            kr_pipeline.force_order_tx.clone(),
            regime_rx,
            kr_db_pool.clone(),
            t,
            {
                // EOD fallback: 장 종료 5분 전 — throttler+TWAP 처리 시간 확보
                let now = chrono::Utc::now();
                let close = kr_adapter
                    .market_close_utc(kr_adapter.local_today())
                    .unwrap()
                    - chrono::Duration::minutes(5);
                if close > now {
                    close
                } else {
                    kr_adapter
                        .market_close_utc(kr_adapter.local_today() + chrono::Duration::days(1))
                        .unwrap()
                        - chrono::Duration::minutes(5)
                }
            },
            cfg.position.clone(),
            kr_pipeline.summary_alert.clone(),
            kr_state.refresh_notify.clone(),
        )));
    }

    // ── US initialization ──────────────────────────────────────────────────
    let us_db_pool = crate::db::connect(&cfg.us.db_path).await?;
    let us_timing = us_adapter.market_timing();
    let us_active = us_timing.is_open;
    if us_active {
        tracing::info!(
            "US market status: is_open=true, since_open={}, until_close={}",
            crate::market::MarketTiming::format_mins(us_timing.mins_since_open),
            crate::market::MarketTiming::format_mins(us_timing.mins_until_close)
        );
        run_market_recovery("US", &us_db_pool, us_adapter.clone()).await;
        let initial_watchlist = pipeline::scheduler::build_watchlist(
            us_adapter.clone(),
            &discovery_strategy,
            &cfg.us,
            &us_alert_router,
            &us_db_pool,
        )
        .await;
        pipeline::signal::seed_symbols(
            &initial_watchlist.all_unique(),
            us_adapter.as_ref(),
            &us_db_pool,
            true,
        )
        .await;
        let _ = us_pipeline.watchlist_tx.send(initial_watchlist);
    } else {
        tracing::info!(
            "US market status: is_open=false, until_open={}",
            crate::market::MarketTiming::format_mins(us_timing.mins_until_open)
        );
        tracing::info!(
            "US market is closed. Pipeline tasks will be started by scheduler when market opens."
        );
    }

    if cfg.us.use_generic_pipeline {
        let t = token.clone();
        let (regime_tx, regime_rx) = tokio::sync::watch::channel(crate::types::MarketRegime::Quiet);
        h_us_regime = Some(tokio::spawn(pipeline::run_generic_regime_task(
            us_adapter.clone(),
            regime_strategy.clone(),
            regime_tx,
            us_alert_router.clone(),
            t,
            "QQQ",
            us_db_pool.clone(),
        )));

        let t = token.clone();
        h_us_tick = Some(tokio::spawn(pipeline::tick::run_tick_task(
            Market::Us,
            us_adapter.clone(),
            shared_stream.clone(),
            us_pipeline.watchlist_rx.clone(),
            us_pipeline.tick_tx.clone(),
            us_pipeline.tick_pos_tx.clone(),
            us_pipeline.quote_tx.clone(),
            us_alert_router.clone(),
            activity.clone(),
            us_db_pool.clone(),
            t,
        )));

        let t = token.clone();
        h_us_signal = Some(tokio::spawn(pipeline::signal::run_signal_task(
            us_pipeline.tick_tx.subscribe(),
            us_pipeline.quote_rx,
            us_pipeline.order_tx.clone(),
            regime_rx.clone(),
            us_db_pool.clone(),
            us_adapter.clone(),
            us_pipeline.watchlist_rx.clone(),
            us_state.summary.clone(),
            cfg.signal.clone(),
            cfg.us.strategies.clone(),
            activity.clone(),
            notion_client.clone(),
            us_state.live_state_rx.clone(),
            signal_strategy.clone(),
            qual_strategy.clone(),
            risk_strategy.clone(),
            t,
        )));

        let t = token.clone();
        let us_poll_sem = Arc::new(tokio::sync::Semaphore::new(1));
        let us_pending = Arc::new(std::sync::atomic::AtomicU32::new(0));

        h_us_exec = Some(tokio::spawn(pipeline::run_generic_execution_task(
            us_adapter.clone(),
            us_pipeline.order_rx,
            us_pipeline.force_order_rx,
            us_pipeline.fill_tx.clone(),
            us_db_pool.clone(),
            us_state.summary.clone(),
            us_alert_router.clone(),
            cfg.risk.twap.clone(),
            t,
            us_pending,
            us_poll_sem,
        )));

        let t = token.clone();
        h_us_pos = Some(tokio::spawn(pipeline::run_generic_position_task(
            us_adapter.clone(),
            us_pipeline.fill_rx,
            us_pipeline.tick_pos_rx,
            us_pipeline.eod_rx.take().expect("eod_rx missing"),
            us_live_tx,
            us_pipeline.force_order_tx.clone(),
            regime_rx,
            us_db_pool.clone(),
            t,
            {
                // EOD fallback: 장 종료 5분 전 — throttler+TWAP 처리 시간 확보
                let now = chrono::Utc::now();
                let close = us_adapter
                    .market_close_utc(us_adapter.local_today())
                    .unwrap()
                    - chrono::Duration::minutes(5);
                if close > now {
                    close
                } else {
                    us_adapter
                        .market_close_utc(us_adapter.local_today() + chrono::Duration::days(1))
                        .unwrap()
                        - chrono::Duration::minutes(5)
                }
            },
            cfg.position.clone(),
            us_pipeline.summary_alert.clone(),
            us_state.refresh_notify.clone(),
        )));
    }

    // Capture market labels before adapters are moved into scheduler tasks
    let kr_market_label = kr_adapter.market_id().label();
    let us_market_label = us_adapter.market_id().label();

    // Scheduler (KR)
    let t = token.clone();
    let h_kr_sched = Some(tokio::spawn(pipeline::scheduler::run_scheduler_task(
        kr_adapter,
        discovery_strategy.clone(),
        kr_pipeline.watchlist_tx,
        kr_pipeline.eod_tx.unwrap(),
        cfg.kr.clone(),
        kr_alert_router.clone(),
        kr_pipeline.summary_alert.clone(),
        activity.clone(),
        kr_db_pool.clone(),
        t,
    )));

    // Scheduler (US)
    let t = token.clone();
    let h_us_sched = Some(tokio::spawn(pipeline::scheduler::run_scheduler_task(
        us_adapter,
        discovery_strategy,
        us_pipeline.watchlist_tx,
        us_pipeline.eod_tx.unwrap(),
        cfg.us.clone(),
        us_alert_router,
        us_pipeline.summary_alert.clone(),
        activity,
        us_db_pool.clone(),
        t,
    )));

    kr_pipeline.summary_alert.info(format!(
        "✅ 준비완료 [{mode}] KR {kr_market_label} | US {us_market_label}",
    ));

    let bot_start_detail = format!("mode={mode}");
    let bot_start_at = chrono::Utc::now().to_rfc3339();
    for (pool, market) in [
        (&kr_db_pool, kr_market_label),
        (&us_db_pool, us_market_label),
    ] {
        sqlx::query(
            "INSERT INTO audit_log (event_type, market, symbol, detail, created_at) VALUES ('bot_started', ?, NULL, ?, ?)",
        )
        .bind(market)
        .bind(&bot_start_detail)
        .bind(&bot_start_at)
        .execute(pool)
        .await
        .ok();
    }

    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate())?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                tracing::info!("Received SIGINT (Ctrl+C) — initiating graceful shutdown");
            }
            _ = sigterm.recv() => {
                tracing::info!("Received SIGTERM — initiating graceful shutdown");
            }
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        tracing::info!("Received Ctrl+C — initiating graceful shutdown");
    }

    let bot_stop_at = chrono::Utc::now().to_rfc3339();
    for (pool, market) in [
        (&kr_db_pool, kr_market_label),
        (&us_db_pool, us_market_label),
    ] {
        sqlx::query(
            "INSERT INTO audit_log (event_type, market, symbol, detail, created_at) VALUES ('bot_stopped', ?, NULL, 'Graceful shutdown', ?)",
        )
        .bind(market)
        .bind(&bot_stop_at)
        .execute(pool)
        .await
        .ok();
    }

    crate::notion::spawn_system_event(
        &notion_client,
        crate::notion::SystemEventRow {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event: "BotShutdown".to_string(),
            market: "ALL".to_string(),
            mode: mode.to_string(),
            detail: "Graceful shutdown initiated".to_string(),
        },
    );

    let stop_time = chrono::Utc::now()
        .with_timezone(&chrono_tz::Asia::Seoul)
        .format("%m/%d %H:%M")
        .to_string();
    kr_pipeline
        .summary_alert
        .info(format!("⛔ 봇 종료 [{stop_time} KST] — Graceful Shutdown"));
    kr_pipeline
        .alert
        .critical("Bot Process Termination Initiated".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    token.cancel();

    if let Some(h) = h_kr_sched {
        let _ = h.await;
    }
    if let Some(h) = h_us_sched {
        let _ = h.await;
    }
    if let Some(h) = h_kr_regime {
        let _ = h.await;
    }
    if let Some(h) = h_us_regime {
        let _ = h.await;
    }
    if let Some(h) = h_kr_tick {
        let _ = h.await;
    }
    if let Some(h) = h_us_tick {
        let _ = h.await;
    }
    if let Some(h) = h_kr_signal {
        let _ = h.await;
    }
    if let Some(h) = h_us_signal {
        let _ = h.await;
    }
    if let Some(h) = h_kr_pos {
        let _ = h.await;
    }
    if let Some(h) = h_us_pos {
        let _ = h.await;
    }
    if let Some(h) = h_kr_exec {
        let _ = h.await;
    }
    if let Some(h) = h_us_exec {
        let _ = h.await;
    }
    let _ = h_kr_ctrl.await;
    let _ = h_us_ctrl.await;
    let _ = h_rest.await;
    let _ = h_alert.await;
    let _ = h_monitor.await;

    tracing::info!("Shutdown complete");
    Ok(())
}

async fn run_market_recovery(
    market: &str,
    db_pool: &sqlx::SqlitePool,
    adapter: Arc<dyn MarketAdapter>,
) {
    let now = chrono::Utc::now().to_rfc3339();

    // broker_id 없는 SUBMITTED 주문 → Failed 자동 처리 (재시작 전 제출 실패 추정)
    let no_broker_id_count: i64 = sqlx::query_scalar::<_, i64>(
        "SELECT COUNT(*) FROM orders WHERE state = 'Submitted' AND broker_order_id IS NULL",
    )
    .fetch_one(db_pool)
    .await
    .unwrap_or(0);

    if no_broker_id_count > 0 {
        let reason =
            r#"{"Failed":{"reason":"재시작 복구: broker_order_id 없음 (제출 미완료 추정)"}}"#;
        let updated = sqlx::query(
            "UPDATE orders SET state = ?, updated_at = ? WHERE state = 'Submitted' AND broker_order_id IS NULL",
        )
        .bind(reason)
        .bind(&now)
        .execute(db_pool)
        .await
        .map(|r| r.rows_affected())
        .unwrap_or(0);
        tracing::warn!(
            market,
            updated,
            "recovery: broker_order_id 없는 SUBMITTED 주문 → Failed 자동 처리"
        );
    }

    // 브로커 미체결 목록과 대조 — orphaned SUBMITTED 주문 → Failed 자동 처리
    let broker_ids: Option<Vec<String>> = match adapter.unfilled_orders().await {
        Ok(orders) => Some(orders.into_iter().map(|o| o.order_no).collect()),
        Err(_) => None,
    };

    if let Some(ref ids) = broker_ids {
        let broker_set: std::collections::HashSet<&str> = ids.iter().map(|s| s.as_str()).collect();

        // (내부 id, broker_order_id) 쌍으로 조회
        let db_submitted: Vec<(String, String)> = sqlx::query_as(
            "SELECT id, broker_order_id FROM orders WHERE state = 'Submitted' AND broker_order_id IS NOT NULL",
        )
        .fetch_all(db_pool)
        .await
        .unwrap_or_default();

        let reason =
            r#"{"Failed":{"reason":"재시작 복구: 브로커 미체결 목록 없음 (체결/취소 추정)"}}"#;
        let mut fixed = 0u64;
        for (order_id, broker_id) in &db_submitted {
            if !broker_set.contains(broker_id.as_str()) {
                sqlx::query("UPDATE orders SET state = ?, updated_at = ? WHERE id = ?")
                    .bind(reason)
                    .bind(&now)
                    .bind(order_id)
                    .execute(db_pool)
                    .await
                    .ok();
                fixed += 1;
            }
        }
        if fixed > 0 {
            tracing::warn!(
                market,
                fixed,
                "recovery: orphaned SUBMITTED 주문 → Failed 자동 처리"
            );
        } else {
            tracing::info!(market, "recovery: orphaned 주문 없음");
        }
    } else {
        tracing::warn!(
            market,
            "recovery: unfilled_orders API 실패 — orphaned 검사 스킵"
        );
    }
}
