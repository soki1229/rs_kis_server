use crate::config::{Secrets, ServerConfig, TunableConfig};
use crate::dual_client::{DualClients, DualKisClient, DualKisDomesticClient};
use crate::pipeline;
use crate::shared;
use crate::state;
use crate::strategy::StrategyBundle;
use crate::types::BotCommand;

use kis_api::{KisClient, KisConfig, KisDomesticApi, KisDomesticClient};
use std::sync::{atomic::AtomicU32, Arc};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

fn is_market_active(market: &str) -> bool {
    let now = chrono::Utc::now();
    if market == "KR" {
        use chrono_tz::Asia::Seoul;
        let kst = now.with_timezone(&Seoul).time();
        let start = chrono::NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let end = chrono::NaiveTime::from_hms_opt(16, 30, 0).unwrap();
        kst >= start && kst <= end
    } else {
        use chrono_tz::America::New_York;
        let et = now.with_timezone(&New_York).time();
        let start = chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap();
        let end = chrono::NaiveTime::from_hms_opt(17, 0, 0).unwrap();
        et >= start && et <= end
    }
}

/// Framework entry point. Call this from your binary after assembling a StrategyBundle.
///
/// Note: StrategyBundle is accepted but not yet wired into pipeline tasks.
/// Pipeline tasks still call regime/qualification logic directly.
/// Future versions will inject strategy traits into each pipeline task.
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
    if kr_effective_dry_run || us_effective_dry_run {
        tracing::warn!(
            kr_dry = kr_effective_dry_run,
            us_dry = us_effective_dry_run,
            "dry_run mode: affected markets use VTS for trade APIs"
        );
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

    if cfg.risk.earnings_blackout_symbols.is_empty() {
        tracing::warn!("earnings_blackout_symbols is empty — earnings blocking is NOT active.");
    } else {
        tracing::info!(
            "Earnings blackout active for {} symbols: {:?}",
            cfg.risk.earnings_blackout_symbols.len(),
            cfg.risk.earnings_blackout_symbols
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

    let tunable = TunableConfig::from_server_config(&cfg);
    let (tunable_tx, tunable_rx) = tokio::sync::watch::channel(tunable);
    let tunable_tx = Arc::new(tunable_tx);
    let _tunable_rx_keepalive = tunable_rx;

    let real_config = KisConfig::from_env()?;
    tracing::debug!(
        app_key_prefix = &real_config.app_key[..8],
        rest_url = %real_config.rest_url,
        mock = real_config.mock,
        "real KisConfig loaded"
    );
    let (kis_client, kr_client): (Arc<dyn kis_api::KisApi>, Arc<dyn KisDomesticApi>) =
        match (us_effective_dry_run, kr_effective_dry_run) {
            (true, true) => {
                let vts_config = KisConfig::from_env_vts()?;
                tracing::info!("dual-token mode (both): data=real, trade=VTS");
                let dual = DualClients::new(real_config, vts_config);
                (Arc::new(dual.overseas), Arc::new(dual.domestic))
            }
            (true, false) => {
                let vts_config = KisConfig::from_env_vts()?;
                tracing::info!("dual-token mode (US only): US trade=VTS, KR trade=real");
                let data_us = KisClient::new(real_config.clone());
                let trade_us = KisClient::new(vts_config);
                let kr = KisDomesticClient::from_client(&data_us);
                (
                    Arc::new(DualKisClient::from_clients(data_us, trade_us)),
                    Arc::new(kr),
                )
            }
            (false, true) => {
                let vts_config = KisConfig::from_env_vts()?;
                tracing::info!("dual-token mode (KR only): KR trade=VTS, US trade=real");
                let real_us = KisClient::new(real_config.clone());
                let data_kr = KisDomesticClient::from_client(&real_us);
                let trade_kr = KisDomesticClient::new(vts_config);
                (
                    Arc::new(real_us),
                    Arc::new(DualKisDomesticClient::from_clients(data_kr, trade_kr)),
                )
            }
            (false, false) => {
                let overseas = KisClient::new(real_config);
                let domestic = KisDomesticClient::from_client(&overseas);
                (Arc::new(overseas), Arc::new(domestic))
            }
        };

    tracing::info!("connecting shared WebSocket stream...");
    let shared_stream = kis_client
        .stream()
        .await
        .expect("failed to establish shared WebSocket stream");
    tracing::info!("shared WebSocket stream established for KR+US tick data");

    let mut kr = pipeline::MarketPipeline::new(&cfg.kr.db_path);
    let mut us = pipeline::MarketPipeline::new(&cfg.us.db_path);

    let kr_config = state::PipelineConfig::new(activity.clone());
    let us_config = state::PipelineConfig::new(activity.clone());

    let (kr_live_tx, kr_state) = kr_config.build();
    let (us_live_tx, us_state) = us_config.build();

    let (kr_cmd_tx, kr_cmd_rx) = tokio::sync::mpsc::channel::<BotCommand>(64);
    let (us_cmd_tx, us_cmd_rx) = tokio::sync::mpsc::channel::<BotCommand>(64);

    // ControlTask (KR)
    let kr_summary = kr_state.summary.clone();
    let kr_summary2 = kr_state.summary.clone();
    let kr_force_tx = kr.force_order_tx.clone();
    let kr_ks_path = cfg.kr.kill_switch_path.clone();
    let t = token.clone();
    let h_kr_ctrl: JoinHandle<()> = tokio::spawn(async move {
        shared::control::run_control_task(kr_cmd_rx, kr_force_tx, kr_summary, kr_ks_path, t).await;
    });

    // ControlTask (US)
    let us_summary = us_state.summary.clone();
    let us_summary2 = us_state.summary.clone();
    let us_signal_summary = us_state.summary.clone();
    let us_force_tx = us.force_order_tx.clone();
    let us_ks_path = cfg.us.kill_switch_path.clone();
    let t = token.clone();
    let h_us_ctrl: JoinHandle<()> = tokio::spawn(async move {
        shared::control::run_control_task(us_cmd_rx, us_force_tx, us_summary, us_ks_path, t).await;
    });

    let kr_alert = kr.alert.clone();
    let us_alert = us.alert.clone();
    let kr_summary_alert = kr.summary_alert.clone();
    let us_summary_alert = us.summary_alert.clone();
    let kr_alert_rx = kr_alert.subscribe();
    let us_alert_rx = us_alert.subscribe();
    let kr_monitor_rx = kr_summary_alert.subscribe();
    let us_monitor_rx = us_summary_alert.subscribe();

    // AlertTask
    let alert_bot_token = secrets.alert_bot_token.clone();
    let alert_chat_id = secrets.alert_chat_id;
    let t = token.clone();
    let h_alert: JoinHandle<()> = tokio::spawn(async move {
        shared::telegram::run_alert_task_with_rx(
            alert_bot_token,
            alert_chat_id,
            kr_alert_rx,
            us_alert_rx,
            t,
        )
        .await;
    });

    // MonitorTask
    let kr_tx = kr_cmd_tx.clone();
    let us_tx = us_cmd_tx.clone();
    let monitor_bot_token = secrets.monitor_bot_token.clone();
    let monitor_chat_id = secrets.monitor_chat_id;
    let kr_st_tg = kr_state.clone();
    let us_st_tg = us_state.clone();
    let monitor_activity = activity.clone();
    let t = token.clone();
    let h_monitor: JoinHandle<()> = tokio::spawn(async move {
        shared::telegram::run_monitor_task(
            monitor_bot_token,
            monitor_chat_id,
            kr_monitor_rx,
            us_monitor_rx,
            kr_tx,
            us_tx,
            kr_st_tg,
            us_st_tg,
            monitor_activity,
            t,
        )
        .await;
    });

    let kr_startup_alert = kr.summary_alert.clone();
    let us_startup_alert = us.summary_alert.clone();

    let mode = match (kr_effective_dry_run, us_effective_dry_run) {
        (true, true) => "모의투자(VTS)",
        (true, false) => "국내모의/미국실전",
        (false, true) => "국내실전/미국모의",
        (false, false) => "실전투자(LIVE)",
    };
    kr_startup_alert.info(format!("🚀 KIS Automated Trading Bot 가동 시작 [{}]", mode));

    // RestApiTask
    let kr_st = kr_state.clone();
    let us_st = us_state.clone();
    let admin_token = secrets.rest_admin_token.clone();
    let t = token.clone();
    let h_rest: JoinHandle<()> = tokio::spawn(async move {
        if let Err(e) = shared::rest::run_rest_task(
            cfg.rest_port,
            kr_st,
            us_st,
            kr_cmd_tx,
            us_cmd_tx,
            admin_token,
            t,
        )
        .await
        {
            tracing::error!("RestApiTask error: {}", e);
        }
    });

    // ── KR pipeline ──────────────────────────────────────────────────────
    let kr_db_pool = crate::db::connect(&cfg.kr.db_path).await?;

    let kr_active = is_market_active("KR");
    let kr_recovery_failed = if kr_active {
        let kr_broker_order_nos: Option<Vec<String>> = if !kr_effective_dry_run {
            let db_submitted: Vec<String> = sqlx::query_scalar(
                "SELECT broker_order_id FROM orders WHERE state = 'Submitted' AND broker_order_id IS NOT NULL",
            )
            .fetch_all(&kr_db_pool)
            .await
            .unwrap_or_default();
            if db_submitted.is_empty() {
                None
            } else {
                match kr_client.domestic_unfilled_orders().await {
                    Ok(unfilled) => Some(unfilled.into_iter().map(|o| o.order_no).collect()),
                    Err(e) => {
                        tracing::warn!(
                            "KR unfilled_orders fetch failed: {e} — skipping orphan check"
                        );
                        None
                    }
                }
            }
        } else {
            None
        };
        run_market_recovery("KR", &kr_db_pool, kr_broker_order_nos).await
    } else {
        false
    };

    if kr_recovery_failed {
        kr_state.summary.write().unwrap().bot_state = state::BotState::HardBlocked;
        tracing::error!("KR bot set to HardBlocked due to recovery failure");
        kr_startup_alert.critical("🚨 복구 실패 — HardBlocked 상태. 수동 점검 필요!".to_string());
    } else {
        kr_state.summary.write().unwrap().bot_state = state::BotState::Active;
        if kr_active {
            kr_startup_alert.info("✅ [국내] 시스템 복구 및 무결성 검사 완료".to_string());
        }
    }

    let (kr_regime_tx, kr_regime_rx) =
        crate::regime::regime_channel(crate::types::MarketRegime::Trending);
    let kr_regime_rx_pos = kr_regime_rx.clone();

    let kr_regime_client = Arc::clone(&kr_client);
    let kr_regime_alert = kr.alert.clone();
    let kr_regime_strategy = Arc::clone(&regime_strategy);
    let t = token.clone();
    let h_kr_regime: JoinHandle<()> = tokio::spawn(async move {
        pipeline::regime::run_kr_regime_task(
            kr_regime_client,
            kr_regime_strategy,
            kr_regime_tx,
            kr_regime_alert,
            t,
        )
        .await;
    });

    if kr_active {
        let kr_wl =
            pipeline::scheduler::build_kr_watchlist(&kr_client, &cfg.kr, &kr.alert, &kr_db_pool)
                .await;
        if !kr_wl.all_unique().is_empty() {
            tracing::info!("🏃 KR Warmup: 과거 데이터(150일치) 수집 시작...");
            let symbols = kr_wl.all_unique();
            let _ =
                pipeline::signal::seed_kr_symbols(&symbols, &kr_client, &kr_db_pool, false).await;
            kr.watchlist_tx.send(kr_wl).ok();
            tracing::info!("✅ KR Warmup: 준비 완료");
        }
    }

    let kr_tick_tx = kr.tick_tx.clone();
    let kr_tick_pos_tx = kr.tick_pos_tx.clone();
    let kr_tick_quote_tx = kr.quote_tx.clone();
    let kr_tick_stream = shared_stream.clone();
    let kr_tick_watchlist_rx = kr.watchlist_rx.clone();
    let kr_tick_alert = kr.alert.clone();
    let kr_tick_activity = activity.clone();
    let kr_tick_db = kr_db_pool.clone();
    let t = token.clone();
    let h_kr_tick: JoinHandle<()> = tokio::spawn(async move {
        pipeline::tick::run_kr_tick_task(
            kr_tick_stream,
            kr_tick_watchlist_rx,
            kr_tick_tx,
            kr_tick_pos_tx,
            kr_tick_quote_tx,
            kr_tick_alert,
            kr_tick_activity,
            kr_tick_db,
            t,
        )
        .await;
    });

    let kr_pending = Arc::new(AtomicU32::new(0));

    let kr_tick_rx = kr.tick_tx.subscribe();
    let kr_quote_rx = kr.quote_rx;
    let kr_order_tx = kr.order_tx.clone();
    let kr_signal_client = Arc::clone(&kr_client);
    let kr_signal_watchlist_rx = kr.watchlist_rx.clone();
    let kr_signal_alert = kr.alert.clone();
    let kr_signal_db = kr_db_pool.clone();
    let kr_signal_summary = kr_state.summary.clone();
    let kr_signal_pending = Arc::clone(&kr_pending);
    let kr_risk_cfg = cfg.risk.clone();
    let kr_signal_cfg = cfg.signal.clone();
    let kr_signal_activity = activity.clone();
    let kr_strategies = cfg.kr.strategies.clone();
    let kr_signal_notion = notion_client.clone();
    let kr_live_state_rx = kr_state.live_state_rx.clone();
    let kr_signal_strategy = Arc::clone(&signal_strategy);
    let kr_qual_strategy = Arc::clone(&qual_strategy);
    let kr_risk_strategy = Arc::clone(&risk_strategy);
    let t = token.clone();
    let h_kr_signal: JoinHandle<()> = tokio::spawn(async move {
        pipeline::signal::run_kr_signal_task(
            kr_tick_rx,
            kr_quote_rx,
            kr_order_tx,
            kr_regime_rx,
            kr_signal_db,
            kr_signal_client,
            kr_signal_watchlist_rx,
            kr_signal_summary,
            kr_signal_alert,
            kr_signal_pending,
            kr_risk_cfg,
            kr_signal_cfg,
            kr_strategies,
            kr_signal_activity,
            kr_signal_notion,
            kr_live_state_rx,
            kr_signal_strategy,
            kr_qual_strategy,
            kr_risk_strategy,
            t,
        )
        .await;
    });

    let poll_sem = Arc::new(tokio::sync::Semaphore::new(3));

    let kr_order_rx = kr.order_rx;
    let kr_force_rx = kr.force_order_rx;
    let kr_fill_tx = kr.fill_tx.clone();
    let kr_exec_client = Arc::clone(&kr_client);
    let kr_exec_alert = kr.alert.clone();
    let kr_exec_db = kr_db_pool.clone();
    let t = token.clone();
    let kr_poll_sem = Arc::clone(&poll_sem);
    let h_kr_exec: JoinHandle<()> = tokio::spawn(async move {
        pipeline::execution::run_kr_execution_task(
            kr_order_rx,
            kr_force_rx,
            kr_fill_tx,
            kr_exec_client,
            kr_exec_db,
            kr_summary2,
            kr_exec_alert,
            t,
            kr_pending,
            kr_poll_sem,
        )
        .await;
    });

    let kr_fill_rx = kr.fill_rx;
    let kr_tick_pos_rx = kr.tick_pos_rx;
    let kr_eod_rx = kr
        .eod_rx
        .take()
        .ok_or_else(|| anyhow::anyhow!("KR eod_rx already taken"))?;
    let kr_force_order_tx2 = kr.force_order_tx.clone();
    let kr_pos_alert = kr.alert.clone();
    let kr_pos_summary = kr.summary_alert.clone();
    let kr_pos_db = kr_db_pool.clone();
    let t = token.clone();
    let kr_pos_cfg = cfg.position.clone();
    let kr_signal_cfg_pos = cfg.signal.clone();
    let kr_notion = notion_client.clone();
    let kr_tunable_tx = Some(Arc::clone(&tunable_tx));
    let kr_dry_run = kr_effective_dry_run;
    let kr_eod_fallback = pipeline::position::market_close_utc(15, 30, chrono_tz::Asia::Seoul);
    let h_kr_pos: JoinHandle<()> = tokio::spawn(async move {
        pipeline::position::run_position_task(
            kr_fill_rx,
            kr_tick_pos_rx,
            kr_eod_rx,
            kr_live_tx,
            kr_force_order_tx2,
            kr_regime_rx_pos,
            kr_pos_db,
            kr_pos_alert,
            kr_pos_summary,
            t,
            kr_eod_fallback,
            "KR".to_string(),
            kr_pos_cfg,
            kr_notion,
            kr_tunable_tx,
            kr_signal_cfg_pos,
            kr_dry_run,
        )
        .await;
    });

    // ── US pipeline ──────────────────────────────────────────────────────
    let us_db_pool = crate::db::connect(&cfg.us.db_path).await?;

    let us_active = is_market_active("US");
    let us_recovery_failed = if us_active {
        let us_broker_order_nos: Option<Vec<String>> = if !us_effective_dry_run {
            let db_submitted: Vec<String> = sqlx::query_scalar(
                "SELECT broker_order_id FROM orders WHERE state = 'Submitted' AND broker_order_id IS NOT NULL",
            )
            .fetch_all(&us_db_pool)
            .await
            .unwrap_or_default();
            if db_submitted.is_empty() {
                None
            } else {
                match kis_client.unfilled_orders().await {
                    Ok(unfilled) => Some(unfilled.into_iter().map(|o| o.order_no).collect()),
                    Err(e) => {
                        tracing::warn!(
                            "US unfilled_orders fetch failed: {e} — skipping orphan check"
                        );
                        None
                    }
                }
            }
        } else {
            None
        };
        run_market_recovery("US", &us_db_pool, us_broker_order_nos).await
    } else {
        false
    };

    if us_recovery_failed {
        us_state.summary.write().unwrap().bot_state = state::BotState::HardBlocked;
        tracing::error!("US bot set to HardBlocked due to recovery failure");
        us_startup_alert.critical("🚨 복구 실패 — HardBlocked 상태. 수동 점검 필요!".to_string());
    } else {
        us_state.summary.write().unwrap().bot_state = state::BotState::Active;
        if us_active {
            us_startup_alert.info("✅ [미국] 시스템 복구 및 무결성 검사 완료".to_string());
        }
    }

    let (regime_tx, regime_rx) =
        crate::regime::regime_channel(crate::types::MarketRegime::Trending);
    let regime_rx_pos = regime_rx.clone();

    let us_active = is_market_active("US");
    if us_active {
        let us_wl =
            pipeline::scheduler::build_watchlist(&kis_client, &cfg.us, &us.alert, &us_db_pool)
                .await;
        if !us_wl.all_unique().is_empty() {
            tracing::info!("🏃 US Warmup: 과거 데이터 수집 시작...");
            let symbols = us_wl.all_unique();
            let mut news_cache = std::collections::HashMap::new();
            let _ = pipeline::signal::seed_symbols(
                &symbols,
                &kis_client,
                &us_db_pool,
                &mut news_cache,
                false,
            )
            .await;
            us.watchlist_tx.send(us_wl).ok();
            tracing::info!("✅ US Warmup: 준비 완료");
        }
    }

    let us_watchlist_rx = us.watchlist_rx.clone();
    let us_alert_tick = us.alert.clone();
    let us_tick_tx = us.tick_tx.clone();
    let us_tick_pos_tx = us.tick_pos_tx.clone();
    let us_quote_tx = us.quote_tx.clone();
    let us_quote_rx = us.quote_rx;
    let us_tick_stream = shared_stream.clone();
    let us_tick_activity = activity.clone();
    let us_tick_db = us_db_pool.clone();
    let t = token.clone();
    let h_us_tick: JoinHandle<()> = tokio::spawn(async move {
        pipeline::tick::run_us_tick_task(
            us_tick_stream,
            us_watchlist_rx,
            us_tick_tx,
            us_tick_pos_tx,
            us_quote_tx,
            us_alert_tick,
            us_tick_activity,
            us_tick_db,
            t,
        )
        .await;
    });

    let us_regime_client = Arc::clone(&kis_client);
    let us_regime_alert = us.alert.clone();
    let us_regime_strategy = Arc::clone(&regime_strategy);
    let t = token.clone();
    let h_us_regime: JoinHandle<()> = tokio::spawn(async move {
        pipeline::regime::run_regime_task(
            us_regime_client,
            us_regime_strategy,
            regime_tx,
            us_regime_alert,
            t,
        )
        .await;
    });

    let us_pending = Arc::new(AtomicU32::new(0));

    let us_tick_rx = us.tick_tx.subscribe();
    let us_order_tx = us.order_tx.clone();
    let us_signal_client = Arc::clone(&kis_client);
    let us_watchlist_rx2 = us.watchlist_rx.clone();
    let us_signal_alert = us.alert.clone();
    let us_signal_db = us_db_pool.clone();
    let us_signal_pending = Arc::clone(&us_pending);
    let us_risk_cfg = cfg.risk.clone();
    let us_signal_cfg = cfg.signal.clone();
    let us_signal_activity = activity.clone();
    let us_strategies = cfg.us.strategies.clone();
    let us_signal_notion = notion_client.clone();
    let us_live_state_rx = us_state.live_state_rx.clone();
    let us_signal_strategy = Arc::clone(&signal_strategy);
    let us_qual_strategy = Arc::clone(&qual_strategy);
    let us_risk_strategy = Arc::clone(&risk_strategy);
    let t = token.clone();
    let h_us_signal: JoinHandle<()> = tokio::spawn(async move {
        pipeline::signal::run_signal_task(
            us_tick_rx,
            us_quote_rx,
            us_order_tx,
            regime_rx,
            us_signal_db,
            us_signal_client,
            us_watchlist_rx2,
            us_signal_summary,
            us_signal_alert,
            us_signal_pending,
            us_risk_cfg,
            us_signal_cfg,
            us_strategies,
            us_signal_activity,
            us_signal_notion,
            us_live_state_rx,
            us_signal_strategy,
            us_qual_strategy,
            us_risk_strategy,
            t,
        )
        .await;
    });

    let us_order_rx = us.order_rx;
    let us_force_rx = us.force_order_rx;
    let us_fill_tx = us.fill_tx.clone();
    let us_exec_client = Arc::clone(&kis_client);
    let us_exec_alert = us.alert.clone();
    let us_exec_db = us_db_pool.clone();
    let t = token.clone();
    let us_poll_sem = Arc::clone(&poll_sem);
    let h_us_exec: JoinHandle<()> = tokio::spawn(async move {
        pipeline::execution::run_execution_task(
            us_order_rx,
            us_force_rx,
            us_fill_tx,
            us_exec_client,
            us_exec_db,
            us_summary2,
            us_exec_alert,
            t,
            us_pending,
            us_poll_sem,
        )
        .await;
    });

    let us_fill_rx = us.fill_rx;
    let us_tick_pos_rx = us.tick_pos_rx;
    let us_eod_rx = us
        .eod_rx
        .take()
        .ok_or_else(|| anyhow::anyhow!("US eod_rx already taken"))?;
    let us_force_order_tx2 = us.force_order_tx.clone();
    let us_pos_alert = us.alert.clone();
    let us_pos_summary = us.summary_alert.clone();
    let us_pos_db = us_db_pool.clone();
    let t = token.clone();
    let us_pos_cfg = cfg.position.clone();
    let us_signal_cfg_pos = cfg.signal.clone();
    let us_notion = notion_client.clone();
    let us_tunable_tx = Some(Arc::clone(&tunable_tx));
    let us_dry_run = us_effective_dry_run;
    let us_eod_fallback = pipeline::position::market_close_utc(16, 0, chrono_tz::America::New_York);
    let h_us_pos: JoinHandle<()> = tokio::spawn(async move {
        pipeline::position::run_position_task(
            us_fill_rx,
            us_tick_pos_rx,
            us_eod_rx,
            us_live_tx,
            us_force_order_tx2,
            regime_rx_pos,
            us_pos_db,
            us_pos_alert,
            us_pos_summary,
            t,
            us_eod_fallback,
            "US".to_string(),
            us_pos_cfg,
            us_notion,
            us_tunable_tx,
            us_signal_cfg_pos,
            us_dry_run,
        )
        .await;
    });

    // SchedulerTask (KR)
    let kr_watchlist_tx = kr.watchlist_tx;
    let kr_eod_tx = kr
        .eod_tx
        .take()
        .ok_or_else(|| anyhow::anyhow!("KR eod_tx already taken"))?;
    let kr_sched_client = Arc::clone(&kr_client);
    let kr_sched_alert = kr.alert.clone();
    let kr_sched_summary = kr.summary_alert.clone();
    let kr_cfg = cfg.kr.clone();
    let kr_sched_activity = activity.clone();
    let kr_sched_db = kr_db_pool.clone();
    let kr_discovery_strategy = Arc::clone(&discovery_strategy);
    let t = token.clone();
    let h_kr_sched: JoinHandle<()> = tokio::spawn(async move {
        pipeline::scheduler::run_kr_scheduler_task(
            kr_sched_client,
            kr_discovery_strategy,
            kr_watchlist_tx,
            kr_eod_tx,
            kr_cfg,
            kr_sched_alert,
            kr_sched_summary,
            kr_sched_activity,
            kr_sched_db,
            t,
        )
        .await;
    });

    // SchedulerTask (US)
    let us_watchlist_tx = us.watchlist_tx;
    let us_eod_tx = us
        .eod_tx
        .take()
        .ok_or_else(|| anyhow::anyhow!("US eod_tx already taken"))?;
    let us_sched_alert = us.alert.clone();
    let us_sched_summary = us.summary_alert.clone();
    let us_cfg = cfg.us.clone();
    let us_sched_client = Arc::clone(&kis_client);
    let us_sched_activity = activity.clone();
    let us_sched_db = us_db_pool.clone();
    let us_discovery_strategy = Arc::clone(&discovery_strategy);
    let t = token.clone();
    let h_us_sched: JoinHandle<()> = tokio::spawn(async move {
        pipeline::scheduler::run_scheduler_task(
            us_sched_client,
            us_discovery_strategy,
            us_watchlist_tx,
            us_eod_tx,
            us_cfg,
            us_sched_alert,
            us_sched_summary,
            us_sched_activity,
            us_sched_db,
            t,
        )
        .await;
    });

    let kr_shutdown_alert = kr.summary_alert.clone();
    let kr_ready_alert = kr.summary_alert.clone();

    kr_ready_alert.info(format!(
        "✅ 모든 태스크 준비 완료 [{}] — 장 시작 대기 중",
        mode
    ));

    // Wait for Ctrl+C
    tokio::signal::ctrl_c().await?;
    tracing::info!("Received Ctrl+C — initiating graceful shutdown");

    crate::notion::spawn_system_event(
        &notion_client,
        crate::notion::SystemEventRow {
            timestamp: chrono::Utc::now().to_rfc3339(),
            event: "BotShutdown".to_string(),
            market: "ALL".to_string(),
            mode: mode.to_string(),
            detail: "Graceful shutdown initiated (Ctrl+C)".to_string(),
        },
    );

    kr_shutdown_alert.info("🛑 봇 종료 신호 수신 — Graceful Shutdown 진행 중...".to_string());
    tokio::time::sleep(std::time::Duration::from_millis(2000)).await;

    token.cancel();

    // Graceful Shutdown
    let _ = h_kr_sched.await;
    let _ = h_us_sched.await;
    let _ = h_kr_regime.await;
    let _ = h_us_regime.await;
    let _ = h_kr_tick.await;
    let _ = h_us_tick.await;
    let _ = h_kr_signal.await;
    let _ = h_us_signal.await;
    let _ = h_kr_pos.await;
    let _ = h_us_pos.await;
    let _ = h_kr_exec.await;
    let _ = h_us_exec.await;
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
