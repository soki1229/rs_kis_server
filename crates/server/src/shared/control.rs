use crate::state::{BotState, MarketSummary};
use crate::types::{BotCommand, OrderRequest};
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub async fn run_control_task(
    mut cmd_rx: mpsc::Receiver<BotCommand>,
    force_order_tx: mpsc::Sender<OrderRequest>,
    summary: Arc<RwLock<MarketSummary>>,
    kill_switch_path: String,
    token: CancellationToken,
) {
    let ks = crate::control::kill_switch::KillSwitch::new(kill_switch_path);
    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("ControlTask: shutting down");
                break;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    None => break,
                    Some(c) => handle_command(c, &force_order_tx, &summary, &ks).await,
                }
            }
        }
    }
}

async fn handle_command(
    cmd: BotCommand,
    force_order_tx: &mpsc::Sender<OrderRequest>,
    summary: &Arc<RwLock<MarketSummary>>,
    ks: &crate::control::kill_switch::KillSwitch,
) {
    match cmd {
        BotCommand::Start => {
            summary.write().unwrap().bot_state = BotState::Active;
            tracing::info!("ControlTask: → Active");
        }
        BotCommand::Stop => {
            summary.write().unwrap().bot_state = BotState::Suspended;
            tracing::info!("ControlTask: → Suspended");
        }
        BotCommand::LiquidateAll => {
            summary.write().unwrap().bot_state = BotState::Suspended;
            tracing::warn!("ControlTask: → Suspended + liquidation requested");
            // Force-sell all positions by sending a liquidate signal
            // The PositionTask handles actual position closure via force_order_tx
        }
        BotCommand::Pause => {
            summary.write().unwrap().bot_state = BotState::EntryPaused;
            tracing::info!("ControlTask: → EntryPaused");
        }
        BotCommand::ForceOrder(req) => {
            if let Err(e) = force_order_tx.send(req).await {
                tracing::error!("ControlTask: force_order_tx send failed: {}", e);
            }
        }
        BotCommand::QueryStatus => {
            tracing::info!("ControlTask: status query received");
        }
        BotCommand::KillHard => {
            if let Err(e) = ks.activate(
                crate::types::KillSwitchMode::Hard,
                "Telegram /kill-hard command",
                "Operator-initiated hard kill switch",
            ) {
                tracing::error!("ControlTask: KillSwitch activate failed: {}", e);
            }
            summary.write().unwrap().bot_state = BotState::HardBlocked;
            summary.write().unwrap().kill_switch = Some("Hard".to_string());
            tracing::warn!("ControlTask: → HardBlocked (kill switch activated)");
        }
        BotCommand::KillSoft => {
            if let Err(e) = ks.activate(
                crate::types::KillSwitchMode::Soft,
                "Telegram /kill-soft command",
                "Operator-initiated soft kill switch",
            ) {
                tracing::error!("ControlTask: KillSwitch activate failed: {}", e);
            }
            summary.write().unwrap().bot_state = BotState::EntryPaused;
            summary.write().unwrap().kill_switch = Some("Soft".to_string());
            tracing::warn!("ControlTask: → EntryPaused (soft kill switch activated)");
        }
        BotCommand::KillClear => {
            if let Err(e) = ks.clear() {
                tracing::error!("ControlTask: KillSwitch clear failed: {}", e);
            }
            summary.write().unwrap().bot_state = BotState::Suspended;
            summary.write().unwrap().kill_switch = None;
            tracing::info!("ControlTask: kill switch cleared → Suspended (use /kr start or /us start to resume)");
        }
        BotCommand::SetRiskLimit(_) => {
            tracing::info!("ControlTask: SetRiskLimit received (not yet implemented)");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{BotState, MarketSummary};
    use crate::types::BotCommand;
    use std::sync::{Arc, RwLock};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn make_state() -> (Arc<RwLock<MarketSummary>>, CancellationToken, String) {
        let dir = tempfile::tempdir().unwrap();
        let ks_path = dir
            .path()
            .join("test_ks.json")
            .to_str()
            .unwrap()
            .to_string();
        // Leak the tempdir so it isn't deleted during test
        std::mem::forget(dir);
        (
            Arc::new(RwLock::new(MarketSummary::new())),
            CancellationToken::new(),
            ks_path,
        )
    }

    #[tokio::test]
    async fn start_sets_active() {
        let (summary, token, ks_path) = make_state();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (force_tx, _force_rx) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });

        cmd_tx.send(BotCommand::Start).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(summary.read().unwrap().bot_state, BotState::Active);
    }

    #[tokio::test]
    async fn stop_sets_suspended() {
        let (summary, token, ks_path) = make_state();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (force_tx, _force_rx) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });

        cmd_tx.send(BotCommand::Stop).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(summary.read().unwrap().bot_state, BotState::Suspended);
    }

    #[tokio::test]
    async fn force_order_forwarded_to_force_tx() {
        let (summary, token, ks_path) = make_state();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (force_tx, mut force_rx) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });

        let req = crate::types::OrderRequest {
            symbol: "NVDA".into(),
            side: crate::types::Side::Buy,
            qty: 1,
            price: None,
            atr: None,
            exchange_code: None,
            strength: None,
            is_short: false,
            max_holding_days: 5,
            strategy_id: "stable".to_string(),
        };
        cmd_tx
            .send(BotCommand::ForceOrder(req.clone()))
            .await
            .unwrap();
        let received: crate::types::OrderRequest =
            tokio::time::timeout(std::time::Duration::from_millis(50), force_rx.recv())
                .await
                .unwrap()
                .unwrap();
        assert_eq!(received.symbol, "NVDA");
    }

    #[tokio::test]
    async fn cancellation_stops_task() {
        let (summary, token, ks_path) = make_state();
        let (_cmd_tx, cmd_rx) = mpsc::channel::<BotCommand>(8);
        let (force_tx, _force_rx) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        let handle =
            tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(100), handle)
            .await
            .expect("task should exit after cancellation")
            .unwrap();
    }

    #[tokio::test]
    async fn kill_hard_sets_hard_blocked() {
        let (summary, token, ks_path) = make_state();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (force_tx, _force_rx) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });

        cmd_tx.send(BotCommand::KillHard).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        assert_eq!(summary.read().unwrap().bot_state, BotState::HardBlocked);
    }

    /// KR + US ControlTask가 독립적으로 상태 전이를 수행하는지
    #[tokio::test]
    async fn kr_and_us_control_tasks_are_independent() {
        let (kr_summary, token_kr, kr_ks) = make_state();
        let (us_summary, token_us, us_ks) = make_state();

        let (kr_cmd_tx, kr_cmd_rx) = mpsc::channel(8);
        let (us_cmd_tx, us_cmd_rx) = mpsc::channel(8);
        let (kr_force_tx, _) = mpsc::channel(8);
        let (us_force_tx, _) = mpsc::channel(8);

        let t_kr = token_kr.clone();
        let s_kr = kr_summary.clone();
        tokio::spawn(
            async move { run_control_task(kr_cmd_rx, kr_force_tx, s_kr, kr_ks, t_kr).await },
        );

        let t_us = token_us.clone();
        let s_us = us_summary.clone();
        tokio::spawn(
            async move { run_control_task(us_cmd_rx, us_force_tx, s_us, us_ks, t_us).await },
        );

        // KR은 Active, US는 Suspended
        kr_cmd_tx.send(BotCommand::Start).await.unwrap();
        us_cmd_tx.send(BotCommand::Stop).await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(30)).await;

        assert_eq!(kr_summary.read().unwrap().bot_state, BotState::Active);
        assert_eq!(us_summary.read().unwrap().bot_state, BotState::Suspended);

        token_kr.cancel();
        token_us.cancel();
    }

    /// 동일 명령을 여러 번 보내도 상태가 멱등적인지
    #[tokio::test]
    async fn stop_command_is_idempotent() {
        let (summary, token, ks_path) = make_state();
        let (cmd_tx, cmd_rx) = mpsc::channel(8);
        let (force_tx, _) = mpsc::channel(8);
        let s = summary.clone();
        let t = token.clone();
        tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, ks_path, t).await });

        for _ in 0..5 {
            cmd_tx.send(BotCommand::Stop).await.unwrap();
        }
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        assert_eq!(summary.read().unwrap().bot_state, BotState::Suspended);
        token.cancel();
    }
}
