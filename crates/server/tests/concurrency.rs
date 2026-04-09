use kis_server::shared::control::run_control_task;
use kis_server::state::{BotState, MarketSummary};
use kis_server::types::BotCommand;
use std::sync::{Arc, RwLock};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

fn temp_ks_path() -> String {
    let dir = tempfile::tempdir().unwrap();
    let path = dir
        .path()
        .join("test_ks.json")
        .to_str()
        .unwrap()
        .to_string();
    std::mem::forget(dir);
    path
}

/// KR + US ControlTask가 독립적으로 상태 전이를 수행하는지
#[tokio::test]
async fn kr_and_us_control_tasks_are_independent() {
    let kr_summary = Arc::new(RwLock::new(MarketSummary::new()));
    let us_summary = Arc::new(RwLock::new(MarketSummary::new()));

    let (kr_cmd_tx, kr_cmd_rx) = mpsc::channel(8);
    let (us_cmd_tx, us_cmd_rx) = mpsc::channel(8);
    let (kr_force_tx, _) = mpsc::channel(8);
    let (us_force_tx, _) = mpsc::channel(8);

    let token = CancellationToken::new();

    let kr_s = kr_summary.clone();
    let t = token.clone();
    tokio::spawn(
        async move { run_control_task(kr_cmd_rx, kr_force_tx, kr_s, temp_ks_path(), t).await },
    );

    let us_s = us_summary.clone();
    let t = token.clone();
    tokio::spawn(
        async move { run_control_task(us_cmd_rx, us_force_tx, us_s, temp_ks_path(), t).await },
    );

    // KR은 Active, US는 Suspended
    kr_cmd_tx.send(BotCommand::Start).await.unwrap();
    us_cmd_tx.send(BotCommand::Stop).await.unwrap();

    tokio::time::sleep(std::time::Duration::from_millis(30)).await;

    assert_eq!(kr_summary.read().unwrap().bot_state, BotState::Active);
    assert_eq!(us_summary.read().unwrap().bot_state, BotState::Suspended);

    token.cancel();
}

/// 동일 명령을 여러 번 보내도 상태가 멱등적인지
#[tokio::test]
async fn stop_command_is_idempotent() {
    let summary = Arc::new(RwLock::new(MarketSummary::new()));
    let (cmd_tx, cmd_rx) = mpsc::channel(8);
    let (force_tx, _) = mpsc::channel(8);
    let token = CancellationToken::new();
    let s = summary.clone();
    let t = token.clone();
    tokio::spawn(async move { run_control_task(cmd_rx, force_tx, s, temp_ks_path(), t).await });

    for _ in 0..5 {
        cmd_tx.send(BotCommand::Stop).await.unwrap();
    }
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    assert_eq!(summary.read().unwrap().bot_state, BotState::Suspended);
    token.cancel();
}
