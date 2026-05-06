#[cfg(test)]
use crate::monitoring::alert::AlertRouter;
use crate::monitoring::alert::AlertSeverity;
use crate::shared::activity::ActivityLog;
use crate::state::{BotState, PipelineState};
use crate::types::{BotCommand, Market};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

/// 텍스트에서 (Market, BotCommand) 파싱.
pub fn parse_command(text: &str) -> Option<(Market, BotCommand)> {
    let parts: Vec<&str> = text.splitn(3, ' ').collect();
    let market = match *parts.first()? {
        "/kr" => Market::Kr,
        "/us" => Market::Us,
        _ => return None,
    };

    let cmd = match *parts.get(1)? {
        "stop" => BotCommand::Stop,
        "start" => BotCommand::Start,
        "pause" => BotCommand::Pause,
        "status" => BotCommand::QueryStatus,
        _ => return None,
    };
    Some((market, cmd))
}

pub fn parse_stop_all(text: &str) -> bool {
    text.trim() == "/stop-all"
}

/// Parse global kill switch commands.
pub fn parse_global_command(text: &str) -> Option<BotCommand> {
    match text.trim() {
        "/kill-hard" => Some(BotCommand::KillHard),
        "/kill-soft" => Some(BotCommand::KillSoft),
        "/kill-clear" => Some(BotCommand::KillClear),
        "/liquidate-all" => Some(BotCommand::LiquidateAll),
        _ => None,
    }
}

/// Send a critical alert with retry (up to 3 attempts with exponential backoff).
async fn send_critical_with_retry(bot: &Bot, chat_id: ChatId, text: &str) -> bool {
    for attempt in 0..3u32 {
        match bot.send_message(chat_id, text).await {
            Ok(_) => return true,
            Err(e) => {
                tracing::error!(
                    "Critical alert send failed (attempt {}/3): {}",
                    attempt + 1,
                    e
                );
                tokio::time::sleep(std::time::Duration::from_secs(2u64.pow(attempt))).await;
            }
        }
    }
    tracing::error!("CRITICAL ALERT PERMANENTLY LOST: {}", text);
    false
}

/// AlertTask 발송 대상 — 기술적 상세 이슈 (AlertBot 수신).
/// Critical: 서킷브레이커, HardBlocked, 복구 실패
/// Warn: API 실패, WebSocket 재연결, 타임아웃
/// Info: WebSocket 연결 성공, 진입/청산 상세, 기타 기술적 이벤트
pub fn should_send_to_alert(severity: &AlertSeverity) -> bool {
    matches!(
        severity,
        AlertSeverity::Critical | AlertSeverity::Warn | AlertSeverity::Info
    )
}

/// MonitorTask 발송 대상 — 운영자용 핵심 요약 (MonitorBot 수신).
/// summary_alert 채널에 쌓인 메시지만 수신 — 모두 Info severity.
/// 시작/종료, 장 오픈/마감, 워치리스트 준비, 일일 리포트
pub fn should_send_to_monitor(severity: &AlertSeverity) -> bool {
    matches!(severity, AlertSeverity::Info)
}

/// 장 상태 (open/closed + 남은/경과 시간) 문자열 반환.
fn market_hours_str(market: &str) -> String {
    use chrono::{Datelike, Timelike, Weekday};
    use chrono_tz::{America::New_York, Asia::Seoul};
    let now = chrono::Utc::now();

    match market {
        "KR" => {
            let local = now.with_timezone(&Seoul);
            let dow = local.weekday();
            let mins = local.hour() * 60 + local.minute();
            let open = 9 * 60;
            let close = 15 * 60 + 30;
            if matches!(dow, Weekday::Sat | Weekday::Sun) {
                return "📴 주말 휴장".to_string();
            }
            if mins >= open && mins < close {
                let left = close - mins;
                format!("🟢 장중 ({}h {}m 남음)", left / 60, left % 60)
            } else if mins < open {
                let until = open - mins;
                format!("⏰ 개장까지 {}h {}m", until / 60, until % 60)
            } else {
                "🔴 장마감".to_string()
            }
        }
        "US" => {
            let local = now.with_timezone(&New_York);
            let dow = local.weekday();
            let mins = local.hour() * 60 + local.minute();
            let open = 9 * 60 + 30;
            let close = 16 * 60;
            if matches!(dow, Weekday::Sat | Weekday::Sun) {
                return "📴 주말 휴장".to_string();
            }
            if mins >= open && mins < close {
                let left = close - mins;
                format!("🟢 장중 ({}h {}m 남음)", left / 60, left % 60)
            } else if mins < open {
                let until = open - mins;
                format!("⏰ 개장까지 {}h {}m", until / 60, until % 60)
            } else {
                let until = (24 * 60 - mins) + open;
                format!("🔴 장마감 (내일 개장까지 {}h {}m)", until / 60, until % 60)
            }
        }
        _ => "알 수 없음".to_string(),
    }
}

/// `/kr status` 또는 `/us status` 응답 문자열 생성.
pub fn format_status(state: &PipelineState, market: &str) -> String {
    use rust_decimal::prelude::ToPrimitive;
    let live = state.live_state_rx.borrow();
    let summary = state.summary.read().unwrap_or_else(|e| e.into_inner());

    let bot_icon = match summary.bot_state {
        BotState::Active => "🟢 운용 중",
        BotState::Idle => "⚪️ 대기",
        BotState::EntryPaused | BotState::EntryPausedLlmOutage => "🔵 청산 모드",
        BotState::Suspended => "🟡 정지",
        BotState::HardBlocked => "🔴 긴급 차단",
    };

    let regime_icon = match live.regime.as_str() {
        "Trending" => "📈",
        "Volatile" => "⚡",
        "Quiet" => "😴",
        _ => "•",
    };

    let total_unrealized: f64 = live
        .positions
        .iter()
        .map(|p| p.unrealized_pnl.to_f64().unwrap_or(0.0))
        .sum();
    let unrealized_icon = if total_unrealized >= 0.0 {
        "▲"
    } else {
        "▼"
    };

    let updated_str = match live.last_updated {
        Some(t) => {
            let ago = (chrono::Utc::now() - t).num_seconds().max(0);
            if ago < 60 {
                format!("{}초 전", ago)
            } else {
                format!("{}분 전", ago / 60)
            }
        }
        None => "미발행".to_string(),
    };

    let cash_str = match live.available_cash {
        Some(c) => format!(" | 가용: {:.0}", c),
        None => String::new(),
    };

    let regime_str = if live.regime.is_empty() {
        "미분류".to_string()
    } else {
        live.regime.clone()
    };

    let pos_count = live.positions.len();
    let pos_section = if live.positions.is_empty() {
        "  포지션 없음".to_string()
    } else {
        live.positions
            .iter()
            .map(format_position_line)
            .collect::<Vec<_>>()
            .join("\n")
    };

    let market_hours = market_hours_str(market);

    // 총 손익 색상
    let total_icon = if total_unrealized >= 0.0 {
        "🔵"
    } else {
        "🔴"
    };
    let total_sign = if total_unrealized >= 0.0 { "+" } else { "" };

    format!(
        "┌─ {market} | {bot_icon}\n\
         │ {market_hours}\n\
         │ {regime_icon} {regime_str} | {total_icon} {total_sign}{total_unrealized:.0}원{cash_str}\n\
         │ 🕐 {updated_str}\n\
         └─ 📦 {pos_count}종목\n\
         {pos_section}"
    )
}

fn format_position_line(p: &crate::types::Position) -> String {
    use rust_decimal::prelude::ToPrimitive;
    let display = match &p.name {
        Some(n) => format!("{}({})", n, p.symbol),
        None => p.symbol.clone(),
    };

    // 손익 색상: 🟢 +2%↑, 🔵 +0~2%, ⚪ 보합, 🔴 손실
    let pnl_icon = if p.pnl_pct >= 2.0 {
        "🟢"
    } else if p.pnl_pct >= 0.0 {
        "🔵"
    } else {
        "🔴"
    };
    let pnl_sign = if p.pnl_pct >= 0.0 { "+" } else { "" };
    let pnl_amt = p.unrealized_pnl.to_i64().unwrap_or(0);
    let pnl_amt_sign = if pnl_amt >= 0 { "+" } else { "" };

    let live_price = if p.current_price == p.avg_price {
        format!("{} (미수신)", p.avg_price)
    } else {
        format!("{}", p.current_price)
    };

    // 스탑까지 거리 (%)
    let stop_dist_pct = if p.current_price > rust_decimal::Decimal::ZERO {
        ((p.stop_price - p.current_price) / p.current_price * rust_decimal::Decimal::from(100))
            .to_f64()
            .unwrap_or(0.0)
    } else {
        0.0
    };

    // TrailingStop 또는 고정스탑
    let stop_label = if let Some(ts) = p.trailing_stop {
        format!("TS {ts} ({stop_dist_pct:.1}%)")
    } else {
        format!("✂ {} ({stop_dist_pct:.1}%)", p.stop_price)
    };

    format!(
        "{pnl_icon} {display} {qty}주\n  {avg} → {live}  {pnl_sign}{pnl_pct:.2}% / {pnl_amt_sign}{pnl_amt:+}원\n  {stop} | 🎯 {pt1} / {pt2}",
        qty = p.qty,
        avg = p.avg_price,
        live = live_price,
        pnl_pct = p.pnl_pct,
        pt1 = p.profit_target_1,
        pt2 = p.profit_target_2,
        stop = stop_label,
    )
}

/// `/positions` 응답 문자열 생성.
pub fn format_positions(state: &PipelineState, market: &str) -> String {
    use rust_decimal::prelude::ToPrimitive;
    let live = state.live_state_rx.borrow();

    let updated_str = match live.last_updated {
        Some(t) => {
            let ago = (chrono::Utc::now() - t).num_seconds().max(0);
            if ago < 60 {
                format!("{}초 전", ago)
            } else {
                format!("{}분 전", ago / 60)
            }
        }
        None => "미발행".to_string(),
    };

    if live.positions.is_empty() {
        return format!("📦 [{market}] 보유 중인 포지션이 없습니다.\n갱신: {updated_str}");
    }

    let total_unrealized: f64 = live
        .positions
        .iter()
        .map(|p| p.unrealized_pnl.to_f64().unwrap_or(0.0))
        .sum();
    let unrealized_icon = if total_unrealized >= 0.0 {
        "▲"
    } else {
        "▼"
    };

    let lines: Vec<String> = live.positions.iter().map(format_position_line).collect();

    format!(
        "💰 [{market} 포지션 현황] {}종목 | 미실현 {unrealized_icon} {total_unrealized:.0} | 갱신: {updated_str}\n\n{}",
        live.positions.len(),
        lines.join("\n\n")
    )
}

use crate::monitoring::alert::AlertMessage;
use teloxide::prelude::*;
use teloxide::types::ChatId;
use tokio::sync::broadcast::{self, error::RecvError};

/// severity 필터 함수를 받아 해당하는 메시지만 chat_id로 전송하는 공용 루프.
async fn run_filtered_alert_loop(
    bot: Bot,
    chat_id: ChatId,
    mut kr_rx: broadcast::Receiver<AlertMessage>,
    mut us_rx: broadcast::Receiver<AlertMessage>,
    filter: fn(&AlertSeverity) -> bool,
    token: CancellationToken,
) {
    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            result = kr_rx.recv() => {
                if matches!(result, Err(RecvError::Closed)) { break; }
                handle_alert(&bot, chat_id, "KR", result, filter).await;
            }
            result = us_rx.recv() => {
                if matches!(result, Err(RecvError::Closed)) { break; }
                handle_alert(&bot, chat_id, "US", result, filter).await;
            }
        }
    }
}

async fn handle_alert(
    bot: &Bot,
    chat_id: ChatId,
    market: &str,
    result: Result<AlertMessage, RecvError>,
    filter: fn(&AlertSeverity) -> bool,
) {
    match result {
        Ok(event) => {
            if filter(&event.severity) {
                let text = format!("[{}] {}", market, event.message);
                match event.severity {
                    AlertSeverity::Critical | AlertSeverity::Warn => {
                        send_critical_with_retry(bot, chat_id, &text).await;
                    }
                    _ => {
                        let _ = bot.send_message(chat_id, text).await;
                    }
                }
            }
        }
        Err(RecvError::Lagged(n)) => {
            tracing::warn!(
                "TelegramTask: {} alert lagged, {} messages dropped",
                market,
                n
            );
        }
        Err(RecvError::Closed) => {
            tracing::debug!("TelegramTask: {} alert channel closed", market);
        }
    }
}

use teloxide::dispatching::UpdateFilterExt;
use teloxide::types::{Message, Update};

#[allow(clippy::too_many_arguments)]
async fn run_command_loop(
    bot: Bot,
    chat_id_whitelist: Vec<i64>,
    kr_cmd_tx: mpsc::Sender<BotCommand>,
    us_cmd_tx: mpsc::Sender<BotCommand>,
    kr_state: PipelineState,
    us_state: PipelineState,
    activity: ActivityLog,
    token: CancellationToken,
) {
    let handler = Update::filter_message().endpoint(move |bot: Bot, msg: Message| {
        let whitelist = chat_id_whitelist.clone();
        let kr_tx = kr_cmd_tx.clone();
        let us_tx = us_cmd_tx.clone();
        let kr_st = kr_state.clone();
        let us_st = us_state.clone();
        let act = activity.clone();
        async move {
            if !whitelist.contains(&msg.chat.id.0) {
                return Ok::<(), teloxide::RequestError>(());
            }

            let text = msg.text().unwrap_or("");

            if text.trim() == "/status" {
                let kr = format_status(&kr_st, "KR");
                let us = format_status(&us_st, "US");
                let reply = format!("{}\n\n{}", kr, us);
                bot.send_message(msg.chat.id, reply).await?;
                return Ok(());
            }

            if text.trim() == "/botstat" {
                let reply = act.format_status();
                bot.send_message(msg.chat.id, reply).await?;
                return Ok(());
            }

            if text.trim() == "/log" || text.trim().starts_with("/log ") {
                let count: usize = text
                    .trim()
                    .strip_prefix("/log")
                    .and_then(|s| s.trim().parse().ok())
                    .unwrap_or(15);
                let reply = act.format_log(count);
                bot.send_message(msg.chat.id, reply).await?;
                return Ok(());
            }

            if text.trim() == "/positions" {
                let kr = format_positions(&kr_st, "KR");
                let us = format_positions(&us_st, "US");
                bot.send_message(msg.chat.id, format!("{}\n{}", kr, us))
                    .await?;
                return Ok(());
            }

            if text.trim().starts_with("/kr watchlist") || text.trim().starts_with("/us watchlist")
            {
                let market = if text.trim().starts_with("/kr") {
                    "KR"
                } else {
                    "US"
                };
                let reply = act.format_watchlist_for_market(market);
                bot.send_message(msg.chat.id, reply).await?;
                return Ok(());
            }

            if text.trim().starts_with("/kr signals") || text.trim().starts_with("/us signals") {
                let market = if text.trim().starts_with("/kr") {
                    "KR"
                } else {
                    "US"
                };
                let count: usize = text
                    .split_whitespace()
                    .nth(2)
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(15);
                let reply = act.format_signals_for_market(market, count);
                bot.send_message(msg.chat.id, reply).await?;
                return Ok(());
            }

            if text.trim() == "/help" {
                let help = "📖 사용 가능한 명령\n\n\
                    [조회]\n\
                    /status — KR+US 현황 (장상태, 레짐, 포지션, 손익)\n\
                    /positions — 보유 포지션 상세\n\
                    /kr status — KR 상태\n\
                    /us status — US 상태\n\
                    /kr watchlist — KR 감시 종목\n\
                    /us watchlist — US 감시 종목\n\
                    /kr signals [N] — KR 최근 시그널 (기본 15건)\n\
                    /us signals [N] — US 최근 시그널 (기본 15건)\n\
                    /log [N] — 최근 활동 로그\n\
                    /botstat — 봇 통계 (uptime, tick수, 주문수)\n\n\
                    [제어]\n\
                    /kr start|stop|pause — KR 봇 제어\n\
                    /us start|stop|pause — US 봇 제어\n\
                    /stop-all — 전체 중단 + 청산\n\
                    /kill-hard — 긴급 차단\n\
                    /kill-soft — 소프트 차단\n\
                    /kill-clear — 차단 해제\n\
                    /liquidate-all — 전체 포지션 청산";
                bot.send_message(msg.chat.id, help).await?;
                return Ok(());
            }

            if parse_stop_all(text) {
                let _ = kr_tx.send(BotCommand::LiquidateAll).await;
                let _ = us_tx.send(BotCommand::LiquidateAll).await;
                bot.send_message(msg.chat.id, "KR + US 봇 중단 + 포지션 청산 요청")
                    .await?;
                return Ok(());
            }

            if let Some(cmd) = parse_global_command(text) {
                let label = format!("{:?}", cmd);
                let _ = kr_tx.send(cmd.clone()).await;
                let _ = us_tx.send(cmd).await;
                bot.send_message(msg.chat.id, format!("KR + US: {} 명령 전달됨", label))
                    .await?;
                return Ok(());
            }

            match parse_command(text) {
                Some((Market::Kr, BotCommand::QueryStatus)) => {
                    let reply = format_status(&kr_st, "KR");
                    bot.send_message(msg.chat.id, reply).await?;
                }
                Some((Market::Us, BotCommand::QueryStatus)) => {
                    let reply = format_status(&us_st, "US");
                    bot.send_message(msg.chat.id, reply).await?;
                }
                Some((Market::Kr, cmd)) => {
                    let _ = kr_tx.send(cmd).await;
                    bot.send_message(msg.chat.id, "KR 명령 전달됨").await?;
                }
                Some((Market::Us, cmd)) => {
                    let _ = us_tx.send(cmd).await;
                    bot.send_message(msg.chat.id, "US 명령 전달됨").await?;
                }
                None => {
                    bot.send_message(msg.chat.id, "알 수 없는 명령\n/help 로 도움말 확인")
                        .await?;
                }
            }
            Ok(())
        }
    });

    let mut dispatcher = Dispatcher::builder(bot, handler).build();
    let shutdown = dispatcher.shutdown_token();

    tokio::select! {
        _ = token.cancelled() => {
            let _ = shutdown.shutdown();
            tracing::info!("TelegramTask: command loop shutdown requested");
        }
        _ = dispatcher.dispatch() => {
            tracing::warn!("TelegramTask: command loop dispatcher exited");
        }
    }
}

/// AlertTask — Critical / Warn 전용 봇 (즉각 대응 필요 이슈).
/// 에러, HardBlocked, 서킷브레이커, API 실패, WebSocket 재연결 등.
///
/// 호출자가 subscribe()를 스폰 이전에 직접 호출하여 receivers를 넘겨야 한다.
/// broadcast::Receiver는 생성 이후 전송된 메시지만 수신하므로
/// spawn 전에 subscribe해야 초기 알림이 유실되지 않는다.
pub async fn run_alert_task_with_rx(
    alert_bot_token: String,
    alert_chat_id: i64,
    kr_rx: broadcast::Receiver<AlertMessage>,
    us_rx: broadcast::Receiver<AlertMessage>,
    token: CancellationToken,
) {
    let bot = Bot::new(alert_bot_token);
    let chat_id = ChatId(alert_chat_id);

    tokio::select! {
        _ = token.cancelled() => {
            tracing::info!("AlertTask: shutting down");
        }
        _ = run_filtered_alert_loop(
            bot, chat_id, kr_rx, us_rx, should_send_to_alert, token.clone(),
        ) => {
            tracing::warn!("AlertTask: alert loop exited unexpectedly");
        }
    }
}

/// MonitorTask — Info 알림 수신 + 명령 처리 봇.
/// 시작/종료, 장 오픈 대기, watchlist 준비, 일일 리포트 등 운영 상황 Info 메시지와
/// /positions, /status, /kr stop 등의 명령을 monitor_chat_id에서 처리.
///
/// 호출자가 subscribe()를 스폰 이전에 직접 호출하여 receivers를 넘겨야 한다.
#[allow(clippy::too_many_arguments)]
pub async fn run_monitor_task(
    monitor_bot_token: String,
    monitor_chat_id: i64,
    kr_rx: broadcast::Receiver<AlertMessage>,
    us_rx: broadcast::Receiver<AlertMessage>,
    kr_cmd_tx: mpsc::Sender<BotCommand>,
    us_cmd_tx: mpsc::Sender<BotCommand>,
    kr_state: PipelineState,
    us_state: PipelineState,
    activity: ActivityLog,
    token: CancellationToken,
) {
    let bot = Bot::new(monitor_bot_token);
    let chat_id = ChatId(monitor_chat_id);

    // Clear any stale getUpdates session from a previous bot instance.
    // 10s timeout to avoid blocking on slow Telegram API.
    match tokio::time::timeout(
        std::time::Duration::from_secs(10),
        bot.delete_webhook().drop_pending_updates(true).send(),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!("MonitorTask: deleteWebhook failed (non-fatal): {e}"),
        Err(_) => tracing::warn!("MonitorTask: deleteWebhook timed out (10s), proceeding"),
    }

    // 봇 시작 알림
    let start_time = chrono::Utc::now()
        .with_timezone(&chrono_tz::Asia::Seoul)
        .format("%m/%d %H:%M")
        .to_string();
    let start_msg = format!("🚀 봇 시작됨 [{start_time} KST]\n/help 로 명령어 확인");
    let _ = bot.send_message(chat_id, &start_msg).await;

    let pnl_bot = bot.clone();
    let pnl_chat = chat_id;
    let pnl_kr = kr_state.clone();
    let pnl_us = us_state.clone();
    let pnl_token = token.clone();

    tokio::select! {
        _ = token.cancelled() => {
            tracing::info!("MonitorTask: shutting down");
        }
        _ = run_filtered_alert_loop(
            bot.clone(),
            chat_id,
            kr_rx,
            us_rx,
            should_send_to_monitor,
            token.clone(),
        ) => {
            tracing::warn!("MonitorTask: info loop exited unexpectedly");
        }
        _ = run_command_loop(
            bot,
            vec![monitor_chat_id],
            kr_cmd_tx,
            us_cmd_tx,
            kr_state,
            us_state,
            activity,
            token.clone(),
        ) => {
            tracing::warn!("MonitorTask: command loop exited unexpectedly");
        }
        _ = run_pnl_report_loop(pnl_bot, pnl_chat, pnl_kr, pnl_us, pnl_token) => {
            tracing::warn!("MonitorTask: PnL report loop exited unexpectedly");
        }
    }
}

/// 30분마다 장 중 포지션이 있을 때 수익 현황 자동 발송.
async fn run_pnl_report_loop(
    bot: Bot,
    chat_id: ChatId,
    kr_state: PipelineState,
    us_state: PipelineState,
    token: CancellationToken,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30 * 60));
    interval.tick().await; // 첫 틱은 즉시 발생 — 건너뜀

    loop {
        tokio::select! {
            _ = token.cancelled() => break,
            _ = interval.tick() => {
                let report = build_pnl_report(&kr_state, &us_state);
                if let Some(text) = report {
                    let _ = bot.send_message(chat_id, text).await;
                }
            }
        }
    }
}

fn build_pnl_report(kr_state: &PipelineState, us_state: &PipelineState) -> Option<String> {
    use rust_decimal::prelude::ToPrimitive;

    let kr_live = kr_state.live_state_rx.borrow();
    let us_live = us_state.live_state_rx.borrow();

    let kr_positions = &kr_live.positions;
    let us_positions = &us_live.positions;

    if kr_positions.is_empty() && us_positions.is_empty() {
        return None;
    }

    let now = chrono::Utc::now()
        .with_timezone(&chrono_tz::Asia::Seoul)
        .format("%H:%M")
        .to_string();

    let mut lines = vec![format!("📊 수익 현황 ({now} KST)")];

    for (positions, market) in [(kr_positions, "KR"), (us_positions, "US")] {
        if positions.is_empty() {
            continue;
        }
        let total: f64 = positions
            .iter()
            .map(|p| p.unrealized_pnl.to_f64().unwrap_or(0.0))
            .sum();
        let total_icon = if total >= 0.0 { "🔵" } else { "🔴" };
        let total_sign = if total >= 0.0 { "+" } else { "" };
        lines.push(format!("─ {market}: {total_icon} {total_sign}{total:.0}원"));
        for p in positions {
            let icon = if p.pnl_pct >= 2.0 {
                "🟢"
            } else if p.pnl_pct >= 0.0 {
                "🔵"
            } else {
                "🔴"
            };
            let sign = if p.pnl_pct >= 0.0 { "+" } else { "" };
            let name = p.name.as_deref().unwrap_or(&p.symbol);
            let amt = p.unrealized_pnl.to_i64().unwrap_or(0);
            let amt_sign = if amt >= 0 { "+" } else { "" };
            lines.push(format!(
                "  {icon} {name} {qty}주  {sign}{pct:.2}% / {amt_sign}{amt}원",
                qty = p.qty,
                pct = p.pnl_pct
            ));
        }
    }

    Some(lines.join("\n"))
}

/// AlertTask + MonitorTask stub — 테스트 전용, 취소 신호만 대기.
#[cfg(test)]
pub async fn run_alert_task_stub(
    _alert_bot_token: String,
    _alert_chat_id: i64,
    _kr_alert: AlertRouter,
    _us_alert: AlertRouter,
    token: CancellationToken,
) {
    token.cancelled().await;
    tracing::info!("AlertTask stub: shutting down");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::{MarketLiveState, PipelineConfig, PipelineState};
    use crate::types::Position;
    use rust_decimal_macros::dec;

    fn make_state_with_position(
        symbol: &str,
        qty: i64,
        price: rust_decimal::Decimal,
    ) -> PipelineState {
        let (sender, ps) = PipelineConfig::new(crate::shared::activity::ActivityLog::new()).build();
        sender
            .send(MarketLiveState {
                positions: vec![Position {
                    symbol: symbol.to_string(),
                    name: None,
                    qty,

                    avg_price: price,
                    current_price: price,
                    unrealized_pnl: rust_decimal::Decimal::ZERO,
                    pnl_pct: 0.0,
                    stop_price: rust_decimal::Decimal::ZERO,
                    trailing_stop: None,
                    profit_target_1: rust_decimal::Decimal::ZERO,
                    profit_target_2: rust_decimal::Decimal::ZERO,
                    regime: String::new(),
                }],
                daily_pnl_r: 1.5,
                regime: "Trending".into(),
                last_updated: Some(chrono::Utc::now()),
                available_cash: None,
            })
            .unwrap();
        ps
    }

    fn make_state_empty() -> PipelineState {
        PipelineState::new_for_test()
    }

    #[test]
    fn format_status_shows_market_label() {
        let state = make_state_empty();
        let result = format_status(&state, "KR");
        assert!(result.contains("KR"), "should contain market label");
    }

    #[test]
    fn format_status_shows_regime() {
        let state = make_state_with_position("NVDA", 1, dec!(130.00));
        let result = format_status(&state, "US");
        assert!(
            result.contains("Trending"),
            "should show regime: got {result}"
        );
    }

    #[test]
    fn format_positions_empty_returns_no_positions_message() {
        let state = make_state_empty();
        let result = format_positions(&state, "US");
        assert!(
            result.contains("보유 중인 포지션이 없습니다"),
            "should say no positions"
        );
    }

    #[test]
    fn format_positions_shows_symbol_qty_price() {
        let state = make_state_with_position("NVDA", 5, dec!(134.20));
        let result = format_positions(&state, "US");
        assert!(result.contains("NVDA"), "should contain symbol");
        assert!(result.contains("5"), "should contain qty");
        assert!(result.contains("134.20"), "should contain price");
    }

    #[test]
    fn parse_kr_stop() {
        let result = parse_command("/kr stop");
        assert_eq!(result, Some((Market::Kr, BotCommand::Stop)));
    }

    #[test]
    fn parse_us_status() {
        let result = parse_command("/us status");
        assert!(matches!(
            result,
            Some((Market::Us, BotCommand::QueryStatus))
        ));
    }

    #[test]
    fn parse_stop_all_command() {
        let result = parse_stop_all("/stop-all");
        assert!(result);
    }

    #[test]
    fn unknown_command_returns_none() {
        assert!(parse_command("/foo bar").is_none());
    }

    #[test]
    fn alert_bot_receives_critical_warn_and_info() {
        assert!(should_send_to_alert(&AlertSeverity::Critical));
        assert!(should_send_to_alert(&AlertSeverity::Warn));
        assert!(should_send_to_alert(&AlertSeverity::Info));
    }

    #[test]
    fn monitor_bot_receives_info_from_summary_channel() {
        // MonitorBot subscribes to summary_alert channel which only contains Info messages.
        assert!(should_send_to_monitor(&AlertSeverity::Info));
        assert!(!should_send_to_monitor(&AlertSeverity::Critical));
        assert!(!should_send_to_monitor(&AlertSeverity::Warn));
    }

    #[tokio::test]
    async fn alert_loop_handles_lagged_gracefully() {
        use crate::monitoring::alert::AlertRouter;
        let alert = AlertRouter::new(1); // tiny buffer → likely to lag
        let mut rx = alert.subscribe();

        for i in 0..10 {
            alert.warn(format!("msg {i}"));
        }

        loop {
            match rx.recv().await {
                Ok(_) => {}
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => break,
                Err(tokio::sync::broadcast::error::RecvError::Closed) => break,
            }
        }
        // No assert needed — just confirming no panic
    }

    #[tokio::test]
    async fn alert_loop_exits_on_cancellation() {
        use tokio_util::sync::CancellationToken;
        let token = CancellationToken::new();
        token.cancel();
        run_alert_task_stub(
            "dummy".into(),
            123,
            crate::monitoring::alert::AlertRouter::new(256),
            crate::monitoring::alert::AlertRouter::new(256),
            token,
        )
        .await;
    }

    #[test]
    fn route_kr_status_returns_status_string() {
        let result = parse_command("/kr status");
        assert!(matches!(
            result,
            Some((Market::Kr, BotCommand::QueryStatus))
        ));
    }

    #[test]
    fn route_positions_is_not_parse_command() {
        assert!(parse_command("/positions").is_none());
    }

    #[test]
    fn whitelist_contains_check() {
        let whitelist = [123456789_i64, 987654321_i64];
        assert!(whitelist.contains(&123456789));
        assert!(!whitelist.contains(&111111111));
    }

    #[test]
    fn parse_kill_hard_command() {
        assert_eq!(
            parse_global_command("/kill-hard"),
            Some(BotCommand::KillHard)
        );
    }

    #[test]
    fn parse_kill_soft_command() {
        assert_eq!(
            parse_global_command("/kill-soft"),
            Some(BotCommand::KillSoft)
        );
    }

    #[test]
    fn parse_kill_clear_command() {
        assert_eq!(
            parse_global_command("/kill-clear"),
            Some(BotCommand::KillClear)
        );
    }

    #[test]
    fn parse_liquidate_all_command() {
        assert_eq!(
            parse_global_command("/liquidate-all"),
            Some(BotCommand::LiquidateAll)
        );
    }

    #[test]
    fn parse_unknown_global_command() {
        assert!(parse_global_command("/foo").is_none());
    }
}
