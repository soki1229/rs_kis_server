use chrono::TimeZone;
use chrono::{DateTime, NaiveTime, Utc};
use futures_util::{SinkExt, StreamExt};
use kis_api::{KisClient, KisError};
use rand::Rng;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use tokio::net::TcpStream;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_tungstenite::{
    connect_async, tungstenite::protocol::Message, MaybeTlsStream, WebSocketStream,
};
use tokio_util::sync::CancellationToken;

type FullWsStream = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionKind {
    Price,
    Orderbook,
    DomesticPrice,
    DomesticOrderbook,
}

impl SubscriptionKind {
    pub fn tr_id(&self) -> &'static str {
        match self {
            Self::Price => "HDFSCNT0",
            Self::Orderbook => "HDFSASP0",
            Self::DomesticPrice => "H0STCNT0",
            Self::DomesticOrderbook => "H0STASP0",
        }
    }
}

#[derive(Debug, Clone)]
pub enum KisEvent {
    Transaction(TransactionData),
    Quote(QuoteData),
}

#[derive(Debug, Clone)]
pub struct TransactionData {
    pub symbol: String,
    pub price: Decimal,
    pub qty: Decimal,
    #[allow(dead_code)]
    pub is_buy: bool,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct QuoteData {
    pub symbol: String,
    #[allow(dead_code)]
    pub ask_price: Decimal,
    #[allow(dead_code)]
    pub bid_price: Decimal,
    pub ask_qty: Decimal,
    pub bid_qty: Decimal,
    #[allow(dead_code)]
    pub time: DateTime<Utc>,
}

#[derive(Clone)]
pub struct StreamManager {
    inner: Arc<StreamInner>,
}

struct StreamInner {
    ws_url: String,
    approval_key: RwLock<String>,
    kis_client: KisClient,
    key_in_use: AtomicBool,
    tx: broadcast::Sender<KisEvent>,
    subscriptions: RwLock<HashMap<(String, SubscriptionKind), ()>>,
    cmd_tx: mpsc::Sender<StreamCmd>,
    cancel: CancellationToken,
}

enum StreamCmd {
    Subscribe(String, SubscriptionKind),
    Unsubscribe(String, SubscriptionKind),
}

impl StreamManager {
    pub async fn connect(
        ws_url: &str,
        approval_key: String,
        kis_client: KisClient,
        event_buffer: usize,
    ) -> Result<Self, KisError> {
        let (tx, _) = broadcast::channel(event_buffer);
        let (cmd_tx, cmd_rx) = mpsc::channel(64);
        let cancel = CancellationToken::new();

        let inner = Arc::new(StreamInner {
            ws_url: ws_url.to_string(),
            approval_key: RwLock::new(approval_key),
            kis_client,
            key_in_use: AtomicBool::new(false),
            tx: tx.clone(),
            subscriptions: RwLock::new(HashMap::new()),
            cmd_tx,
            cancel: cancel.clone(),
        });

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            run_connection_loop(inner_clone, cmd_rx).await;
        });

        Ok(Self { inner })
    }

    pub fn receiver(&self) -> EventReceiver {
        EventReceiver {
            inner: self.inner.tx.subscribe(),
        }
    }

    pub async fn subscribe(&self, tr_key: &str, kind: SubscriptionKind) -> Result<(), KisError> {
        self.inner
            .cmd_tx
            .send(StreamCmd::Subscribe(tr_key.to_string(), kind))
            .await
            .map_err(|_| KisError::WebSocket("stream worker stopped".into()))
    }

    pub async fn unsubscribe(&self, tr_key: &str, kind: SubscriptionKind) -> Result<(), KisError> {
        self.inner
            .cmd_tx
            .send(StreamCmd::Unsubscribe(tr_key.to_string(), kind))
            .await
            .map_err(|_| KisError::WebSocket("stream worker stopped".into()))
    }
}

pub struct EventReceiver {
    inner: broadcast::Receiver<KisEvent>,
}

impl EventReceiver {
    pub async fn recv(&mut self) -> Result<KisEvent, KisError> {
        match self.inner.recv().await {
            Ok(e) => Ok(e),
            Err(broadcast::error::RecvError::Lagged(n)) => Err(KisError::Lagged(n)),
            Err(broadcast::error::RecvError::Closed) => Err(KisError::StreamClosed),
        }
    }
}

async fn run_connection_loop(inner: Arc<StreamInner>, mut cmd_rx: mpsc::Receiver<StreamCmd>) {
    let mut attempt = 0;
    loop {
        // If the last disconnect was due to OPSP8996 (key already in use),
        // refresh the approval_key before reconnecting and wait longer.
        if inner.key_in_use.swap(false, Ordering::Relaxed) {
            tracing::info!(
                "WS: ALREADY IN USE detected — refreshing approval_key before reconnect"
            );
            match inner.kis_client.approval_key().await {
                Ok(new_key) => {
                    *inner.approval_key.write().await = new_key;
                    tracing::info!("WS: approval_key refreshed");
                }
                Err(e) => tracing::warn!("WS: approval_key refresh failed: {e}"),
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
        }

        tracing::info!("WS 접속 중...");
        match connect_async(&inner.ws_url).await {
            Ok((mut ws_stream, _)) => {
                tracing::info!("WS 접속 완료 ✓");
                attempt = 0;

                // Resubscribe existing (rate-limited, errors logged but continue)
                let subs_snapshot: Vec<_> =
                    inner.subscriptions.read().await.keys().cloned().collect();
                if !subs_snapshot.is_empty() {
                    tracing::info!(
                        "WS: resubscribing {} entries after reconnect",
                        subs_snapshot.len()
                    );
                }
                let current_key = inner.approval_key.read().await.clone();
                for (tr_key, kind) in &subs_snapshot {
                    if let Err(e) =
                        send_sub(&mut ws_stream, &current_key, tr_key, *kind, true).await
                    {
                        tracing::warn!("WS: resubscribe failed for {tr_key}: {e}");
                        break;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                }

                if let Err(e) = handle_stream(&inner, &mut ws_stream, &mut cmd_rx).await {
                    tracing::warn!("WS 연결 끊김: {e} — 재접속 시도");
                }
            }
            Err(e) => tracing::error!("WS 접속 실패: {e}"),
        }

        if inner.cancel.is_cancelled() {
            break;
        }
        attempt += 1;
        tokio::time::sleep(backoff_duration(attempt)).await;
    }
}

async fn handle_stream(
    inner: &StreamInner,
    ws: &mut FullWsStream,
    cmd_rx: &mut mpsc::Receiver<StreamCmd>,
) -> Result<(), String> {
    loop {
        tokio::select! {
            msg = ws.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    if text.starts_with('{') {
                        if text.contains("\"tr_id\":\"PINGPONG\"") {
                            ws.send(Message::Text(text)).await.ok();
                        } else if log_server_json_response(&text) {
                            // OPSP8996: key already in use — signal reconnect loop to refresh key
                            inner.key_in_use.store(true, Ordering::Relaxed);
                        }
                    } else if let Some(event) = parse_ws_message(&text) {
                        let _ = inner.tx.send(event);
                    }
                }
                Some(Ok(Message::Close(_))) => return Err("Closed by server".into()),
                Some(Err(e)) => return Err(e.to_string()),
                None => return Err("Stream EOF".into()),
                _ => {}
            },
            cmd = cmd_rx.recv() => match cmd {
                Some(StreamCmd::Subscribe(key, kind)) => {
                    let already = inner.subscriptions.read().await.contains_key(&(key.clone(), kind));
                    if !already {
                        let current_key = inner.approval_key.read().await.clone();
                        if let Err(e) = send_sub(ws, &current_key, &key, kind, true).await {
                            return Err(e.to_string());
                        }
                        inner.subscriptions.write().await.insert((key, kind), ());
                        tokio::time::sleep(std::time::Duration::from_millis(300)).await;
                    }
                }
                Some(StreamCmd::Unsubscribe(key, kind)) => {
                    let was_present = inner.subscriptions.write().await.remove(&(key.clone(), kind)).is_some();
                    if was_present {
                        let current_key = inner.approval_key.read().await.clone();
                        send_sub(ws, &current_key, &key, kind, false).await.ok();
                    }
                }
                None => break,
            },
            _ = inner.cancel.cancelled() => break,
        }
    }
    Ok(())
}

async fn send_sub(
    ws: &mut FullWsStream,
    approval_key: &str,
    tr_key: &str,
    kind: SubscriptionKind,
    is_sub: bool,
) -> Result<(), KisError> {
    let msg = serde_json::json!({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": if is_sub { "1" } else { "2" },
            "content-type": "utf-8"
        },
        "body": { "input": { "tr_id": kind.tr_id(), "tr_key": tr_key } }
    });
    let text = msg.to_string();
    ws.send(Message::Text(text))
        .await
        .map_err(|e| KisError::WebSocket(e.to_string()))
}

fn backoff_duration(attempt: u32) -> std::time::Duration {
    let base = 1000u64
        .saturating_mul(1u64.checked_shl(attempt).unwrap_or(60000))
        .min(60000);
    std::time::Duration::from_millis(base + rand::thread_rng().gen_range(0..300))
}

fn parse_ws_message(text: &str) -> Option<KisEvent> {
    let parts: Vec<&str> = text.splitn(4, '|').collect();
    if parts.len() < 4 {
        return None;
    }
    let fields: Vec<&str> = parts[3].split('^').collect();
    match parts[1] {
        "HDFSCNT0" | "HDFSCNT1" | "H0STCNT0" => parse_transaction(parts[1], &fields),
        "HDFSASP0" | "HDFSASP1" | "H0STASP0" | "H0STASP1" => parse_quote(parts[1], &fields),
        _ => None,
    }
}

fn parse_transaction(tr_id: &str, fields: &[&str]) -> Option<KisEvent> {
    let (symbol, time_str, p_idx, q_idx, s_idx) = match tr_id {
        "HDFSCNT0" | "HDFSCNT1" => (fields.get(1)?, fields.get(7)?, 11, 19, 25),
        "H0STCNT0" => (fields.first()?, fields.get(1)?, 2, 9, 20),
        _ => return None,
    };
    Some(KisEvent::Transaction(TransactionData {
        symbol: symbol.to_string(),
        price: Decimal::from_str(fields.get(p_idx)?).ok()?,
        qty: Decimal::from_str(fields.get(q_idx)?).ok()?,
        is_buy: *fields.get(s_idx)? == "1",
        time: parse_time(time_str, tr_id)?,
    }))
}

fn parse_quote(tr_id: &str, fields: &[&str]) -> Option<KisEvent> {
    let (sym, time_str, ap_idx, bp_idx, aq_idx, bq_idx) = match tr_id {
        "HDFSASP0" | "HDFSASP1" => (fields.get(1)?, fields.get(5)?, 11, 12, 13, 14),
        "H0STASP0" | "H0STASP1" => (fields.first()?, fields.get(1)?, 3, 4, 13, 14),
        _ => return None,
    };
    Some(KisEvent::Quote(QuoteData {
        symbol: sym.to_string(),
        ask_price: Decimal::from_str(fields.get(ap_idx)?).ok()?,
        bid_price: Decimal::from_str(fields.get(bp_idx)?).ok()?,
        ask_qty: Decimal::from_str(fields.get(aq_idx)?).ok()?,
        bid_qty: Decimal::from_str(fields.get(bq_idx)?).ok()?,
        time: parse_time(time_str, tr_id)?,
    }))
}

fn parse_time(hms: &str, tr_id: &str) -> Option<DateTime<Utc>> {
    if hms.len() < 6 {
        return None;
    }
    let time = NaiveTime::parse_from_str(&hms[..6], "%H%M%S").ok()?;

    // KR 이벤트: H0STCNT0, H0STASP0 → KST (+09:00)
    // US 이벤트: HDFSCNT0, HDFSCNT1, HDFSASP0, HDFSASP1 → ET (America/New_York)
    let is_kr = tr_id.starts_with("H0ST");

    if is_kr {
        use chrono_tz::Asia::Seoul;
        let now_kst = Utc::now().with_timezone(&Seoul);
        Seoul
            .from_local_datetime(&now_kst.date_naive().and_time(time))
            .single()
            .map(|dt| dt.with_timezone(&Utc))
    } else {
        use chrono_tz::America::New_York;
        let now_ny = Utc::now().with_timezone(&New_York);
        New_York
            .from_local_datetime(&now_ny.date_naive().and_time(time))
            .single()
            .map(|dt| dt.with_timezone(&Utc))
    }
}

/// Returns true if the message is a fatal rejection that requires key refresh (OPSP8996).
fn log_server_json_response(text: &str) -> bool {
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
        let rt_cd = v
            .pointer("/body/rt_cd")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let msg1 = v
            .pointer("/body/msg1")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let msg_cd = v
            .pointer("/body/msg_cd")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let tr_id = v
            .pointer("/header/tr_id")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        let tr_key = v
            .pointer("/header/tr_key")
            .and_then(|x| x.as_str())
            .unwrap_or("");
        if rt_cd == "0" {
            tracing::debug!(tr_id, tr_key, msg1, "WS server ack");
            false
        } else if msg_cd == "OPSP8996" {
            tracing::warn!(
                tr_id,
                tr_key,
                rt_cd,
                msg_cd,
                msg1,
                "WS: ALREADY IN USE — will refresh key"
            );
            true
        } else if !rt_cd.is_empty() {
            tracing::warn!(
                tr_id,
                tr_key,
                rt_cd,
                msg_cd,
                msg1,
                "WS server rejected subscription"
            );
            false
        } else {
            tracing::debug!("WS server message: {}", text);
            false
        }
    } else {
        tracing::debug!("WS server non-JSON: {}", text);
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Timelike;

    #[test]
    fn test_parse_time_kr() {
        let tr_id = "H0STCNT0"; // KR
        let hms = "090000";
        let dt = parse_time(hms, tr_id).unwrap();

        // KST 09:00:00 should be UTC 00:00:00
        assert_eq!(dt.time().hour(), 0);
        assert_eq!(dt.time().minute(), 0);
    }

    #[test]
    fn test_parse_time_us() {
        let tr_id = "HDFSCNT0"; // US
        let hms = "100000"; // 10:00 AM ET
        let dt = parse_time(hms, tr_id).unwrap();

        // US ET is UTC-5 (Standard) or UTC-4 (DST).
        // Since we use Utc::now() to get the date, the exact offset depends on current date.
        // But it should definitely NOT be KST (UTC+9).
        // KST 10:00 AM would be UTC 01:00 AM.
        // ET 10:00 AM would be UTC 14:00 or 15:00.
        assert!(dt.time().hour() >= 14);
    }
}
