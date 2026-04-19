use chrono::{DateTime, FixedOffset, NaiveTime, TimeZone};
use futures_util::{SinkExt, StreamExt};
use kis_api::KisError;
use rand::Rng;
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::{broadcast, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::protocol::Message};
use tokio_util::sync::CancellationToken;

// ── Types ──────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum KisEvent {
    Transaction(TransactionData),
    Quote(QuoteData),
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TransactionData {
    pub symbol: String,
    pub price: Decimal,
    pub qty: Decimal,
    pub time: DateTime<FixedOffset>,
    pub is_buy: bool,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct QuoteData {
    pub symbol: String,
    pub ask_price: Decimal,
    pub bid_price: Decimal,
    pub ask_qty: Decimal,
    pub bid_qty: Decimal,
    pub time: DateTime<FixedOffset>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SubscriptionKind {
    Price,             // HDFSCNT0
    Orderbook,         // HDFSASP0/1
    DomesticPrice,     // H0STCNT0
    DomesticOrderbook, // H0STASP0/1
}

#[derive(Clone)]
pub struct StreamManager {
    inner: Arc<StreamInner>,
}

struct StreamInner {
    ws_url: String,
    approval_key: String,
    app_key: String,
    tx: broadcast::Sender<KisEvent>,
    subscriptions: RwLock<HashMap<(String, SubscriptionKind), ()>>,
    cancel: CancellationToken,
    ws_tx: Mutex<Option<WsSink>>,
}

type WsSink = futures_util::stream::SplitSink<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
    tokio_tungstenite::tungstenite::Message,
>;

type WsReadHalf = futures_util::stream::SplitStream<
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>,
>;

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

// ── Implementation ──────────────────────────────────────────────────────────

impl StreamManager {
    pub async fn connect(
        ws_url: &str,
        approval_key: String,
        app_key: String,
        event_buffer: usize,
    ) -> Result<Self, KisError> {
        let (tx, _) = broadcast::channel(event_buffer);
        let cancel = CancellationToken::new();

        let (ws_stream, _) = connect_async(ws_url)
            .await
            .map_err(|e| KisError::WebSocket(e.to_string()))?;
        let (ws_write, ws_read) = ws_stream.split();

        let inner = Arc::new(StreamInner {
            ws_url: ws_url.to_string(),
            approval_key,
            app_key,
            tx: tx.clone(),
            subscriptions: RwLock::new(HashMap::new()),
            cancel: cancel.clone(),
            ws_tx: Mutex::new(Some(ws_write)),
        });

        let inner_clone = inner.clone();
        tokio::spawn(async move {
            run_connection_loop(inner_clone, ws_read).await;
        });

        Ok(Self { inner })
    }

    pub fn receiver(&self) -> EventReceiver {
        EventReceiver {
            inner: self.inner.tx.subscribe(),
        }
    }

    pub async fn subscribe(&self, symbol: &str, kind: SubscriptionKind) -> Result<(), KisError> {
        let key = (symbol.to_string(), kind);
        let mut subs = self.inner.subscriptions.write().await;
        if subs.contains_key(&key) {
            return Ok(());
        }
        send_subscribe_raw(
            &self.inner.ws_tx,
            &self.inner.approval_key,
            &self.inner.app_key,
            symbol,
            kind,
            true,
        )
        .await?;
        subs.insert(key, ());
        Ok(())
    }

    pub async fn unsubscribe(&self, symbol: &str, kind: SubscriptionKind) -> Result<(), KisError> {
        let key = (symbol.to_string(), kind);
        let mut subs = self.inner.subscriptions.write().await;
        if !subs.contains_key(&key) {
            return Ok(());
        }
        send_subscribe_raw(
            &self.inner.ws_tx,
            &self.inner.approval_key,
            &self.inner.app_key,
            symbol,
            kind,
            false,
        )
        .await?;
        subs.remove(&key);
        Ok(())
    }

    #[allow(dead_code)]
    pub fn close(&self) {
        self.inner.cancel.cancel();
    }
}

// ── Private Helpers ─────────────────────────────────────────────────────────

async fn send_subscribe_raw(
    ws_tx: &Mutex<Option<WsSink>>,
    approval_key: &str,
    _app_key: &str,
    symbol: &str,
    kind: SubscriptionKind,
    subscribe: bool,
) -> Result<(), KisError> {
    let tr_id = match kind {
        SubscriptionKind::Price => "HDFSCNT0",
        SubscriptionKind::Orderbook => "HDFSASP0",
        SubscriptionKind::DomesticPrice => "H0STCNT0",
        SubscriptionKind::DomesticOrderbook => "H0STASP0",
    };
    let msg = serde_json::json!({
        "header": {
            "approval_key": approval_key,
            "custtype": "P",
            "tr_type": if subscribe { "1" } else { "2" },
            "content-type": "utf-8"
        },
        "body": {
            "input": {
                "tr_id": tr_id,
                "tr_key": symbol
            }
        }
    });
    let text = serde_json::to_string(&msg).map_err(|e| KisError::WebSocket(e.to_string()))?;
    let mut guard = ws_tx.lock().await;
    match *guard {
        Some(ref mut writer) => writer
            .send(Message::Text(text))
            .await
            .map_err(|e| KisError::WebSocket(e.to_string())),
        None => Err(KisError::WebSocket("not connected".into())),
    }
}

const BACKOFF_INITIAL_MS: u64 = 1_000;
const BACKOFF_MAX_MS: u64 = 60_000;
const BACKOFF_JITTER_FRACTION: f64 = 0.3;

fn backoff_duration(attempt: u32) -> std::time::Duration {
    let base = BACKOFF_INITIAL_MS
        .saturating_mul(1u64.checked_shl(attempt).unwrap_or(u64::MAX))
        .min(BACKOFF_MAX_MS);
    let mut rng = rand::thread_rng();
    let jitter = (base as f64 * BACKOFF_JITTER_FRACTION * rng.gen::<f64>()) as u64;
    std::time::Duration::from_millis(base + jitter)
}

enum DisconnectReason {
    Cancelled,
    Error(String),
    Eof,
}

async fn run_connection_loop(inner: Arc<StreamInner>, initial_ws_read: WsReadHalf) {
    let mut attempt: u32 = 0;
    let mut current_ws_read = Some(initial_ws_read);

    loop {
        if inner.cancel.is_cancelled() {
            break;
        }

        let ws_read = if let Some(r) = current_ws_read.take() {
            r
        } else {
            let delay = backoff_duration(attempt.saturating_sub(1));
            tokio::select! {
                _ = inner.cancel.cancelled() => break,
                _ = tokio::time::sleep(delay) => {}
            }
            match connect_async(&inner.ws_url).await {
                Ok((ws, _)) => {
                    let (w, r) = ws.split();
                    *inner.ws_tx.lock().await = Some(w);
                    resubscribe_all(&inner).await;
                    r
                }
                Err(e) => {
                    tracing::warn!("WS reconnect failed: {e}");
                    attempt = attempt.saturating_add(1);
                    continue;
                }
            }
        };

        let (reason, had_data) = read_loop(&inner, ws_read, &inner.cancel).await;
        *inner.ws_tx.lock().await = None;

        match reason {
            DisconnectReason::Cancelled => break,
            DisconnectReason::Error(e) => tracing::warn!("WS disconnected: {e}"),
            DisconnectReason::Eof => tracing::warn!("WS closed by server"),
        }

        if had_data {
            attempt = 0;
        } else {
            attempt = attempt.saturating_add(1);
        }
    }
}

async fn resubscribe_all(inner: &StreamInner) {
    let subs = inner.subscriptions.read().await;
    for (symbol, kind) in subs.keys() {
        if let Err(e) = send_subscribe_raw(
            &inner.ws_tx,
            &inner.approval_key,
            &inner.app_key,
            symbol,
            *kind,
            true,
        )
        .await
        {
            tracing::error!("failed to resubscribe {}/{:?}: {}", symbol, kind, e);
        }
    }
}

async fn read_loop(
    inner: &StreamInner,
    mut reader: WsReadHalf,
    cancel: &CancellationToken,
) -> (DisconnectReason, bool) {
    let mut had_data = false;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return (DisconnectReason::Cancelled, had_data),
            msg = reader.next() => match msg {
                Some(Ok(Message::Text(text))) => {
                    match classify_text_message(&text) {
                        TextMessage::PingPong => {
                            tracing::info!("WS: PINGPONG received, echoing back for session maintenance");
                            let mut g = inner.ws_tx.lock().await;
                            if let Some(ref mut w) = *g {
                                if let Err(e) = w.send(Message::Text(text)).await {
                                    return (DisconnectReason::Error(e.to_string()), had_data);
                                }
                            }
                        }
                        TextMessage::OtherJson => {}
                        TextMessage::Data => {
                            if let Some(event) = parse_ws_message(&text) {
                                had_data = true;
                                let _ = inner.tx.send(event);
                            }
                        }
                    }
                }
                Some(Ok(Message::Ping(_))) => {} // tungstenite auto-pongs
                Some(Err(e)) => return (DisconnectReason::Error(e.to_string()), had_data),
                None => return (DisconnectReason::Eof, had_data),
                _ => {}
            }
        }
    }
}

enum TextMessage {
    PingPong,
    OtherJson,
    Data,
}

fn classify_text_message(text: &str) -> TextMessage {
    if !text.starts_with('{') {
        return TextMessage::Data;
    }
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(text) {
        if v.get("header")
            .and_then(|h| h.get("tr_id"))
            .and_then(|t| t.as_str())
            == Some("PINGPONG")
        {
            return TextMessage::PingPong;
        }
    }
    TextMessage::OtherJson
}

fn parse_ws_message(text: &str) -> Option<KisEvent> {
    if text.starts_with('{') {
        return None;
    }
    let parts: Vec<&str> = text.splitn(4, '|').collect();
    if parts.len() < 4 {
        return None;
    }
    let fields: Vec<&str> = parts[3].split('^').collect();
    match parts[1] {
        "HDFSCNT0" => parse_transaction(parts[1], &fields),
        "HDFSASP0" | "HDFSASP1" => parse_quote(parts[1], &fields),
        "H0STCNT0" => parse_transaction(parts[1], &fields),
        "H0STASP0" | "H0STASP1" => parse_quote(parts[1], &fields),
        _ => None,
    }
}

fn parse_transaction(tr_id: &str, fields: &[&str]) -> Option<KisEvent> {
    let (symbol, time_str, price_idx, qty_idx, side_idx) = match tr_id {
        "HDFSCNT0" => (fields.get(1)?, fields.get(2)?, 11, 19, 21),
        "H0STCNT0" => (fields.first()?, fields.get(1)?, 2, 9, 20),
        _ => return None,
    };

    let price = Decimal::from_str(fields.get(price_idx)?).ok()?;
    let qty = Decimal::from_str(fields.get(qty_idx)?).ok()?;
    let is_buy = match *fields.get(side_idx)? {
        "1" => true,
        "2" => false,
        _ => return None,
    };
    let time = parse_time(time_str)?;

    Some(KisEvent::Transaction(TransactionData {
        symbol: symbol.to_string(),
        price,
        qty,
        is_buy,
        time,
    }))
}

fn parse_quote(tr_id: &str, fields: &[&str]) -> Option<KisEvent> {
    let (symbol, time_str, ask_p_idx, bid_p_idx, ask_q_idx, bid_q_idx) = match tr_id {
        "HDFSASP0" | "HDFSASP1" => (fields.get(1)?, fields.get(2)?, 14, 15, 16, 17),
        "H0STASP0" | "H0STASP1" => (fields.first()?, fields.get(1)?, 3, 4, 13, 14),
        _ => return None,
    };

    let ask_price = Decimal::from_str(fields.get(ask_p_idx)?).ok()?;
    let bid_price = Decimal::from_str(fields.get(bid_p_idx)?).ok()?;
    let ask_qty = Decimal::from_str(fields.get(ask_q_idx)?).ok()?;
    let bid_qty = Decimal::from_str(fields.get(bid_q_idx)?).ok()?;
    let time = parse_time(time_str)?;

    Some(KisEvent::Quote(QuoteData {
        symbol: symbol.to_string(),
        ask_price,
        bid_price,
        ask_qty,
        bid_qty,
        time,
    }))
}

fn parse_time(hms: &str) -> Option<DateTime<FixedOffset>> {
    let time = NaiveTime::parse_from_str(hms, "%H%M%S").ok()?;
    let now = chrono::Utc::now().with_timezone(&FixedOffset::east_opt(9 * 3600)?);
    let dt = now.date_naive().and_time(time);
    FixedOffset::east_opt(9 * 3600)?
        .from_local_datetime(&dt)
        .single()
}

// ── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_hdfscnt0_transaction() {
        let mut fields = vec![""; 26];
        fields[1] = "NVDA";
        fields[2] = "143022";
        fields[11] = "134.20";
        fields[19] = "50";
        fields[21] = "1";
        let msg = format!("0|HDFSCNT0|1|{}", fields.join("^"));
        let ev = parse_ws_message(&msg).unwrap();
        if let KisEvent::Transaction(d) = ev {
            assert_eq!(d.symbol, "NVDA");
            assert_eq!(d.price, Decimal::from_str("134.20").unwrap());
            assert_eq!(d.qty, Decimal::from_str("50").unwrap());
            assert!(d.is_buy);
        } else {
            panic!("wrong event type");
        }
    }

    #[test]
    fn parse_h0stcnt0_transaction() {
        let mut fields = vec![""; 30];
        fields[0] = "005930";
        fields[1] = "102030";
        fields[2] = "75000";
        fields[9] = "100";
        fields[20] = "2";
        let msg = format!("0|H0STCNT0|1|{}", fields.join("^"));
        let ev = parse_ws_message(&msg).unwrap();
        if let KisEvent::Transaction(d) = ev {
            assert_eq!(d.symbol, "005930");
            assert_eq!(d.price, Decimal::from_str("75000").unwrap());
            assert_eq!(d.qty, Decimal::from_str("100").unwrap());
            assert!(!d.is_buy);
        } else {
            panic!("wrong event type");
        }
    }

    #[test]
    fn classify_pingpong_message() {
        let msg = r#"{"header":{"tr_id":"PINGPONG","datetime":"20240321102030"}}"#;
        assert!(matches!(classify_text_message(msg), TextMessage::PingPong));
    }

    #[test]
    fn backoff_duration_increases() {
        let d1 = backoff_duration(0);
        let d4 = backoff_duration(3);
        assert!(d1 < d4);
    }
}
