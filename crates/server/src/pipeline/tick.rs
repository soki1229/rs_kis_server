use crate::monitoring::alert::AlertRouter;
use crate::pipeline::TickData;
use crate::types::{Market, WatchlistSet};
use kis_api::{KisError, KisEvent, KisStream, SubscriptionKind, TransactionData};
use rust_decimal::prelude::ToPrimitive;
use std::collections::HashSet;
use tokio::sync::{broadcast, mpsc};
use tokio_util::sync::CancellationToken;

impl From<TransactionData> for TickData {
    fn from(tx: TransactionData) -> Self {
        Self {
            symbol: tx.symbol,
            price: tx.price,
            volume: tx.qty.to_u64().unwrap_or(0),
            timestamp: tx.time.into(),
        }
    }
}

const KIS_WS_MAX_SYMBOLS: usize = 20;

#[allow(clippy::too_many_arguments)]
pub async fn run_tick_task(
    market: Market,
    shared_stream: KisStream,
    mut watchlist_rx: tokio::sync::watch::Receiver<WatchlistSet>,
    tick_tx: broadcast::Sender<TickData>,
    tick_pos_tx: mpsc::Sender<TickData>,
    quote_tx: mpsc::Sender<crate::pipeline::QuoteSnapshot>,
    alert: AlertRouter,
    activity: crate::shared::activity::ActivityLog,
    db: sqlx::SqlitePool,
    token: CancellationToken,
) {
    let label = market.label();
    tracing::info!("{label} TickTask: starting WebSocket loop (shared stream)");

    let mut rx = shared_stream.receiver();
    let mut current_syms: HashSet<String> = HashSet::new();

    // 초기 워치리스트 반영
    {
        let all_syms = watchlist_rx.borrow().all_unique();
        if !all_syms.is_empty() {
            let syms_to_sub: HashSet<String> =
                all_syms.into_iter().take(KIS_WS_MAX_SYMBOLS).collect();
            let sub_ok = subscribe_symbols(&shared_stream, &syms_to_sub, market, &alert, &db).await;
            tracing::info!(
                "{label} TickTask: initial subscription of {} symbols",
                sub_ok
            );
            current_syms = syms_to_sub;
        }
    }

    let mut tick_count: u64 = 0;
    let mut log_interval = tokio::time::interval(std::time::Duration::from_secs(60));
    log_interval.tick().await;

    loop {
        tokio::select! {
            result = rx.recv() => {
                match result {
                    Ok(KisEvent::Transaction(tx)) if current_syms.contains(&tx.symbol) => {
                        tick_count += 1;
                        activity.record_tick(label);
                        if tick_count == 1 { tracing::info!(symbol = %tx.symbol, price = %tx.price, "{label} TickTask: first tick received"); }
                        forward_tick(tx.into(), &tick_tx, &tick_pos_tx).await;
                    }
                    Ok(KisEvent::Quote(q)) if current_syms.contains(&q.symbol) => {
                        let snapshot = crate::pipeline::QuoteSnapshot { symbol: q.symbol.clone(), bid_qty: q.bid_qty.to_u64().unwrap_or(0), ask_qty: q.ask_qty.to_u64().unwrap_or(0) };
                        quote_tx.send(snapshot).await.ok();
                    }
                    Ok(_) => {}
                    Err(KisError::Lagged(n)) => { tracing::warn!("{label} TickTask: lagged {n} events"); alert.warn(format!("{label} TickTask: lagged {n} events")); }
                    Err(e) => { tracing::error!("{label} TickTask: stream error: {e}"); break; }
                }
            }
            _ = log_interval.tick() => {
                if tick_count > 0 { tracing::info!("{label} TickTask: {tick_count} ticks received so far"); }
                else if !current_syms.is_empty() { tracing::warn!("{label} TickTask: no ticks received yet (subscribed to {} symbols)", current_syms.len()); }
            }
            _ = watchlist_rx.changed() => {
                let wl_set = watchlist_rx.borrow().clone();
                let new_syms_vec = wl_set.all_unique();
                let new_syms: HashSet<String> = new_syms_vec.iter().cloned().collect();
                let to_sub: HashSet<String> = new_syms.difference(&current_syms).cloned().collect();
                let to_unsub: HashSet<String> = current_syms.difference(&new_syms).cloned().collect();

                activity.set_watchlist(label, &new_syms_vec);
                if !to_sub.is_empty() || !to_unsub.is_empty() {
                    tracing::info!("🔄 {label} 감시 종목 갱신 시작 (안정: {}건, 공격: {}건, 합계: {}건)", wl_set.stable.len(), wl_set.aggressive.len(), new_syms.len());
                }

                if !to_sub.is_empty() { subscribe_symbols(&shared_stream, &to_sub, market, &alert, &db).await; }
                for sym in to_unsub { unsubscribe_symbol(&shared_stream, &sym, market, &db).await; }
                current_syms = new_syms;

                if !current_syms.is_empty() { tracing::info!("✅ {label} 감시 종목 갱신 완료 ({}건 운용 중)", current_syms.len()); }
                else { tracing::info!("😴 {label} 감시 종목 없음 (모두 해제됨)"); }
            }
            _ = token.cancelled() => return,
        }
    }
}

async fn subscribe_symbols(
    stream: &KisStream,
    symbols: &HashSet<String>,
    market: Market,
    alert: &AlertRouter,
    db: &sqlx::SqlitePool,
) -> usize {
    let label = market.label();
    let (p_kind, ob_kind) = match market {
        Market::Kr => (
            SubscriptionKind::DomesticPrice,
            SubscriptionKind::DomesticOrderbook,
        ),
        Market::Us => (SubscriptionKind::Price, SubscriptionKind::Orderbook),
    };
    let mut ok = 0usize;
    for sym in symbols {
        let price_ok = stream.subscribe(sym, p_kind).await;
        let ob_ok = stream.subscribe(sym, ob_kind).await;
        let name: Option<String> = sqlx::query_scalar(
            "SELECT name FROM daily_ohlc WHERE symbol = ? AND name IS NOT NULL LIMIT 1",
        )
        .bind(sym)
        .fetch_optional(db)
        .await
        .unwrap_or(None);
        let display_name = match name {
            Some(n) if !n.is_empty() => format!("{}({})", n, sym),
            _ => sym.clone(),
        };
        match (price_ok, ob_ok) {
            (Ok(()), Ok(())) => {
                ok += 1;
                tracing::info!("{label} WS: SUBSCRIBE SUCCESS [{display_name}]");
            }
            (Err(e), _) => {
                let msg =
                    format!("{label} TickTask: price subscribe failed for {display_name}: {e}");
                tracing::warn!("{msg}");
                alert.warn(msg);
            }
            (_, Err(e)) => {
                let msg =
                    format!("{label} TickTask: orderbook subscribe failed for {display_name}: {e}");
                tracing::warn!("{msg}");
                alert.warn(msg);
            }
        }
    }
    ok
}

async fn unsubscribe_symbol(stream: &KisStream, sym: &str, market: Market, db: &sqlx::SqlitePool) {
    let label = market.label();
    let (p_kind, ob_kind) = match market {
        Market::Kr => (
            SubscriptionKind::DomesticPrice,
            SubscriptionKind::DomesticOrderbook,
        ),
        Market::Us => (SubscriptionKind::Price, SubscriptionKind::Orderbook),
    };
    let _ = stream.unsubscribe(sym, p_kind).await;
    let _ = stream.unsubscribe(sym, ob_kind).await;
    let name: Option<String> = sqlx::query_scalar(
        "SELECT name FROM daily_ohlc WHERE symbol = ? AND name IS NOT NULL LIMIT 1",
    )
    .bind(sym)
    .fetch_optional(db)
    .await
    .unwrap_or(None);
    let display_name = match name {
        Some(n) if !n.is_empty() => format!("{}({})", n, sym),
        _ => sym.to_string(),
    };
    tracing::info!("{label} WS: UNSUBSCRIBE SUCCESS [{display_name}]");
}

async fn forward_tick(
    tick: TickData,
    tick_tx: &broadcast::Sender<TickData>,
    tick_pos_tx: &mpsc::Sender<TickData>,
) {
    let _ = tick_tx.send(tick.clone());
    let _ = tick_pos_tx.send(tick).await;
}

#[allow(clippy::too_many_arguments)]
pub async fn run_us_tick_task(
    shared_stream: KisStream,
    watchlist_rx: tokio::sync::watch::Receiver<WatchlistSet>,
    tick_tx: broadcast::Sender<TickData>,
    tick_pos_tx: mpsc::Sender<TickData>,
    quote_tx: mpsc::Sender<crate::pipeline::QuoteSnapshot>,
    alert: AlertRouter,
    activity: crate::shared::activity::ActivityLog,
    db: sqlx::SqlitePool,
    token: CancellationToken,
) {
    run_tick_task(
        Market::Us,
        shared_stream,
        watchlist_rx,
        tick_tx,
        tick_pos_tx,
        quote_tx,
        alert,
        activity,
        db,
        token,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub async fn run_kr_tick_task(
    shared_stream: KisStream,
    watchlist_rx: tokio::sync::watch::Receiver<WatchlistSet>,
    tick_tx: broadcast::Sender<TickData>,
    tick_pos_tx: mpsc::Sender<TickData>,
    quote_tx: mpsc::Sender<crate::pipeline::QuoteSnapshot>,
    alert: AlertRouter,
    activity: crate::shared::activity::ActivityLog,
    db: sqlx::SqlitePool,
    token: CancellationToken,
) {
    run_tick_task(
        Market::Kr,
        shared_stream,
        watchlist_rx,
        tick_tx,
        tick_pos_tx,
        quote_tx,
        alert,
        activity,
        db,
        token,
    )
    .await
}

#[cfg(test)]
#[allow(dead_code)]
pub async fn run_kr_tick_task_stub(
    _tick_tx: broadcast::Sender<TickData>,
    _tick_pos_tx: mpsc::Sender<TickData>,
    token: CancellationToken,
) {
    token.cancelled().await;
    tracing::info!("TickTask: shutting down");
}
