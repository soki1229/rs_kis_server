use crate::monitoring::alert::AlertRouter;
use crate::state::{BotState, MarketSummary};
use crate::types::{FillInfo, OrderRequest, Side};
use kis_api::{
    CancelKind, CancelOrderRequest, DomesticCancelOrderRequest, DomesticExchange,
    DomesticOrderHistoryRequest, DomesticPlaceOrderRequest, Exchange, KisApi, KisDomesticApi,
    OrderHistoryRequest, OrderSide, OrderType, PlaceOrderRequest,
};
use rust_decimal::Decimal;
use sqlx::{Row, SqlitePool};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, RwLock,
};
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

// DEPRECATED: Use `run_generic_execution_task` from `generic_execution.rs` instead.
// This function will be removed in a future version after all callers migrate to the generic version.
//
// 9 parameters: 2 receivers, 1 sender, client, db_pool, summary, alert, token, pending_count — all required for this task
#[allow(clippy::too_many_arguments)]
pub async fn run_execution_task(
    mut order_rx: mpsc::Receiver<OrderRequest>,
    mut force_order_rx: mpsc::Receiver<OrderRequest>,
    fill_tx: mpsc::Sender<FillInfo>,
    client: Arc<dyn KisApi>,
    db_pool: SqlitePool,
    summary: Arc<RwLock<MarketSummary>>,
    alert: AlertRouter,
    token: CancellationToken,
    pending_count: Arc<AtomicU32>,
    poll_sem: Arc<Semaphore>,
) {
    // On startup, recover any orders that were left in 'Submitted' state by a previous
    // run (e.g. server crash between place_order and fill confirmation).
    reconcile_submitted_orders(&client, &db_pool, &fill_tx, &alert, &poll_sem).await;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("ExecutionTask: initiating graceful shutdown");
                cancel_unfilled_on_shutdown(&client, &alert).await;
                break;
            }
            req = order_rx.recv() => {
                match req {
                    None => break,
                    Some(r) => {
                        // Signal incremented pending_count before send; decrement now that we've received it
                        pending_count.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some(v.saturating_sub(1))).ok();
                        let state = summary.read().unwrap_or_else(|e| e.into_inner()).bot_state.clone();
                        if matches!(state, BotState::Active) {
                            let (tx, cl, db, al, ps) = (fill_tx.clone(), Arc::clone(&client), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move { process_order(r, &tx, &cl, &db, &al, &ps).await }));
                        } else {
                            tracing::warn!("ExecutionTask: regular order blocked (state={:?})", state);
                        }
                        handles.retain(|h| !h.is_finished());
                    }
                }
            }
            req = force_order_rx.recv() => {
                match req {
                    None => break,
                    Some(r) => {
                        let state = summary.read().unwrap_or_else(|e| e.into_inner()).bot_state.clone();
                        if matches!(state, BotState::HardBlocked) {
                            tracing::warn!("ExecutionTask: force order blocked (HardBlocked)");
                        } else {
                            let (tx, cl, db, al, ps) = (fill_tx.clone(), Arc::clone(&client), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move { process_order(r, &tx, &cl, &db, &al, &ps).await }));
                        }
                        handles.retain(|h| !h.is_finished());
                    }
                }
            }
        }
    }

    // Wait for all in-flight order tasks before exiting
    for h in handles {
        let _ = h.await;
    }
}

#[tracing::instrument(
    skip(fill_tx, client, db_pool, alert, poll_sem),
    fields(symbol = %req.symbol, side = ?req.side, qty = req.qty)
)]
async fn process_order(
    req: OrderRequest,
    fill_tx: &mpsc::Sender<FillInfo>,
    client: &Arc<dyn KisApi>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let order_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    if let Err(e) = sqlx::query(
        "INSERT INTO orders (id, symbol, side, state, order_type, qty, price, atr, exchange_code, submitted_at, updated_at) VALUES (?, ?, ?, 'PendingSubmit', 'marketable_limit', ?, ?, ?, ?, ?, ?)"
    )
    .bind(&order_id)
    .bind(&req.symbol)
    .bind(if req.side == Side::Buy { "buy" } else { "sell" })
    .bind(req.qty.to_string())
    .bind(req.price.map(|p| p.to_string()))
    .bind(req.atr.map(|a| a.to_string()))
    .bind(&req.exchange_code)
    .bind(&now)
    .bind(&now)
    .execute(db_pool)
    .await
    {
        alert.warn(format!(
            "ExecutionTask: DB insert failed for {} — aborting order: {}",
            req.symbol, e
        ));
        return;
    }

    let exchange = match req.exchange_code.as_deref() {
        Some("NYSE") => Exchange::NYSE,
        Some("AMEX") => Exchange::AMEX,
        Some("NASD") => Exchange::NASD,
        None => {
            tracing::warn!(
                symbol = %req.symbol,
                "US order exchange_code is None — defaulting to NASDAQ. \
                 Symbol may be on NYSE/AMEX. Check seed_symbols exchange detection."
            );
            Exchange::NASD
        }
        Some(other) => {
            tracing::warn!(
                symbol = %req.symbol,
                exchange = other,
                "Unknown US exchange code — defaulting to NASDAQ"
            );
            Exchange::NASD
        }
    };
    // Apply aggressive limit pricing for strong buy signals
    let adjusted_price = match (req.side.clone(), req.price, req.strength) {
        (Side::Buy, Some(base_price), Some(strength)) if strength >= 0.85 => {
            let bump_pct = Decimal::new(2, 3); // 0.2%
            let adjusted = base_price * (Decimal::ONE + bump_pct);
            tracing::debug!(
                symbol = %req.symbol,
                strength,
                base_price = %base_price,
                adjusted_price = %adjusted,
                "ExecutionTask: aggressive limit pricing applied"
            );
            Some(adjusted)
        }
        _ => req.price,
    };

    let place_req = PlaceOrderRequest {
        symbol: req.symbol.clone(),
        exchange,
        side: if req.side == Side::Buy {
            OrderSide::Buy
        } else {
            OrderSide::Sell
        },
        order_type: OrderType::Limit,
        qty: Decimal::from(req.qty),
        price: adjusted_price,
    };

    let broker_order_id = match client.place_order(place_req).await {
        Err(e) => {
            tracing::error!(
                symbol = %req.symbol,
                price = ?req.price,
                qty = req.qty,
                exchange_code = ?req.exchange_code,
                error = %e,
                "ExecutionTask: place_order API error"
            );
            alert.warn(format!(
                "ExecutionTask: place_order failed for {} (qty={}, price={:?}, exchange={:?}): {}",
                req.symbol, req.qty, req.price, req.exchange_code, e
            ));
            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(&order_id)
                .execute(db_pool)
                .await;
            return;
        }
        Ok(resp) => resp.order_org_no,
    };

    let _ = sqlx::query(
        "UPDATE orders SET broker_order_id = ?, state = 'Submitted', updated_at = ? WHERE id = ?",
    )
    .bind(&broker_order_id)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&order_id)
    .execute(db_pool)
    .await;

    poll_until_filled(
        client,
        &order_id,
        &broker_order_id,
        &req.symbol,
        req.qty,
        req.exchange_code.clone(),
        fill_tx,
        db_pool,
        alert,
        poll_sem,
    )
    .await;
}

/// 주문 폴링 결과 — 미체결 목록에서 사라진 이유를 order_history로 확인해 구분
enum PollResult {
    Filled {
        filled_qty: u64,
        filled_price: Decimal,
    },
    /// 부분 체결 — 타임아웃 전에 일부만 체결된 후 취소됨
    PartialFilled {
        filled_qty: u64,
        filled_price: Decimal,
    },
    Cancelled,
    TimedOut,
}

// 9 parameters required for fill polling context — all distinct responsibilities
#[allow(clippy::too_many_arguments)]
async fn poll_until_filled(
    client: &Arc<dyn KisApi>,
    order_id: &str,
    broker_order_id: &str,
    symbol: &str,
    qty: u64,
    exchange_code: Option<String>,
    fill_tx: &mpsc::Sender<FillInfo>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);

    loop {
        let orders = {
            let _permit = poll_sem.acquire().await;
            client.unfilled_orders().await
        };
        match orders {
            Ok(orders) => {
                let still_open = orders.iter().any(|o| o.order_no == broker_order_id);
                if !still_open {
                    // 미체결 목록에서 사라짐 → order_history로 체결 vs 취소 구분
                    let result = confirm_fill_from_history(client, broker_order_id, qty).await;
                    match result {
                        PollResult::Filled {
                            filled_qty,
                            filled_price,
                        } => {
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'FullyFilled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;

                            tracing::info!(
                                "✅ [체결완료] {} {}주 (@{})",
                                symbol,
                                filled_qty,
                                filled_price
                            );
                            alert.info(format!(
                                "✅ [체결완료] {} {}주 (@{})",
                                symbol, filled_qty, filled_price
                            ));

                            let _ = fill_tx
                                .send(FillInfo {
                                    order_id: order_id.to_string(),
                                    symbol: symbol.to_string(),
                                    filled_qty,
                                    filled_price,
                                    exchange_code: exchange_code.clone(),
                                    atr: None,
                                })
                                .await;
                        }
                        PollResult::PartialFilled {
                            filled_qty,
                            filled_price,
                        } => {
                            tracing::warn!(
                                "🌗 [부분체결] {} {}/{}주 (@{})",
                                symbol,
                                filled_qty,
                                qty,
                                filled_price
                            );
                            alert.warn(format!(
                                "🌗 [부분체결] {} {}/{}주 (@{}) - 잔량은 자동 취소되었습니다.",
                                symbol, filled_qty, qty, filled_price
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'PartiallyFilled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                            let _ = fill_tx
                                .send(FillInfo {
                                    order_id: order_id.to_string(),
                                    symbol: symbol.to_string(),
                                    filled_qty,
                                    filled_price,
                                    exchange_code: exchange_code.clone(),
                                    atr: None,
                                })
                                .await;
                        }
                        PollResult::Cancelled => {
                            tracing::info!(
                                "⚪️ [취소됨] {} 주문이 거래소에 의해 취소되었습니다.",
                                symbol
                            );
                            alert.warn(format!(
                                "⚪️ [취소됨] {} 주문이 거래소에 의해 취소되었습니다. (주문번호: {})", symbol, broker_order_id
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                        }
                        PollResult::TimedOut => {
                            // history 조회 실패 → 취소로 보수적 처리
                            alert.warn(format!(
                                "ExecutionTask: could not confirm fill for {broker_order_id} ({symbol}) — marking Cancelled"
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                        }
                    }
                    return;
                }
            }
            Err(e) => {
                tracing::warn!("ExecutionTask: unfilled_orders error: {}", e);
            }
        }

        // Sleep first so API errors do not cause premature timeout:
        // all retry paths (still-open or transient error) wait the full interval
        // before the deadline is re-evaluated.
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                order_id = %order_id,
                symbol = %symbol,
                "ExecutionTask: polling deadline exceeded — attempting broker cancel before marking Failed"
            );

            // Attempt to cancel the order at the broker before marking Failed locally.
            // Without this, the order remains live and may fill as a ghost position.
            let cancel_result = async {
                let unfilled = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    client.unfilled_orders(),
                )
                .await
                .map_err(|_| "unfilled_orders timed out".to_string())?
                .map_err(|e| format!("unfilled_orders error: {e}"))?;

                if let Some(o) = unfilled.iter().find(|o| o.order_no == broker_order_id) {
                    let exchange = match o.exchange.as_str() {
                        "NYSE" => Exchange::NYSE,
                        "AMEX" => Exchange::AMEX,
                        _ => Exchange::NASD,
                    };
                    let cancel_req = CancelOrderRequest {
                        symbol: o.symbol.clone(),
                        exchange,
                        original_order_id: o.order_no.clone(),
                        kind: CancelKind::Cancel,
                        qty: o.remaining_qty,
                        price: Some(o.price),
                    };
                    tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        client.cancel_order(cancel_req),
                    )
                    .await
                    .map_err(|_| "cancel_order timed out".to_string())?
                    .map_err(|e| format!("cancel_order error: {e}"))?;
                    Ok::<bool, String>(true) // cancelled
                } else {
                    Ok(false) // not in unfilled list — may have filled or already cancelled
                }
            }
            .await;

            match cancel_result {
                Ok(true) => {
                    alert.warn(format!(
                        "ExecutionTask: order {broker_order_id} ({symbol}) cancelled at broker after poll timeout"
                    ));
                }
                Ok(false) => {
                    alert.warn(format!(
                        "ExecutionTask: order {broker_order_id} ({symbol}) not in unfilled list at timeout — may have filled"
                    ));
                }
                Err(e) => {
                    alert.critical(format!(
                        "ExecutionTask: CRITICAL — failed to cancel order {broker_order_id} ({symbol}) at broker: {e} — MANUAL REVIEW NEEDED: order may still be live"
                    ));
                }
            }

            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(order_id)
                .execute(db_pool)
                .await;
            return;
        }
    }
}

/// order_history API로 broker_order_id 주문의 실제 체결 여부와 가격 확인
async fn confirm_fill_from_history(
    client: &Arc<dyn KisApi>,
    broker_order_id: &str,
    submitted_qty: u64,
) -> PollResult {
    let today = chrono::Utc::now().format("%Y%m%d").to_string();
    match client
        .order_history(OrderHistoryRequest {
            start_date: today.clone(),
            end_date: today,
            exchange_cd: String::new(), // 빈 문자열 = 전체 거래소
        })
        .await
    {
        Ok(history) => {
            if let Some(h) = history.iter().find(|h| h.order_no == broker_order_id) {
                let filled_qty = h
                    .filled_qty
                    .to_string()
                    .parse::<u64>()
                    .unwrap_or(submitted_qty);
                if filled_qty >= submitted_qty {
                    // 전량 체결
                    return PollResult::Filled {
                        filled_qty,
                        filled_price: h.filled_price,
                    };
                } else if filled_qty > 0 {
                    // 부분 체결 — 나머지는 이미 취소됐거나 곧 취소될 예정
                    return PollResult::PartialFilled {
                        filled_qty,
                        filled_price: h.filled_price,
                    };
                }
            }
            PollResult::Cancelled
        }
        Err(e) => {
            tracing::warn!("ExecutionTask: order_history error for {broker_order_id}: {e}");
            PollResult::TimedOut
        }
    }
}

/// On startup, query SQLite for orders left in `Submitted` state from a previous run,
/// then re-enter `poll_until_filled` for each one.
///
/// Three outcomes per stale order:
/// - Broker confirms it is already filled → emit FillInfo, mark FullyFilled
/// - Broker still shows it as open       → poll until filled or 60s timeout
/// - Broker has no record of it           → mark Failed and alert
async fn reconcile_submitted_orders(
    client: &Arc<dyn KisApi>,
    db_pool: &SqlitePool,
    fill_tx: &mpsc::Sender<FillInfo>,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let rows = match sqlx::query(
        "SELECT id, broker_order_id, symbol, qty, price, exchange_code FROM orders WHERE state = 'Submitted'",
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            alert.warn(format!(
                "ExecutionTask: reconciliation query failed — stale orders may exist: {}",
                e
            ));
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    tracing::info!(
        "ExecutionTask: reconciling {} stale Submitted order(s) from previous run",
        rows.len()
    );

    for row in rows {
        let order_id: String = row.get("id");
        let broker_order_id: Option<String> = row.try_get("broker_order_id").ok().flatten();
        let symbol: String = row.get("symbol");
        let qty: String = row.get("qty");
        let exchange_code: Option<String> = row.try_get("exchange_code").ok().flatten();

        let broker_order_id = match broker_order_id {
            Some(id) => id,
            None => {
                // No broker ID — place_order succeeded but ID was never written;
                // mark Failed so it is not orphaned indefinitely.
                alert.warn(format!(
                    "ExecutionTask: stale order {} ({}) has no broker_order_id — marking Failed",
                    order_id, symbol
                ));
                let _ =
                    sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                        .bind(chrono::Utc::now().to_rfc3339())
                        .bind(&order_id)
                        .execute(db_pool)
                        .await;
                continue;
            }
        };

        let qty: u64 = qty.parse().unwrap_or(0);

        alert.info(format!(
            "ExecutionTask: reconciling stale order {} ({}) broker_id={}",
            order_id, symbol, broker_order_id
        ));

        poll_until_filled(
            client,
            &order_id,
            &broker_order_id,
            &symbol,
            qty,
            exchange_code,
            fill_tx,
            db_pool,
            alert,
            poll_sem,
        )
        .await;
    }
}

async fn cancel_unfilled_on_shutdown(client: &Arc<dyn KisApi>, alert: &AlertRouter) {
    let unfilled =
        tokio::time::timeout(std::time::Duration::from_secs(10), client.unfilled_orders()).await;
    let unfilled_result = match unfilled {
        Ok(r) => r,
        Err(_) => {
            alert.warn("ExecutionTask: shutdown unfilled_orders timed out".to_string());
            return;
        }
    };
    match unfilled_result {
        Ok(orders) if orders.is_empty() => {}
        Ok(orders) => {
            for o in orders {
                let exchange = match o.exchange.as_str() {
                    "NYSE" => Exchange::NYSE,
                    "AMEX" => Exchange::AMEX,
                    _ => Exchange::NASD,
                };
                let cancel_req = CancelOrderRequest {
                    symbol: o.symbol.clone(),
                    exchange,
                    original_order_id: o.order_no.clone(),
                    kind: CancelKind::Cancel,
                    qty: o.remaining_qty,
                    price: Some(o.price),
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.cancel_order(cancel_req),
                )
                .await
                {
                    Ok(Ok(_)) => tracing::info!("ExecutionTask: cancelled order {}", o.order_no),
                    Ok(Err(e)) => alert.warn(format!(
                        "ExecutionTask: cancel unconfirmed for {}: {}",
                        o.order_no, e
                    )),
                    Err(_) => alert.warn(format!(
                        "ExecutionTask: cancel timed out for {}",
                        o.order_no
                    )),
                }
            }
        }
        Err(e) => {
            alert.warn(format!(
                "ExecutionTask: shutdown unfilled_orders error: {}",
                e
            ));
        }
    }
}

// DEPRECATED: Use `run_generic_execution_task` with `KrMarketAdapter` instead.
// This function will be removed in a future version after all callers migrate to the generic version.
#[allow(clippy::too_many_arguments)]
pub async fn run_kr_execution_task(
    mut order_rx: mpsc::Receiver<OrderRequest>,
    mut force_order_rx: mpsc::Receiver<OrderRequest>,
    fill_tx: mpsc::Sender<FillInfo>,
    client: Arc<dyn KisDomesticApi>,
    db_pool: SqlitePool,
    summary: Arc<RwLock<MarketSummary>>,
    alert: AlertRouter,
    token: CancellationToken,
    pending_count: Arc<AtomicU32>,
    poll_sem: Arc<Semaphore>,
) {
    reconcile_kr_submitted_orders(&client, &db_pool, &fill_tx, &alert, &poll_sem).await;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("KrExecutionTask: initiating graceful shutdown");
                cancel_kr_unfilled_on_shutdown(&client, &alert).await;
                break;
            }
            req = order_rx.recv() => {
                match req {
                    None => break,
                    Some(r) => {
                        // Signal incremented pending_count before send; decrement now that we've received it
                        pending_count.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some(v.saturating_sub(1))).ok();
                        let state = summary.read().unwrap_or_else(|e| e.into_inner()).bot_state.clone();
                        if matches!(state, BotState::Active) {
                            let (tx, cl, db, al, ps) = (fill_tx.clone(), Arc::clone(&client), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move { process_kr_order(r, &tx, &cl, &db, &al, &ps).await }));
                        } else {
                            tracing::warn!("KrExecutionTask: regular order blocked (state={:?})", state);
                        }
                        handles.retain(|h| !h.is_finished());
                    }
                }
            }
            req = force_order_rx.recv() => {
                match req {
                    None => break,
                    Some(r) => {
                        let state = summary.read().unwrap_or_else(|e| e.into_inner()).bot_state.clone();
                        if matches!(state, BotState::HardBlocked) {
                            tracing::warn!("KrExecutionTask: force order blocked (HardBlocked)");
                        } else {
                            let (tx, cl, db, al, ps) = (fill_tx.clone(), Arc::clone(&client), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move { process_kr_order(r, &tx, &cl, &db, &al, &ps).await }));
                        }
                        handles.retain(|h| !h.is_finished());
                    }
                }
            }
        }
    }

    // Wait for all in-flight order tasks before exiting
    for h in handles {
        let _ = h.await;
    }
}

#[tracing::instrument(
    skip(fill_tx, client, db_pool, alert, poll_sem),
    fields(symbol = %req.symbol, side = ?req.side, qty = req.qty)
)]
async fn process_kr_order(
    req: OrderRequest,
    fill_tx: &mpsc::Sender<FillInfo>,
    client: &Arc<dyn KisDomesticApi>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let order_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    if let Err(e) = sqlx::query(
        "INSERT INTO orders (id, symbol, side, state, order_type, qty, price, atr, exchange_code, submitted_at, updated_at) VALUES (?, ?, ?, 'PendingSubmit', 'marketable_limit', ?, ?, ?, ?, ?, ?)"
    )
    .bind(&order_id)
    .bind(&req.symbol)
    .bind(if req.side == Side::Buy { "buy" } else { "sell" })
    .bind(req.qty.to_string())
    .bind(req.price.map(|p| p.to_string()))
    .bind(req.atr.map(|a| a.to_string()))
    .bind(&req.exchange_code)
    .bind(&now)
    .bind(&now)
    .execute(db_pool)
    .await
    {
        alert.warn(format!(
            "KrExecutionTask: DB insert failed for {} — aborting order: {}",
            req.symbol, e
        ));
        return;
    }

    let exchange = match req.exchange_code.as_deref().unwrap_or("J") {
        "Q" => DomesticExchange::KOSDAQ,
        _ => DomesticExchange::KOSPI,
    };

    // Apply aggressive limit pricing for strong buy signals
    let adjusted_price = match (req.side.clone(), req.price, req.strength) {
        (Side::Buy, Some(base_price), Some(strength)) if strength >= 0.85 => {
            let bump_pct = Decimal::new(2, 3); // 0.2%
            let adjusted = base_price * (Decimal::ONE + bump_pct);
            tracing::debug!(
                symbol = %req.symbol,
                strength,
                base_price = %base_price,
                adjusted_price = %adjusted,
                "KrExecutionTask: aggressive limit pricing applied"
            );
            Some(adjusted)
        }
        _ => req.price,
    };

    let place_req = DomesticPlaceOrderRequest {
        symbol: req.symbol.clone(),
        exchange,
        side: if req.side == Side::Buy {
            kis_api::OrderSide::Buy
        } else {
            kis_api::OrderSide::Sell
        },
        order_type: kis_api::DomesticOrderType::Limit,
        qty: req.qty as u32,
        price: adjusted_price,
    };

    let broker_order_no = match client.domestic_place_order(place_req).await {
        Err(e) => {
            tracing::error!(
                symbol = %req.symbol,
                price = ?req.price,
                qty = req.qty,
                exchange_code = ?req.exchange_code,
                error = %e,
                "KrExecutionTask: domestic_place_order API error"
            );
            alert.warn(format!(
                "KrExecutionTask: domestic_place_order failed for {} (qty={}, price={:?}, exchange={:?}): {}",
                req.symbol, req.qty, req.price, req.exchange_code, e
            ));
            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(&order_id)
                .execute(db_pool)
                .await;
            return;
        }
        Ok(resp) => resp.order_no,
    };

    let _ = sqlx::query(
        "UPDATE orders SET broker_order_id = ?, state = 'Submitted', updated_at = ? WHERE id = ?",
    )
    .bind(&broker_order_no)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&order_id)
    .execute(db_pool)
    .await;

    let exchange_code = req.exchange_code.clone();
    poll_kr_until_filled(
        client,
        &order_id,
        &broker_order_no,
        &req.symbol,
        req.qty,
        exchange_code,
        fill_tx,
        db_pool,
        alert,
        poll_sem,
    )
    .await;
}

#[allow(clippy::too_many_arguments)]
async fn poll_kr_until_filled(
    client: &Arc<dyn KisDomesticApi>,
    order_id: &str,
    broker_order_no: &str,
    symbol: &str,
    qty: u64,
    exchange_code: Option<String>,
    fill_tx: &mpsc::Sender<FillInfo>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);

    loop {
        let orders = {
            let _permit = poll_sem.acquire().await;
            client.domestic_unfilled_orders().await
        };
        match orders {
            Ok(orders) => {
                let still_open = orders.iter().any(|o| o.order_no == broker_order_no);
                if !still_open {
                    // 미체결 목록에서 사라짐 → domestic_order_history로 체결 vs 취소 구분
                    let result = confirm_kr_fill_from_history(client, broker_order_no, qty).await;
                    match result {
                        PollResult::Filled {
                            filled_qty,
                            filled_price,
                        } => {
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'FullyFilled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;

                            tracing::info!(
                                "✅ [국내 체결완료] {} {}주 (@{})",
                                symbol,
                                filled_qty,
                                filled_price
                            );
                            alert.info(format!(
                                "✅ [국내 체결완료] {} {}주 (@{})",
                                symbol, filled_qty, filled_price
                            ));

                            let _ = fill_tx
                                .send(FillInfo {
                                    order_id: order_id.to_string(),
                                    symbol: symbol.to_string(),
                                    filled_qty,
                                    filled_price,
                                    exchange_code: exchange_code.clone(),
                                    atr: None,
                                })
                                .await;
                        }
                        PollResult::PartialFilled {
                            filled_qty,
                            filled_price,
                        } => {
                            tracing::warn!(
                                "🌗 [국내 부분체결] {} {}/{}주 (@{})",
                                symbol,
                                filled_qty,
                                qty,
                                filled_price
                            );
                            alert.warn(format!(
                                "🌗 [국내 부분체결] {} {}/{}주 (@{}) - 잔량은 자동 취소되었습니다.",
                                symbol, filled_qty, qty, filled_price
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'PartiallyFilled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                            let _ = fill_tx
                                .send(FillInfo {
                                    order_id: order_id.to_string(),
                                    symbol: symbol.to_string(),
                                    filled_qty,
                                    filled_price,
                                    exchange_code: exchange_code.clone(),
                                    atr: None,
                                })
                                .await;
                        }
                        PollResult::Cancelled => {
                            tracing::info!("⚪️ [국내 취소됨] {} 주문이 취소되었습니다.", symbol);
                            alert.warn(format!(
                                "⚪️ [국내 취소됨] {} 주문이 취소되었습니다. (주문번호: {})",
                                symbol, broker_order_no
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                        }
                        PollResult::TimedOut => {
                            alert.warn(format!(
                                "KrExecutionTask: could not confirm fill for {broker_order_no} ({symbol}) — marking Cancelled"
                            ));
                            let _ = sqlx::query(
                                "UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?",
                            )
                            .bind(chrono::Utc::now().to_rfc3339())
                            .bind(order_id)
                            .execute(db_pool)
                            .await;
                        }
                    }
                    return;
                }
            }
            Err(e) => {
                tracing::warn!(
                    "KrExecutionTask: domestic_unfilled_orders error: {} — trying balance fallback",
                    e
                );

                // VTS fallback: check balance to detect fill when unfilled_orders API fails
                match try_kr_balance_fallback(client, symbol, qty).await {
                    Some((filled_qty, filled_price)) => {
                        let _ = sqlx::query(
                            "UPDATE orders SET state = 'FullyFilled', updated_at = ? WHERE id = ?",
                        )
                        .bind(chrono::Utc::now().to_rfc3339())
                        .bind(order_id)
                        .execute(db_pool)
                        .await;

                        tracing::info!(
                            "✅ [국내 체결완료-폴백] {} {}주 (@{})",
                            symbol,
                            filled_qty,
                            filled_price
                        );
                        alert.info(format!(
                            "✅ [국내 체결완료-폴백] {} {}주 (@{}) — 잔고 변화로 체결 감지됨",
                            symbol, filled_qty, filled_price
                        ));

                        let _ = fill_tx
                            .send(FillInfo {
                                order_id: order_id.to_string(),
                                symbol: symbol.to_string(),
                                filled_qty,
                                filled_price,
                                exchange_code: exchange_code.clone(),
                                atr: None,
                            })
                            .await;
                        return;
                    }
                    None => {
                        tracing::debug!(
                            "KrExecutionTask: balance fallback did not detect fill for {}",
                            symbol
                        );
                    }
                }
            }
        }

        tokio::time::sleep(std::time::Duration::from_secs(5)).await;

        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                order_id = %order_id,
                symbol = %symbol,
                broker_order_no = %broker_order_no,
                "KrExecutionTask: polling deadline exceeded — attempting broker cancel before marking Failed"
            );

            // Attempt to cancel the order at the broker before marking Failed locally.
            let cancel_result = async {
                let unfilled = tokio::time::timeout(
                    std::time::Duration::from_secs(10),
                    client.domestic_unfilled_orders(),
                )
                .await
                .map_err(|_| "domestic_unfilled_orders timed out".to_string())?
                .map_err(|e| format!("domestic_unfilled_orders error: {e}"))?;

                if let Some(o) = unfilled.iter().find(|o| o.order_no == broker_order_no) {
                    let exchange = match o.exchange.as_str() {
                        "KSQ" => DomesticExchange::KOSDAQ,
                        _ => DomesticExchange::KOSPI,
                    };
                    let cancel_req = DomesticCancelOrderRequest {
                        symbol: o.symbol.clone(),
                        exchange,
                        original_order_no: o.order_no.clone(),
                        qty: o.remaining_qty,
                        price: Some(o.price),
                    };
                    tokio::time::timeout(
                        std::time::Duration::from_secs(5),
                        client.domestic_cancel_order(cancel_req),
                    )
                    .await
                    .map_err(|_| "domestic_cancel_order timed out".to_string())?
                    .map_err(|e| format!("domestic_cancel_order error: {e}"))?;
                    Ok::<bool, String>(true)
                } else {
                    Ok(false)
                }
            }
            .await;

            match cancel_result {
                Ok(true) => {
                    alert.warn(format!(
                        "KrExecutionTask: order {broker_order_no} ({symbol}) cancelled at broker after poll timeout"
                    ));
                }
                Ok(false) => {
                    alert.warn(format!(
                        "KrExecutionTask: order {broker_order_no} ({symbol}) not in unfilled list at timeout — may have filled"
                    ));
                }
                Err(e) => {
                    alert.critical(format!(
                        "KrExecutionTask: CRITICAL — failed to cancel order {broker_order_no} ({symbol}) at broker: {e} — MANUAL REVIEW NEEDED: order may still be live"
                    ));
                }
            }

            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(order_id)
                .execute(db_pool)
                .await;
            return;
        }
    }
}

async fn confirm_kr_fill_from_history(
    client: &Arc<dyn KisDomesticApi>,
    broker_order_no: &str,
    submitted_qty: u64,
) -> PollResult {
    let today = chrono::Utc::now().format("%Y%m%d").to_string();
    match client
        .domestic_order_history(DomesticOrderHistoryRequest {
            start_date: today.clone(),
            end_date: today,
        })
        .await
    {
        Ok(history) => {
            if let Some(h) = history.iter().find(|h| h.order_no == broker_order_no) {
                let filled_qty = h.filled_qty as u64;
                if filled_qty >= submitted_qty {
                    // 전량 체결
                    return PollResult::Filled {
                        filled_qty,
                        filled_price: h.filled_price,
                    };
                } else if filled_qty > 0 {
                    // 부분 체결
                    return PollResult::PartialFilled {
                        filled_qty,
                        filled_price: h.filled_price,
                    };
                }
            }
            PollResult::Cancelled
        }
        Err(e) => {
            tracing::warn!(
                "KrExecutionTask: domestic_order_history error for {broker_order_no}: {e}"
            );
            PollResult::TimedOut
        }
    }
}

/// VTS fallback: detect fill by checking balance change when unfilled_orders API fails.
/// Returns (filled_qty, filled_price) if the symbol appears in balance with expected qty.
async fn try_kr_balance_fallback(
    client: &Arc<dyn KisDomesticApi>,
    symbol: &str,
    expected_qty: u64,
) -> Option<(u64, Decimal)> {
    use rust_decimal::prelude::ToPrimitive;
    match client.domestic_balance().await {
        Ok(balance) => {
            if let Some(holding) = balance.items.iter().find(|h| h.symbol == symbol) {
                let qty = holding.qty.to_u64().unwrap_or(0);
                if qty >= expected_qty {
                    tracing::info!(
                        "VTS balance fallback: found {} with qty={}, avg_price={}",
                        symbol,
                        qty,
                        holding.avg_price
                    );
                    return Some((qty, holding.avg_price));
                }
            }
            None
        }
        Err(e) => {
            tracing::warn!(
                "KrExecutionTask: domestic_balance fallback failed for {}: {}",
                symbol,
                e
            );
            None
        }
    }
}

async fn reconcile_kr_submitted_orders(
    client: &Arc<dyn KisDomesticApi>,
    db_pool: &SqlitePool,
    fill_tx: &mpsc::Sender<FillInfo>,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let rows = match sqlx::query(
        "SELECT id, broker_order_id, symbol, qty, price, exchange_code FROM orders WHERE state = 'Submitted'",
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            alert.warn(format!(
                "KrExecutionTask: reconciliation query failed — stale orders may exist: {}",
                e
            ));
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    tracing::info!(
        "KrExecutionTask: reconciling {} stale Submitted order(s)",
        rows.len()
    );

    for row in rows {
        use sqlx::Row;
        let order_id: String = row.get("id");
        let broker_order_id: Option<String> = row.try_get("broker_order_id").ok().flatten();
        let symbol: String = row.get("symbol");
        let qty: String = row.get("qty");
        let exchange_code: Option<String> = row.try_get("exchange_code").ok().flatten();

        let broker_order_no = match broker_order_id {
            Some(id) => id,
            None => {
                alert.warn(format!(
                    "KrExecutionTask: stale order {} ({}) has no broker_order_id — marking Failed",
                    order_id, symbol
                ));
                let _ =
                    sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                        .bind(chrono::Utc::now().to_rfc3339())
                        .bind(&order_id)
                        .execute(db_pool)
                        .await;
                continue;
            }
        };

        let qty: u64 = qty.parse().unwrap_or(0);

        alert.info(format!(
            "KrExecutionTask: reconciling stale order {} ({}) broker_no={}",
            order_id, symbol, broker_order_no
        ));

        poll_kr_until_filled(
            client,
            &order_id,
            &broker_order_no,
            &symbol,
            qty,
            exchange_code,
            fill_tx,
            db_pool,
            alert,
            poll_sem,
        )
        .await;
    }
}

async fn cancel_kr_unfilled_on_shutdown(client: &Arc<dyn KisDomesticApi>, alert: &AlertRouter) {
    let unfilled = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        client.domestic_unfilled_orders(),
    )
    .await;
    let unfilled_result = match unfilled {
        Ok(r) => r,
        Err(_) => {
            alert.warn("KrExecutionTask: shutdown domestic_unfilled_orders timed out".to_string());
            return;
        }
    };
    match unfilled_result {
        Ok(orders) if orders.is_empty() => {}
        Ok(orders) => {
            for o in orders {
                let exchange = match o.exchange.as_str() {
                    "KSQ" => DomesticExchange::KOSDAQ,
                    _ => DomesticExchange::KOSPI,
                };
                let cancel_req = DomesticCancelOrderRequest {
                    symbol: o.symbol.clone(),
                    exchange,
                    original_order_no: o.order_no.clone(),
                    qty: o.remaining_qty,
                    price: Some(o.price),
                };
                match tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    client.domestic_cancel_order(cancel_req),
                )
                .await
                {
                    Ok(Ok(_)) => {
                        tracing::info!("KrExecutionTask: cancelled order {}", o.order_no)
                    }
                    Ok(Err(e)) => alert.warn(format!(
                        "KrExecutionTask: cancel unconfirmed for {}: {}",
                        o.order_no, e
                    )),
                    Err(_) => alert.warn(format!(
                        "KrExecutionTask: cancel timed out for {}",
                        o.order_no
                    )),
                }
            }
        }
        Err(e) => {
            alert.warn(format!(
                "KrExecutionTask: shutdown domestic_unfilled_orders error: {}",
                e
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::monitoring::alert::AlertRouter;
    use crate::state::{BotState, MarketSummary};
    use crate::types::{OrderRequest, Side};
    use async_trait::async_trait;
    use kis_api::{
        CancelOrderRequest, CancelOrderResponse, CandleBar, DailyChartRequest,
        DomesticCancelOrderRequest, DomesticCancelOrderResponse, DomesticDailyChartRequest,
        DomesticExchange, DomesticOrderHistoryItem, DomesticOrderHistoryRequest,
        DomesticPlaceOrderRequest, DomesticPlaceOrderResponse, DomesticRankingItem,
        DomesticUnfilledOrder, Exchange, Holiday, KisDomesticApi, KisStream, NewsItem,
        OrderHistoryItem, OrderHistoryRequest, PlaceOrderRequest, PlaceOrderResponse, RankingItem,
        UnfilledOrder,
    };
    use rust_decimal_macros::dec;
    use std::sync::{Arc, RwLock};
    use tokio::sync::mpsc;
    use tokio_util::sync::CancellationToken;

    fn make_req(symbol: &str) -> OrderRequest {
        OrderRequest {
            symbol: symbol.into(),
            side: Side::Buy,
            qty: 1,
            price: Some(dec!(130.00)),
            atr: Some(dec!(2.5)),
            exchange_code: None,
            strength: None,
        }
    }

    struct ImmediateFillClient;

    #[async_trait]
    impl kis_api::KisApi for ImmediateFillClient {
        async fn stream(&self) -> Result<KisStream, kis_api::KisError> {
            unimplemented!()
        }
        async fn volume_ranking(
            &self,
            _: &Exchange,
            _: u32,
        ) -> Result<Vec<RankingItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn holidays(&self, _: &str) -> Result<Vec<Holiday>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn place_order(
            &self,
            _: PlaceOrderRequest,
        ) -> Result<PlaceOrderResponse, kis_api::KisError> {
            Ok(PlaceOrderResponse {
                order_date: "20260323".into(),
                order_org_no: "12345".into(),
                order_time: "103000".into(),
            })
        }
        async fn cancel_order(
            &self,
            _: CancelOrderRequest,
        ) -> Result<CancelOrderResponse, kis_api::KisError> {
            unimplemented!()
        }
        async fn daily_chart(
            &self,
            _: DailyChartRequest,
        ) -> Result<Vec<CandleBar>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn news(&self, _: &str) -> Result<Vec<NewsItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn order_history(
            &self,
            _: OrderHistoryRequest,
        ) -> Result<Vec<OrderHistoryItem>, kis_api::KisError> {
            // "12345" = place_order 응답 + 재조정 테스트 시드 broker_order_id와 일치
            Ok(vec![OrderHistoryItem {
                order_no: "12345".into(),
                symbol: String::new(),
                name: String::new(),
                side_cd: "02".into(),
                qty: dec!(1),
                filled_qty: dec!(1),
                filled_price: dec!(130.00),
                filled_date: "20260328".into(),
                filled_time: "103000".into(),
            }])
        }
        async fn balance(&self) -> Result<kis_api::BalanceResponse, kis_api::KisError> {
            Ok(kis_api::BalanceResponse {
                items: vec![],
                summary: kis_api::BalanceSummary {
                    purchase_amount: rust_decimal::Decimal::ZERO,
                    available_cash: rust_decimal::Decimal::ZERO,
                    realized_pnl: rust_decimal::Decimal::ZERO,
                    total_pnl: rust_decimal::Decimal::ZERO,
                },
            })
        }
        async fn check_deposit(&self) -> Result<kis_api::DepositInfo, kis_api::KisError> {
            Ok(kis_api::DepositInfo {
                currency: "USD".to_string(),
                amount: rust_decimal::Decimal::ZERO,
            })
        }
    }

    struct FailPlaceClient;

    #[async_trait]
    impl kis_api::KisApi for FailPlaceClient {
        async fn stream(&self) -> Result<KisStream, kis_api::KisError> {
            unimplemented!()
        }
        async fn volume_ranking(
            &self,
            _: &Exchange,
            _: u32,
        ) -> Result<Vec<RankingItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn holidays(&self, _: &str) -> Result<Vec<Holiday>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn place_order(
            &self,
            _: PlaceOrderRequest,
        ) -> Result<PlaceOrderResponse, kis_api::KisError> {
            Err(kis_api::KisError::Api {
                code: "ERR".into(),
                message: "rejected".into(),
            })
        }
        async fn cancel_order(
            &self,
            _: CancelOrderRequest,
        ) -> Result<CancelOrderResponse, kis_api::KisError> {
            unimplemented!()
        }
        async fn daily_chart(
            &self,
            _: DailyChartRequest,
        ) -> Result<Vec<CandleBar>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn news(&self, _: &str) -> Result<Vec<NewsItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn order_history(
            &self,
            _: OrderHistoryRequest,
        ) -> Result<Vec<OrderHistoryItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn balance(&self) -> Result<kis_api::BalanceResponse, kis_api::KisError> {
            Ok(kis_api::BalanceResponse {
                items: vec![],
                summary: kis_api::BalanceSummary {
                    purchase_amount: rust_decimal::Decimal::ZERO,
                    available_cash: rust_decimal::Decimal::ZERO,
                    realized_pnl: rust_decimal::Decimal::ZERO,
                    total_pnl: rust_decimal::Decimal::ZERO,
                },
            })
        }
        async fn check_deposit(&self) -> Result<kis_api::DepositInfo, kis_api::KisError> {
            Ok(kis_api::DepositInfo {
                currency: "USD".to_string(),
                amount: rust_decimal::Decimal::ZERO,
            })
        }
    }

    #[tokio::test]
    async fn active_order_produces_fill_on_immediate_fill() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let (order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::Active,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(ImmediateFillClient);

        tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        order_tx.send(make_req("NVDA")).await.unwrap();
        let fill = tokio::time::timeout(std::time::Duration::from_millis(500), fill_rx.recv())
            .await
            .expect("should receive fill")
            .unwrap();
        assert_eq!(fill.symbol, "NVDA");
        token.cancel();
    }

    #[tokio::test]
    async fn place_order_failure_does_not_send_fill() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let (order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::Active,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(FailPlaceClient);

        tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        order_tx.send(make_req("NVDA")).await.unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(200), fill_rx.recv()).await;
        assert!(
            result.is_err(),
            "failed place_order should not produce fill"
        );
        token.cancel();
    }

    #[tokio::test]
    async fn suspended_state_blocks_regular_order() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let (order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::Suspended,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(ImmediateFillClient);

        tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        order_tx.send(make_req("NVDA")).await.unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), fill_rx.recv()).await;
        assert!(result.is_err(), "Suspended should block regular orders");
        token.cancel();
    }

    #[tokio::test]
    async fn hard_blocked_blocks_force_orders_too() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let (_order_tx, order_rx) = mpsc::channel(8);
        let (force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::HardBlocked,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(ImmediateFillClient);

        tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        force_tx.send(make_req("AAPL")).await.unwrap();
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(100), fill_rx.recv()).await;
        assert!(result.is_err(), "HardBlocked should block force orders");
        token.cancel();
    }

    #[tokio::test]
    async fn graceful_shutdown_cancels_unfilled_orders() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let cancel_called = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cancel_called2 = cancel_called.clone();

        struct TrackingCancelClient {
            cancelled: Arc<std::sync::atomic::AtomicBool>,
        }
        #[async_trait]
        impl kis_api::KisApi for TrackingCancelClient {
            async fn stream(&self) -> Result<KisStream, kis_api::KisError> {
                unimplemented!()
            }
            async fn volume_ranking(
                &self,
                _: &Exchange,
                _: u32,
            ) -> Result<Vec<RankingItem>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn holidays(&self, _: &str) -> Result<Vec<Holiday>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn place_order(
                &self,
                _: PlaceOrderRequest,
            ) -> Result<PlaceOrderResponse, kis_api::KisError> {
                Ok(PlaceOrderResponse {
                    order_date: "20260323".into(),
                    order_org_no: "99".into(),
                    order_time: "103000".into(),
                })
            }
            async fn cancel_order(
                &self,
                _: CancelOrderRequest,
            ) -> Result<CancelOrderResponse, kis_api::KisError> {
                self.cancelled
                    .store(true, std::sync::atomic::Ordering::SeqCst);
                Ok(CancelOrderResponse {
                    order_date: "20260323".into(),
                    order_org_no: "99".into(),
                    order_time: "103001".into(),
                })
            }
            async fn daily_chart(
                &self,
                _: DailyChartRequest,
            ) -> Result<Vec<CandleBar>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, kis_api::KisError> {
                Ok(vec![UnfilledOrder {
                    order_no: "99".into(),
                    orig_order_no: "".into(),
                    symbol: "NVDA".into(),
                    name: "NVIDIA".into(),
                    exchange: "NASD".into(),
                    side_cd: "02".into(),
                    qty: dec!(1),
                    price: dec!(130.0),
                    filled_qty: dec!(0),
                    remaining_qty: dec!(1),
                }])
            }
            async fn news(&self, _: &str) -> Result<Vec<NewsItem>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn order_history(
                &self,
                _: OrderHistoryRequest,
            ) -> Result<Vec<OrderHistoryItem>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn balance(&self) -> Result<kis_api::BalanceResponse, kis_api::KisError> {
                Ok(kis_api::BalanceResponse {
                    items: vec![],
                    summary: kis_api::BalanceSummary {
                        purchase_amount: rust_decimal::Decimal::ZERO,
                        available_cash: rust_decimal::Decimal::ZERO,
                        realized_pnl: rust_decimal::Decimal::ZERO,
                        total_pnl: rust_decimal::Decimal::ZERO,
                    },
                })
            }
            async fn check_deposit(&self) -> Result<kis_api::DepositInfo, kis_api::KisError> {
                Ok(kis_api::DepositInfo {
                    currency: "USD".to_string(),
                    amount: rust_decimal::Decimal::ZERO,
                })
            }
        }

        let (_order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, _fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary::new()));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(TrackingCancelClient {
            cancelled: cancel_called2,
        });

        let handle = tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        token.cancel();
        tokio::time::timeout(std::time::Duration::from_millis(500), handle)
            .await
            .expect("task should exit")
            .unwrap();

        assert!(
            cancel_called.load(std::sync::atomic::Ordering::SeqCst),
            "cancel_order should be called on shutdown when there are unfilled orders"
        );
    }

    #[tokio::test]
    async fn reconcile_stale_submitted_order_emits_fill() {
        // Simulate a crash: order was placed (broker_order_id="42") and marked Submitted,
        // but the server restarted before fill confirmation arrived.
        // On startup, reconciliation should re-poll and emit FillInfo when the order
        // is no longer in the broker's unfilled list.
        let pool = crate::db::connect(":memory:").await.unwrap();

        sqlx::query(
            "INSERT INTO orders (id, broker_order_id, symbol, side, state, order_type, qty, price, submitted_at, updated_at) \
             VALUES ('stale-1', '12345', 'NVDA', 'buy', 'Submitted', 'marketable_limit', '2', '130.00', '2026-03-23T00:00:00Z', '2026-03-23T00:00:00Z')"
        )
        .execute(&pool)
        .await
        .unwrap();

        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let alert = AlertRouter::new(256);
        // ImmediateFillClient returns empty unfilled_orders → order already filled
        let client: Arc<dyn kis_api::KisApi> = Arc::new(ImmediateFillClient);

        reconcile_submitted_orders(
            &client,
            &pool,
            &fill_tx,
            &alert,
            &Arc::new(Semaphore::new(10)),
        )
        .await;

        let fill = tokio::time::timeout(std::time::Duration::from_millis(200), fill_rx.recv())
            .await
            .expect("reconcile should emit fill")
            .unwrap();
        assert_eq!(fill.symbol, "NVDA");
        assert_eq!(fill.filled_qty, 1); // ImmediateFillClient order_history 반환값 기준

        // DB state should be updated to PartiallyFilled
        // (submitted_qty=2 > filled_qty=1 → PartialFilled)
        let state: String = sqlx::query("SELECT state FROM orders WHERE id = 'stale-1'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("state");
        assert_eq!(state, "PartiallyFilled");
    }

    #[tokio::test]
    async fn partial_fill_emits_actual_filled_qty() {
        // Broker filled only 50 shares of a 100-share order.
        // FillInfo.filled_qty must reflect the broker's actual fill (50), not the
        // submitted qty (100). Downstream PositionTask uses filled_qty for position sizing.
        struct PartialFillClient;
        #[async_trait]
        impl kis_api::KisApi for PartialFillClient {
            async fn stream(&self) -> Result<kis_api::KisStream, kis_api::KisError> {
                unimplemented!()
            }
            async fn volume_ranking(
                &self,
                _: &Exchange,
                _: u32,
            ) -> Result<Vec<RankingItem>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn holidays(&self, _: &str) -> Result<Vec<kis_api::Holiday>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn place_order(
                &self,
                _: PlaceOrderRequest,
            ) -> Result<kis_api::PlaceOrderResponse, kis_api::KisError> {
                Ok(kis_api::PlaceOrderResponse {
                    order_date: "20260329".into(),
                    order_org_no: "partial-1".into(),
                    order_time: "103000".into(),
                })
            }
            async fn cancel_order(
                &self,
                _: CancelOrderRequest,
            ) -> Result<kis_api::CancelOrderResponse, kis_api::KisError> {
                unimplemented!()
            }
            async fn daily_chart(
                &self,
                _: kis_api::DailyChartRequest,
            ) -> Result<Vec<CandleBar>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, kis_api::KisError> {
                Ok(vec![]) // empty → broker considers it done
            }
            async fn news(&self, _: &str) -> Result<Vec<kis_api::NewsItem>, kis_api::KisError> {
                Ok(vec![])
            }
            async fn order_history(
                &self,
                _: OrderHistoryRequest,
            ) -> Result<Vec<OrderHistoryItem>, kis_api::KisError> {
                Ok(vec![OrderHistoryItem {
                    order_no: "partial-1".into(),
                    symbol: String::new(),
                    name: String::new(),
                    side_cd: "02".into(),
                    qty: dec!(100),       // submitted 100
                    filled_qty: dec!(50), // broker filled only 50
                    filled_price: dec!(130.00),
                    filled_date: "20260329".into(),
                    filled_time: "103000".into(),
                }])
            }
            async fn balance(&self) -> Result<kis_api::BalanceResponse, kis_api::KisError> {
                Ok(kis_api::BalanceResponse {
                    items: vec![],
                    summary: kis_api::BalanceSummary {
                        purchase_amount: rust_decimal::Decimal::ZERO,
                        available_cash: rust_decimal::Decimal::ZERO,
                        realized_pnl: rust_decimal::Decimal::ZERO,
                        total_pnl: rust_decimal::Decimal::ZERO,
                    },
                })
            }
            async fn check_deposit(&self) -> Result<kis_api::DepositInfo, kis_api::KisError> {
                Ok(kis_api::DepositInfo {
                    currency: "USD".to_string(),
                    amount: rust_decimal::Decimal::ZERO,
                })
            }
        }

        let pool = crate::db::connect(":memory:").await.unwrap();
        let (order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::Active,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn kis_api::KisApi> = Arc::new(PartialFillClient);

        tokio::spawn(async move {
            run_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        let mut req = make_req("NVDA");
        req.qty = 100;
        order_tx.send(req).await.unwrap();

        let fill = tokio::time::timeout(std::time::Duration::from_millis(500), fill_rx.recv())
            .await
            .expect("should receive fill for partial fill")
            .unwrap();

        assert_eq!(fill.symbol, "NVDA");
        assert_eq!(
            fill.filled_qty, 50,
            "FillInfo must carry broker-reported qty, not submitted qty"
        );
        token.cancel();
    }

    #[tokio::test]
    async fn reconcile_order_with_no_broker_id_marks_failed() {
        // broker_order_id IS NULL — place_order succeeded but ID was never persisted.
        let pool = crate::db::connect(":memory:").await.unwrap();

        sqlx::query(
            "INSERT INTO orders (id, symbol, side, state, order_type, qty, submitted_at, updated_at) \
             VALUES ('stale-2', 'AAPL', 'buy', 'Submitted', 'marketable_limit', '1', '2026-03-23T00:00:00Z', '2026-03-23T00:00:00Z')"
        )
        .execute(&pool)
        .await
        .unwrap();

        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let alert = AlertRouter::new(256);
        let client: Arc<dyn kis_api::KisApi> = Arc::new(ImmediateFillClient);

        reconcile_submitted_orders(
            &client,
            &pool,
            &fill_tx,
            &alert,
            &Arc::new(Semaphore::new(10)),
        )
        .await;

        // No fill should be emitted
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), fill_rx.recv()).await;
        assert!(
            result.is_err(),
            "no fill expected for order with missing broker_order_id"
        );

        let state: String = sqlx::query("SELECT state FROM orders WHERE id = 'stale-2'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("state");
        assert_eq!(state, "Failed");
    }

    struct KrImmediateFillClient;

    #[async_trait]
    impl KisDomesticApi for KrImmediateFillClient {
        async fn domestic_stream(&self) -> Result<KisStream, kis_api::KisError> {
            unimplemented!()
        }
        async fn domestic_volume_ranking(
            &self,
            _: &DomesticExchange,
            _: u32,
        ) -> Result<Vec<DomesticRankingItem>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn domestic_holidays(
            &self,
            _: &str,
        ) -> Result<Vec<kis_api::Holiday>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn domestic_place_order(
            &self,
            _: DomesticPlaceOrderRequest,
        ) -> Result<DomesticPlaceOrderResponse, kis_api::KisError> {
            Ok(DomesticPlaceOrderResponse {
                order_no: "KR-001".into(),
                order_date: "20260327".into(),
                order_time: "100000".into(),
            })
        }
        async fn domestic_cancel_order(
            &self,
            _: DomesticCancelOrderRequest,
        ) -> Result<DomesticCancelOrderResponse, kis_api::KisError> {
            unimplemented!()
        }
        async fn domestic_daily_chart(
            &self,
            _: DomesticDailyChartRequest,
        ) -> Result<Vec<CandleBar>, kis_api::KisError> {
            Ok(vec![])
        }
        async fn domestic_unfilled_orders(
            &self,
        ) -> Result<Vec<DomesticUnfilledOrder>, kis_api::KisError> {
            Ok(vec![]) // empty → already filled
        }
        async fn domestic_order_history(
            &self,
            _: DomesticOrderHistoryRequest,
        ) -> Result<Vec<DomesticOrderHistoryItem>, kis_api::KisError> {
            // "KR-001" = domestic_place_order 응답 + reconcile 테스트 시드와 일치
            Ok(vec![DomesticOrderHistoryItem {
                order_no: "KR-001".into(),
                symbol: String::new(),
                side_cd: "02".into(),
                qty: 2,
                filled_qty: 2,
                filled_price: dec!(75000),
                filled_date: "20260328".into(),
            }])
        }
        async fn domestic_balance(&self) -> Result<kis_api::BalanceResponse, kis_api::KisError> {
            Ok(kis_api::BalanceResponse {
                items: vec![],
                summary: kis_api::BalanceSummary {
                    purchase_amount: rust_decimal::Decimal::ZERO,
                    available_cash: rust_decimal::Decimal::ZERO,
                    realized_pnl: rust_decimal::Decimal::ZERO,
                    total_pnl: rust_decimal::Decimal::ZERO,
                },
            })
        }
    }

    #[tokio::test]
    async fn kr_active_order_produces_fill_on_immediate_fill() {
        let pool = crate::db::connect(":memory:").await.unwrap();
        let (order_tx, order_rx) = mpsc::channel(8);
        let (_force_tx, force_rx) = mpsc::channel(8);
        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let summary = Arc::new(RwLock::new(MarketSummary {
            bot_state: BotState::Active,
            ..MarketSummary::new()
        }));
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();
        let client: Arc<dyn KisDomesticApi> = Arc::new(KrImmediateFillClient);

        tokio::spawn(async move {
            run_kr_execution_task(
                order_rx,
                force_rx,
                fill_tx,
                client,
                pool,
                summary,
                alert,
                t,
                std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0)),
                Arc::new(Semaphore::new(10)),
            )
            .await;
        });

        order_tx.send(make_req("005930")).await.unwrap();
        let fill = tokio::time::timeout(std::time::Duration::from_millis(500), fill_rx.recv())
            .await
            .expect("should receive kr fill")
            .unwrap();
        assert_eq!(fill.symbol, "005930");
        token.cancel();
    }

    #[tokio::test]
    async fn kr_reconcile_stale_submitted_order_emits_fill() {
        let pool = crate::db::connect(":memory:").await.unwrap();

        sqlx::query(
            "INSERT INTO orders (id, broker_order_id, symbol, side, state, order_type, qty, price, submitted_at, updated_at) \
             VALUES ('kr-stale-1', 'KR-001', '005930', 'buy', 'Submitted', 'marketable_limit', '2', '75000', '2026-03-27T00:00:00Z', '2026-03-27T00:00:00Z')"
        )
        .execute(&pool)
        .await
        .unwrap();

        let (fill_tx, mut fill_rx) = mpsc::channel(8);
        let alert = AlertRouter::new(256);
        let client: Arc<dyn KisDomesticApi> = Arc::new(KrImmediateFillClient);

        reconcile_kr_submitted_orders(
            &client,
            &pool,
            &fill_tx,
            &alert,
            &Arc::new(Semaphore::new(10)),
        )
        .await;

        let fill = tokio::time::timeout(std::time::Duration::from_millis(200), fill_rx.recv())
            .await
            .expect("kr reconcile should emit fill")
            .unwrap();
        assert_eq!(fill.symbol, "005930");
        assert_eq!(fill.filled_qty, 2);

        let state: String = sqlx::query("SELECT state FROM orders WHERE id = 'kr-stale-1'")
            .fetch_one(&pool)
            .await
            .unwrap()
            .get("state");
        assert_eq!(state, "FullyFilled");
    }
}
