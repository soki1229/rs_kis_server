//! Generic execution task using MarketAdapter trait.
//!
//! This module provides a unified execution pipeline that works with any market
//! through the MarketAdapter abstraction. It replaces the market-specific
//! `process_order` and `process_kr_order` functions.

use crate::market::{MarketAdapter, OrderMetadata, PollOutcome, UnifiedOrderRequest};
use crate::monitoring::alert::AlertRouter;
use crate::state::{BotState, MarketSummary};
use crate::types::{FillInfo, OrderRequest, Side};
use sqlx::{Row, SqlitePool};
use std::sync::{
    atomic::{AtomicU32, Ordering},
    Arc, RwLock,
};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

/// Generic execution task that works with any MarketAdapter.
#[allow(clippy::too_many_arguments)]
pub async fn run_generic_execution_task(
    adapter: Arc<dyn MarketAdapter>,
    mut order_rx: mpsc::Receiver<OrderRequest>,
    mut force_order_rx: mpsc::Receiver<OrderRequest>,
    fill_tx: mpsc::Sender<FillInfo>,
    db_pool: SqlitePool,
    summary: Arc<RwLock<MarketSummary>>,
    alert: AlertRouter,
    token: CancellationToken,
    pending_count: Arc<AtomicU32>,
    poll_sem: Arc<Semaphore>,
) {
    let market_id = adapter.market_id();
    let market_name = adapter.name();

    tracing::info!(
        market_id = ?market_id,
        market = %market_name,
        task = "execution",
        "starting generic execution task"
    );

    // Reconcile any orders left in 'Submitted' state from previous run
    reconcile_submitted_orders(adapter.as_ref(), &db_pool, &fill_tx, &alert, &poll_sem).await;

    let mut handles: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!(market = %market_name, task = "execution", "initiating graceful shutdown");
                cancel_unfilled_on_shutdown(adapter.as_ref(), &alert).await;
                break;
            }
            req = order_rx.recv() => {
                match req {
                    None => break,
                    Some(r) => {
                        pending_count.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |v| Some(v.saturating_sub(1))).ok();
                        let state = summary.read().unwrap_or_else(|e| e.into_inner()).bot_state.clone();
                        if matches!(state, BotState::Active) {
                            let adapter_clone = Arc::clone(&adapter);
                            let (tx, db, al, ps) = (fill_tx.clone(), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move {
                                process_order(adapter_clone.as_ref(), r, &tx, &db, &al, &ps).await
                            }));
                        } else {
                            tracing::warn!(market = %market_name, state = ?state, "regular order blocked due to bot state");
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
                            tracing::warn!(market = %market_name, "force order blocked (HardBlocked)");
                        } else {
                            let adapter_clone = Arc::clone(&adapter);
                            let (tx, db, al, ps) = (fill_tx.clone(), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            handles.push(tokio::spawn(async move {
                                process_order(adapter_clone.as_ref(), r, &tx, &db, &al, &ps).await
                            }));
                        }
                        handles.retain(|h| !h.is_finished());
                    }
                }
            }
        }
    }

    for h in handles {
        let _ = h.await;
    }
}

async fn process_order(
    adapter: &dyn MarketAdapter,
    req: OrderRequest,
    fill_tx: &mpsc::Sender<FillInfo>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let order_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();

    let metadata = OrderMetadata {
        exchange_code: req.exchange_code.clone(),
        exchange_hint: None,
    };

    let unified_req = UnifiedOrderRequest {
        symbol: req.symbol.clone(),
        side: match req.side {
            Side::Buy => crate::market::UnifiedSide::Buy,
            Side::Sell => crate::market::UnifiedSide::Sell,
        },
        qty: req.qty,
        price: req.price,
        atr: req.atr,
        metadata,
        strength: req.strength,
    };

    // 1. Record in DB
    if let Err(e) = sqlx::query("INSERT INTO orders (id, symbol, side, qty, price, state, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)")
        .bind(&order_id).bind(&req.symbol).bind(format!("{:?}", req.side)).bind(req.qty as i64).bind(req.price.map(|p| p.to_string())).bind("Created").bind(&now)
        .execute(db_pool).await {
        alert.warn(format!("DB error recording order {}: {}", req.symbol, e));
        return;
    }

    // 2. Place Order
    match adapter.place_order(unified_req).await {
        Ok(res) => {
            sqlx::query("UPDATE orders SET broker_order_id = ?, state = 'Submitted', updated_at = ? WHERE id = ?")
                .bind(&res.broker_id).bind(&now).bind(&order_id).execute(db_pool).await.ok();

            // 3. Poll for status
            let start = Duration::from_secs(1);
            let mut interval = start;
            for attempt in 1..=30 {
                tokio::time::sleep(interval).await;
                interval = (interval + Duration::from_secs(1)).min(Duration::from_secs(5));

                let _permit = poll_sem.acquire().await.unwrap();
                match adapter
                    .poll_order_status(&res.broker_id, &req.symbol, req.qty)
                    .await
                {
                    Ok(outcome) => {
                        match outcome {
                            PollOutcome::Filled {
                                filled_qty,
                                filled_price,
                            } => {
                                sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                    .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&now).bind(&order_id).execute(db_pool).await.ok();
                                fill_tx
                                    .send(FillInfo {
                                        order_id: order_id.clone(),
                                        symbol: req.symbol.clone(),
                                        filled_qty,
                                        filled_price,
                                        exchange_code: req.exchange_code.clone(),
                                    })
                                    .await
                                    .ok();
                                tracing::info!(symbol = %req.symbol, qty = filled_qty, price = %filled_price, "Order filled");
                                return;
                            }
                            PollOutcome::PartialFilled {
                                filled_qty,
                                filled_price,
                            } => {
                                tracing::info!(symbol = %req.symbol, qty = filled_qty, "Order partially filled, continuing poll...");
                                sqlx::query("UPDATE orders SET filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                    .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&now).bind(&order_id).execute(db_pool).await.ok();
                            }
                            PollOutcome::Cancelled => {
                                sqlx::query("UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?")
                                    .bind(&now).bind(&order_id).execute(db_pool).await.ok();
                                alert.warn(format!("Order {} cancelled by broker", req.symbol));
                                return;
                            }
                            PollOutcome::Failed { reason } => {
                                sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                                    .bind(&now).bind(&order_id).execute(db_pool).await.ok();
                                alert.warn(format!("Order {} failed: {}", req.symbol, reason));
                                return;
                            }
                            PollOutcome::StillOpen => {
                                if attempt == 30 {
                                    tracing::warn!(symbol = %req.symbol, "Order still open after 30 attempts, giving up poll");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(symbol = %req.symbol, "Error polling order status: {}", e);
                    }
                }
            }
        }
        Err(e) => {
            sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(&now)
                .bind(&order_id)
                .execute(db_pool)
                .await
                .ok();
            alert.warn(format!("Order placement failed for {}: {}", req.symbol, e));
        }
    }
}

async fn reconcile_submitted_orders(
    adapter: &dyn MarketAdapter,
    db_pool: &SqlitePool,
    fill_tx: &mpsc::Sender<FillInfo>,
    _alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let rows = sqlx::query("SELECT id, broker_order_id, symbol, qty, exchange_code FROM orders WHERE state = 'Submitted'")
        .fetch_all(db_pool).await.unwrap_or_default();

    for row in rows {
        let order_id: String = row.get("id");
        let broker_id: String = row.get("broker_order_id");
        let symbol: String = row.get("symbol");
        let qty: i64 = row.get("qty");
        let ex_code: Option<String> = row.get("exchange_code");

        let _permit = poll_sem.acquire().await.unwrap();
        match adapter
            .poll_order_status(&broker_id, &symbol, qty as u64)
            .await
        {
            Ok(outcome) => match outcome {
                PollOutcome::Filled {
                    filled_qty,
                    filled_price,
                } => {
                    sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ? WHERE id = ?")
                            .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&order_id).execute(db_pool).await.ok();
                    fill_tx
                        .send(FillInfo {
                            order_id,
                            symbol,
                            filled_qty,
                            filled_price,
                            exchange_code: ex_code,
                        })
                        .await
                        .ok();
                }
                PollOutcome::Cancelled => {
                    sqlx::query("UPDATE orders SET state = 'Cancelled' WHERE id = ?")
                        .bind(&order_id)
                        .execute(db_pool)
                        .await
                        .ok();
                }
                _ => {}
            },
            Err(e) => {
                tracing::warn!("Reconciliation failed for {}: {}", symbol, e);
            }
        }
    }
}

async fn cancel_unfilled_on_shutdown(adapter: &dyn MarketAdapter, alert: &AlertRouter) {
    match adapter.unfilled_orders().await {
        Ok(orders) => {
            for order in orders {
                match adapter.cancel_order(&order).await {
                    Ok(_) => {
                        tracing::info!(symbol = %order.symbol, "Cancelled unfilled order on shutdown")
                    }
                    Err(e) => alert.warn(format!(
                        "Failed to cancel {} on shutdown: {}",
                        order.symbol, e
                    )),
                }
            }
        }
        Err(e) => tracing::error!("Failed to fetch unfilled orders on shutdown: {}", e),
    }
}
