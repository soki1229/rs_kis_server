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
pub async fn run_generic_execution_task<M: MarketAdapter>(
    adapter: Arc<M>,
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
    tracing::info!(market = %market_name, task = "execution", "shutdown complete");
}

/// Process a single order through the adapter.
#[tracing::instrument(
    skip(adapter, fill_tx, db_pool, alert, poll_sem),
    fields(
        market_id = ?adapter.market_id(),
        market = %adapter.name(),
        symbol = %req.symbol,
        side = ?req.side,
        qty = req.qty,
        price = ?req.price
    )
)]
async fn process_order<M: MarketAdapter>(
    adapter: &M,
    req: OrderRequest,
    fill_tx: &mpsc::Sender<FillInfo>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let order_id = Uuid::new_v4().to_string();
    let now = chrono::Utc::now().to_rfc3339();
    let market_name = adapter.name();

    // Insert order into DB
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
            "GenericExecutionTask[{}]: DB insert failed for {} — aborting order: {}",
            market_name, req.symbol, e
        ));
        return;
    }

    // Convert to unified order request
    let unified_req = UnifiedOrderRequest {
        symbol: req.symbol.clone(),
        side: req.side.clone().into(),
        qty: req.qty,
        price: req.price,
        atr: req.atr,
        strength: req.strength,
        metadata: OrderMetadata {
            exchange_code: req.exchange_code.clone(),
            exchange_hint: None,
        },
    };

    // Place order through adapter
    let result = match adapter.place_order(unified_req).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                symbol = %req.symbol,
                price = ?req.price,
                qty = req.qty,
                error = %e,
                "GenericExecutionTask[{}]: place_order API error", market_name
            );
            alert.warn(format!(
                "GenericExecutionTask[{}]: place_order failed for {} (qty={}, price={:?}): {}",
                market_name, req.symbol, req.qty, req.price, e
            ));
            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(&order_id)
                .execute(db_pool)
                .await;
            return;
        }
    };

    let broker_order_id = result.broker_id.clone();

    // Update DB with broker order ID and Submitted state
    let _ = sqlx::query(
        "UPDATE orders SET broker_order_id = ?, state = 'Submitted', updated_at = ? WHERE id = ?",
    )
    .bind(&broker_order_id)
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(&order_id)
    .execute(db_pool)
    .await;

    tracing::info!(
        "GenericExecutionTask[{}]: order submitted {} broker_id={}",
        market_name,
        req.symbol,
        broker_order_id
    );

    // Poll until filled
    poll_until_filled(
        adapter,
        &order_id,
        &broker_order_id,
        &req.symbol,
        req.qty,
        req.exchange_code,
        fill_tx,
        db_pool,
        alert,
        poll_sem,
    )
    .await;
}

/// Poll order status until filled, cancelled, or timeout.
#[allow(clippy::too_many_arguments)]
async fn poll_until_filled<M: MarketAdapter>(
    adapter: &M,
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
    let market_name = adapter.name();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let poll_interval = Duration::from_secs(5);

    loop {
        let outcome = {
            let _permit = poll_sem.acquire().await;
            adapter
                .poll_order_status(broker_order_id, symbol, qty)
                .await
        };

        match outcome {
            Ok(PollOutcome::Filled {
                filled_qty,
                filled_price,
            }) => {
                let _ = sqlx::query(
                    "UPDATE orders SET state = 'FullyFilled', updated_at = ? WHERE id = ?",
                )
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(order_id)
                .execute(db_pool)
                .await;

                tracing::info!(
                    "✅ [{}] [체결완료] {} {}주 (@{})",
                    market_name,
                    symbol,
                    filled_qty,
                    filled_price
                );
                alert.info(format!(
                    "✅ [{}] [체결완료] {} {}주 (@{})",
                    market_name, symbol, filled_qty, filled_price
                ));

                let _ = fill_tx
                    .send(FillInfo {
                        order_id: order_id.to_string(),
                        symbol: symbol.to_string(),
                        filled_qty,
                        filled_price,
                        exchange_code: exchange_code.clone(),
                    })
                    .await;
                return;
            }
            Ok(PollOutcome::PartialFilled {
                filled_qty,
                filled_price,
            }) => {
                tracing::warn!(
                    "🌗 [{}] [부분체결] {} {}/{}주 (@{})",
                    market_name,
                    symbol,
                    filled_qty,
                    qty,
                    filled_price
                );
                alert.warn(format!(
                    "🌗 [{}] [부분체결] {} {}/{}주 (@{}) - 잔량은 자동 취소되었습니다.",
                    market_name, symbol, filled_qty, qty, filled_price
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
                    })
                    .await;
                return;
            }
            Ok(PollOutcome::Cancelled) => {
                tracing::info!(
                    "⚪️ [{}] [취소됨] {} 주문이 취소되었습니다.",
                    market_name,
                    symbol
                );
                alert.warn(format!(
                    "⚪️ [{}] [취소됨] {} 주문이 취소되었습니다. (주문번호: {})",
                    market_name, symbol, broker_order_id
                ));
                let _ = sqlx::query(
                    "UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?",
                )
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(order_id)
                .execute(db_pool)
                .await;
                return;
            }
            Ok(PollOutcome::StillOpen) => {
                // Continue polling
            }
            Ok(PollOutcome::Failed { reason }) => {
                tracing::error!(
                    "GenericExecutionTask[{}]: poll_order_status failed for {}: {}",
                    market_name,
                    symbol,
                    reason
                );
                let _ =
                    sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                        .bind(chrono::Utc::now().to_rfc3339())
                        .bind(order_id)
                        .execute(db_pool)
                        .await;
                return;
            }
            Err(e) => {
                tracing::warn!(
                    "GenericExecutionTask[{}]: poll_order_status error for {}: {}",
                    market_name,
                    symbol,
                    e
                );
                // Continue polling, error might be transient
            }
        }

        // Check deadline
        if tokio::time::Instant::now() >= deadline {
            tracing::error!(
                order_id = %order_id,
                symbol = %symbol,
                broker_order_id = %broker_order_id,
                "GenericExecutionTask[{}]: polling deadline exceeded — attempting cancel", market_name
            );

            // Attempt to cancel
            if let Ok(unfilled) = adapter.unfilled_orders().await {
                if let Some(order) = unfilled.iter().find(|o| o.order_no == broker_order_id) {
                    match adapter.cancel_order(order).await {
                        Ok(_) => {
                            alert.warn(format!(
                                "GenericExecutionTask[{}]: order {} ({}) cancelled at broker after poll timeout",
                                market_name, broker_order_id, symbol
                            ));
                        }
                        Err(e) => {
                            alert.critical(format!(
                                "GenericExecutionTask[{}]: CRITICAL — failed to cancel order {} ({}): {} — MANUAL REVIEW NEEDED",
                                market_name, broker_order_id, symbol, e
                            ));
                        }
                    }
                }
            }

            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(order_id)
                .execute(db_pool)
                .await;
            return;
        }

        tokio::time::sleep(poll_interval).await;
    }
}

/// Reconcile any orders left in 'Submitted' state from previous run.
async fn reconcile_submitted_orders<M: MarketAdapter>(
    adapter: &M,
    db_pool: &SqlitePool,
    fill_tx: &mpsc::Sender<FillInfo>,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    let market_name = adapter.name();

    let rows = match sqlx::query(
        "SELECT id, broker_order_id, symbol, qty, price, exchange_code FROM orders WHERE state = 'Submitted'"
    )
    .fetch_all(db_pool)
    .await
    {
        Ok(r) => r,
        Err(e) => {
            alert.warn(format!(
                "GenericExecutionTask[{}]: reconciliation query failed: {}",
                market_name, e
            ));
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    tracing::info!(
        "GenericExecutionTask[{}]: reconciling {} stale Submitted order(s)",
        market_name,
        rows.len()
    );

    for row in rows {
        let order_id: String = row.get("id");
        let broker_order_id: Option<String> = row.try_get("broker_order_id").ok().flatten();
        let symbol: String = row.get("symbol");
        let qty: String = row.get("qty");
        let exchange_code: Option<String> = row.try_get("exchange_code").ok().flatten();

        let broker_order_no = match broker_order_id {
            Some(id) => id,
            None => {
                alert.warn(format!(
                    "GenericExecutionTask[{}]: stale order {} ({}) has no broker_order_id — marking Failed",
                    market_name, order_id, symbol
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
            "GenericExecutionTask[{}]: reconciling stale order {} ({}) broker_no={}",
            market_name, order_id, symbol, broker_order_no
        ));

        poll_until_filled(
            adapter,
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

/// Cancel all unfilled orders on shutdown.
async fn cancel_unfilled_on_shutdown<M: MarketAdapter>(adapter: &M, alert: &AlertRouter) {
    let market_name = adapter.name();

    let unfilled = match adapter.unfilled_orders().await {
        Ok(orders) => orders,
        Err(e) => {
            alert.warn(format!(
                "GenericExecutionTask[{}]: failed to fetch unfilled orders on shutdown: {}",
                market_name, e
            ));
            return;
        }
    };

    if unfilled.is_empty() {
        tracing::info!(
            "GenericExecutionTask[{}]: no unfilled orders to cancel",
            market_name
        );
        return;
    }

    tracing::info!(
        "GenericExecutionTask[{}]: cancelling {} unfilled order(s) on shutdown",
        market_name,
        unfilled.len()
    );

    for order in unfilled {
        match adapter.cancel_order(&order).await {
            Ok(_) => {
                tracing::info!(
                    "GenericExecutionTask[{}]: cancelled order {} ({})",
                    market_name,
                    order.order_no,
                    order.symbol
                );
            }
            Err(e) => {
                alert.warn(format!(
                    "GenericExecutionTask[{}]: failed to cancel order {} ({}): {}",
                    market_name, order.order_no, order.symbol, e
                ));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::BotError;
    use crate::market::{
        MarketId, MarketTiming, UnifiedBalance, UnifiedCandleBar, UnifiedDailyBar,
        UnifiedOrderHistoryItem, UnifiedOrderResult, UnifiedSide, UnifiedUnfilledOrder,
    };
    use async_trait::async_trait;
    use rust_decimal::Decimal;
    use rust_decimal_macros::dec;
    use std::sync::atomic::AtomicBool;

    struct MockAdapter {
        market_id: MarketId,
        fill_immediately: AtomicBool,
    }

    impl MockAdapter {
        fn new(market_id: MarketId) -> Self {
            Self {
                market_id,
                fill_immediately: AtomicBool::new(true),
            }
        }
    }

    #[async_trait]
    impl MarketAdapter for MockAdapter {
        fn market_id(&self) -> MarketId {
            self.market_id
        }

        fn name(&self) -> &'static str {
            match self.market_id {
                MarketId::Kr | MarketId::KrVts => "KR",
                MarketId::Us | MarketId::UsVts => "US",
            }
        }

        async fn place_order(
            &self,
            _req: UnifiedOrderRequest,
        ) -> Result<UnifiedOrderResult, BotError> {
            Ok(UnifiedOrderResult {
                internal_id: "test-internal".to_string(),
                broker_id: "test-broker-123".to_string(),
                symbol: "TEST".to_string(),
                side: UnifiedSide::Buy,
                qty: 1,
                price: Some(dec!(100.00)),
            })
        }

        async fn cancel_order(&self, _order: &UnifiedUnfilledOrder) -> Result<bool, BotError> {
            Ok(true)
        }

        async fn unfilled_orders(&self) -> Result<Vec<UnifiedUnfilledOrder>, BotError> {
            Ok(vec![])
        }

        async fn order_history(
            &self,
            _start: &str,
            _end: &str,
        ) -> Result<Vec<UnifiedOrderHistoryItem>, BotError> {
            Ok(vec![])
        }

        async fn poll_order_status(
            &self,
            _broker_order_no: &str,
            _symbol: &str,
            _expected_qty: u64,
        ) -> Result<PollOutcome, BotError> {
            if self.fill_immediately.load(Ordering::SeqCst) {
                Ok(PollOutcome::Filled {
                    filled_qty: 1,
                    filled_price: dec!(100.00),
                })
            } else {
                Ok(PollOutcome::StillOpen)
            }
        }

        async fn balance(&self) -> Result<UnifiedBalance, BotError> {
            Ok(UnifiedBalance {
                total_equity: dec!(10000),
                available_cash: dec!(10000),
                positions: vec![],
            })
        }

        async fn daily_chart(
            &self,
            _symbol: &str,
            _days: u32,
        ) -> Result<Vec<UnifiedDailyBar>, BotError> {
            Ok(vec![])
        }

        async fn intraday_candles(
            &self,
            _symbol: &str,
            _interval: u32,
        ) -> Result<Vec<UnifiedCandleBar>, BotError> {
            Ok(vec![])
        }

        async fn current_price(&self, _symbol: &str) -> Result<Decimal, BotError> {
            Ok(dec!(100.00))
        }

        fn market_timing(&self) -> MarketTiming {
            MarketTiming {
                is_open: true,
                mins_since_open: 60,
                mins_until_close: 300,
                is_holiday: false,
            }
        }

        async fn is_holiday(&self) -> Result<bool, BotError> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn mock_adapter_place_order_returns_result() {
        let adapter = MockAdapter::new(MarketId::Us);
        let req = UnifiedOrderRequest {
            symbol: "NVDA".to_string(),
            side: UnifiedSide::Buy,
            qty: 1,
            price: Some(dec!(130.00)),
            atr: None,
            strength: None,
            metadata: OrderMetadata::default(),
        };

        let result = adapter.place_order(req).await.unwrap();
        assert_eq!(result.broker_id, "test-broker-123");
    }

    #[tokio::test]
    async fn mock_adapter_poll_returns_filled() {
        let adapter = MockAdapter::new(MarketId::Kr);
        let outcome = adapter.poll_order_status("123", "TEST", 1).await.unwrap();

        match outcome {
            PollOutcome::Filled { filled_qty, .. } => assert_eq!(filled_qty, 1),
            _ => panic!("Expected Filled outcome"),
        }
    }
}
