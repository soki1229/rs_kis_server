//! Generic execution task using MarketAdapter trait.
//!
//! This module provides a unified execution pipeline that works with any market
//! through the MarketAdapter abstraction. It replaces the market-specific
//! `process_order` and `process_kr_order` functions.

use crate::market::{
    MarketAdapter, OrderMetadata, PollOutcome, UnifiedOrderRequest, UnifiedSide,
    UnifiedUnfilledOrder,
};
use crate::monitoring::alert::AlertRouter;
use crate::state::{BotState, MarketSummary};
use crate::types::{FillInfo, OrderRequest, Side};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
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
    twap_cfg: crate::config::TwapConfig,
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
                        let state = summary.read().expect("RwLock poisoned, cannot read MarketSummary").bot_state.clone();
                        if matches!(state, BotState::Active) {
                            let adapter_clone = Arc::clone(&adapter);
                            let (tx, db, al, ps) = (fill_tx.clone(), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            let tc = twap_cfg.clone();
                            handles.push(tokio::spawn(async move {
                                process_order(adapter_clone.as_ref(), r, &tx, &db, &al, tc, &ps).await
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
                        let state = summary.read().expect("RwLock poisoned, cannot read MarketSummary").bot_state.clone();
                        if matches!(state, BotState::HardBlocked) {
                            tracing::warn!(market = %market_name, "force order blocked (HardBlocked)");
                        } else {
                            let adapter_clone = Arc::clone(&adapter);
                            let (tx, db, al, ps) = (fill_tx.clone(), db_pool.clone(), alert.clone(), Arc::clone(&poll_sem));
                            let tc = twap_cfg.clone();
                            handles.push(tokio::spawn(async move {
                                process_order(adapter_clone.as_ref(), r, &tx, &db, &al, tc, &ps).await
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
    twap_cfg: crate::config::TwapConfig,
    poll_sem: &Arc<Semaphore>,
) {
    if req.side == Side::Buy && req.qty >= twap_cfg.threshold_qty && twap_cfg.slice_count > 1 {
        let slice_count = twap_cfg.slice_count;
        let base_qty = req.qty / slice_count as u64;
        let mut remaining_qty = req.qty % slice_count as u64;

        tracing::info!(
            symbol = %req.symbol,
            total_qty = req.qty,
            slices = slice_count,
            "Starting TWAP execution"
        );

        for i in 0..slice_count {
            let mut slice_qty = base_qty;
            if remaining_qty > 0 {
                slice_qty += 1;
                remaining_qty -= 1;
            }

            if slice_qty == 0 {
                continue;
            }

            let slice_req = OrderRequest {
                qty: slice_qty,
                ..req.clone()
            };

            if i > 0 {
                let delay_ms = rand::random::<u64>() % (twap_cfg.delay_secs_per_slice * 1000);
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }

            process_single_order(adapter, slice_req, fill_tx, db_pool, alert, poll_sem).await;
        }
    } else {
        process_single_order(adapter, req, fill_tx, db_pool, alert, poll_sem).await;
    }
}

async fn process_single_order(
    adapter: &dyn MarketAdapter,
    req: OrderRequest,
    fill_tx: &mpsc::Sender<FillInfo>,
    db_pool: &SqlitePool,
    alert: &AlertRouter,
    poll_sem: &Arc<Semaphore>,
) {
    // 0. Pre-flight: 주문 가능 수량 확인 (API 기반, soft check — API 실패 시 원래 수량으로 진행)
    let preflight_price = req.price.unwrap_or(Decimal::ZERO);
    let checked_qty = match req.side {
        Side::Buy => {
            adapter
                .check_buy_orderable(&req.symbol, preflight_price, req.qty)
                .await
        }
        Side::Sell => adapter.check_sell_orderable(&req.symbol, req.qty).await,
    };
    if checked_qty == 0 {
        tracing::warn!(
            symbol = %req.symbol,
            side = %req.side,
            requested = req.qty,
            "주문가능수량 0 — 주문 스킵 (check_{}orderable API 반환값 0)",
            if req.side == Side::Sell { "sell_" } else { "buy_" }
        );
        if req.side == Side::Sell {
            // exit_pending을 유지 — 30초 타임아웃이 누적되어 balance 확인 후 phantom 포지션 강제 제거
            // (FillInfo(filled_qty=0) 전송 시 exit_pending이 즉시 리셋되어 TS 재발동 루프 발생)
        }
        return;
    }
    let req = if checked_qty < req.qty {
        tracing::info!(
            symbol = %req.symbol,
            side = %req.side,
            requested = req.qty,
            orderable = checked_qty,
            "주문수량 조정 (가용 한도 내)"
        );
        OrderRequest {
            qty: checked_qty,
            ..req
        }
    } else {
        req
    };

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
        is_short: req.is_short,
    };

    // 1. Record in DB
    if let Err(e) = sqlx::query("INSERT INTO orders (id, symbol, side, qty, price, atr, state, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
        .bind(&order_id).bind(&req.symbol).bind(req.side.to_string()).bind(req.qty as i64).bind(req.price.map(|p| p.to_string())).bind(req.atr.map(|v| v.to_string())).bind("Created").bind(&now)
        .execute(db_pool).await {
        alert.warn(format!("DB error recording order {}: {}", req.symbol, e));
        return;
    }

    // 2. Place Order
    match adapter.place_order(unified_req).await {
        Ok(res) => {
            let submitted_at = chrono::Utc::now().to_rfc3339();
            if let Err(e) = sqlx::query("UPDATE orders SET broker_order_id = ?, state = 'Submitted', submitted_at = ?, updated_at = ? WHERE id = ?")
                .bind(&res.broker_id).bind(&submitted_at).bind(&submitted_at).bind(&order_id).execute(db_pool).await {
                tracing::error!(order_id = %order_id, broker_id = %res.broker_id, "Submitted 상태 DB 업데이트 실패: {e}");
                alert.warn(format!("[{}] {} broker_id DB 저장 실패 — 재시작 후 reconcile 불가", adapter.name(), req.symbol));
            }
            if res.broker_id.is_empty() {
                tracing::error!(order_id = %order_id, symbol = %req.symbol, "broker_id 공백 — 주문 추적 불가, 폴링 중단");
                return;
            }

            // 3. Poll for status — 각 이터레이션마다 신선한 타임스탬프 사용
            let start = Duration::from_secs(1);
            let mut interval = start;
            let mut last_reported_qty: u64 = 0;
            for attempt in 1..=30 {
                tokio::time::sleep(interval).await;
                interval = (interval + Duration::from_secs(1)).min(Duration::from_secs(5));
                let ts = chrono::Utc::now().to_rfc3339();

                let _permit = poll_sem.acquire().await.expect("poll semaphore closed");
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
                                if let Err(e) = sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                    .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&ts).bind(&order_id).execute(db_pool).await {
                                    tracing::error!(order_id = %order_id, "Filled: 'Filled' 상태 DB 업데이트 실패: {e}");
                                }

                                if let Some(order_price) = req.price {
                                    if !order_price.is_zero() {
                                        let slippage_pct = ((filled_price - order_price).abs()
                                            / order_price
                                            * rust_decimal::Decimal::from(100))
                                        .to_f64()
                                        .unwrap_or(0.0);
                                        if slippage_pct > 0.5 {
                                            tracing::warn!(
                                                symbol = %req.symbol,
                                                order_price = %order_price,
                                                filled_price = %filled_price,
                                                slippage_pct = format!("{:.3}%", slippage_pct),
                                                "High slippage detected"
                                            );
                                            sqlx::query(
                                                "INSERT INTO audit_log (event_type, market, symbol, detail, created_at) VALUES ('slippage_warn', ?, ?, ?, ?)",
                                            )
                                            .bind(adapter.market_id().label())
                                            .bind(&req.symbol)
                                            .bind(format!(
                                                "order={} fill={} slippage={:.3}%",
                                                order_price, filled_price, slippage_pct
                                            ))
                                            .bind(&ts)
                                            .execute(db_pool)
                                            .await
                                            .unwrap_or_else(|e| {
                                                tracing::error!(order_id = %order_id, "Slippage audit log DB insert failed: {e}");
                                                Default::default()
                                            });
                                        } else {
                                            tracing::info!(symbol = %req.symbol, qty = filled_qty, price = %filled_price, slippage_pct = format!("{:.3}%", slippage_pct), "Order filled");
                                        }
                                    }
                                } else {
                                    tracing::info!(symbol = %req.symbol, qty = filled_qty, price = %filled_price, "Order filled (market order)");
                                }

                                let delta = filled_qty.saturating_sub(last_reported_qty);
                                if delta > 0 {
                                    if let Err(e) = fill_tx
                                        .send(FillInfo {
                                            order_id: order_id.clone(),
                                            symbol: req.symbol.clone(),
                                            filled_qty: delta,
                                            filled_price,
                                            exchange_code: req.exchange_code.clone(),
                                            atr: req.atr,
                                            fatal: false,
                                        })
                                        .await
                                    {
                                        tracing::error!(symbol = %req.symbol, "Filled: fill_tx 전송 실패 — 포지션 태스크 종료?: {e}");
                                    }
                                }
                                return;
                            }
                            PollOutcome::PartialFilled {
                                filled_qty,
                                filled_price,
                            } => {
                                tracing::info!(symbol = %req.symbol, qty = filled_qty, "Order partially filled, continuing poll...");
                                if let Err(e) = sqlx::query("UPDATE orders SET filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                    .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&ts).bind(&order_id).execute(db_pool).await {
                                    tracing::warn!(order_id = %order_id, "PartialFilled DB 업데이트 실패: {e}");
                                }
                                let delta = filled_qty.saturating_sub(last_reported_qty);
                                if delta > 0 {
                                    if let Err(e) = fill_tx
                                        .send(FillInfo {
                                            order_id: order_id.clone(),
                                            symbol: req.symbol.clone(),
                                            filled_qty: delta,
                                            filled_price,
                                            exchange_code: req.exchange_code.clone(),
                                            atr: req.atr,
                                            fatal: false,
                                        })
                                        .await
                                    {
                                        tracing::error!(symbol = %req.symbol, "PartialFilled: fill_tx 전송 실패: {e}");
                                    } else {
                                        last_reported_qty = filled_qty;
                                    }
                                }
                            }
                            PollOutcome::Cancelled => {
                                if let Err(e) = sqlx::query("UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?")
                                    .bind(&ts).bind(&order_id).execute(db_pool).await {
                                    tracing::error!(order_id = %order_id, "Cancelled: 'Cancelled' 상태 DB 업데이트 실패: {e}");
                                }
                                alert.warn(format!("Order {} cancelled by broker", req.symbol));
                                return;
                            }
                            PollOutcome::Failed { reason } => {
                                if let Err(e) = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                                    .bind(&ts).bind(&order_id).execute(db_pool).await {
                                    tracing::error!(order_id = %order_id, "Failed: 'Failed' 상태 DB 업데이트 실패: {e}");
                                }
                                alert.warn(format!("Order {} failed: {}", req.symbol, reason));
                                let _ = fill_tx
                                    .send(FillInfo {
                                        order_id: order_id.clone(),
                                        symbol: req.symbol.clone(),
                                        filled_qty: 0,
                                        filled_price: Decimal::ZERO,
                                        exchange_code: req.exchange_code.clone(),
                                        atr: req.atr,
                                        fatal: false,
                                    })
                                    .await;
                                return;
                            }
                            PollOutcome::StillOpen => {
                                if attempt == 30 {
                                    tracing::warn!(symbol = %req.symbol, broker_id = %res.broker_id, "30회 폴링 후에도 미체결 — 주문 취소 시도");
                                    let cancel_order = UnifiedUnfilledOrder {
                                        order_no: res.broker_id.clone(),
                                        symbol: req.symbol.clone(),
                                        side: match req.side {
                                            Side::Buy => UnifiedSide::Buy,
                                            Side::Sell => UnifiedSide::Sell,
                                        },
                                        qty: req.qty,
                                        remaining_qty: req.qty.saturating_sub(last_reported_qty),
                                        price: req.price.unwrap_or(Decimal::ZERO),
                                        exchange_code: req.exchange_code.clone(),
                                    };

                                    // 취소/재확인 후 체결 내역 처리 공통 헬퍼
                                    let handle_recheck =
                                        |outcome: Result<PollOutcome, _>| -> Option<(u64, Decimal)> {
                                            match outcome {
                                                Ok(PollOutcome::Filled { filled_qty, filled_price }) => {
                                                    Some((filled_qty, filled_price))
                                                }
                                                Ok(PollOutcome::PartialFilled { filled_qty, filled_price }) => {
                                                    // 부분 체결이라도 알려진 수량을 포지션으로 등록
                                                    tracing::warn!(filled_qty, "재확인: 부분 체결 상태 — 체결된 수량만 처리");
                                                    Some((filled_qty, filled_price))
                                                }
                                                _ => None,
                                            }
                                        };

                                    match adapter.cancel_order(&cancel_order).await {
                                        Ok(true) => {
                                            if let Err(e) = sqlx::query("UPDATE orders SET state = 'Cancelled', updated_at = ? WHERE id = ?")
                                                .bind(&ts).bind(&order_id).execute(db_pool).await {
                                                tracing::error!(order_id = %order_id, "StillOpen 취소 후 DB 업데이트 실패: {e}");
                                            }
                                            alert.warn(format!(
                                                "[{}] {} 주문 30회 미체결 → 취소 완료",
                                                adapter.name(),
                                                req.symbol
                                            ));
                                            let _ = fill_tx.try_send(FillInfo {
                                                order_id: order_id.clone(),
                                                symbol: req.symbol.clone(),
                                                filled_qty: 0,
                                                filled_price: Decimal::ZERO,
                                                exchange_code: req.exchange_code.clone(),
                                                atr: req.atr,
                                                fatal: false,
                                            });
                                        }
                                        Ok(false) => {
                                            tracing::warn!(symbol = %req.symbol, "StillOpen 취소 거부 — 체결 여부 재확인");
                                            let recheck = adapter
                                                .poll_order_status(
                                                    &res.broker_id,
                                                    &req.symbol,
                                                    req.qty,
                                                )
                                                .await;
                                            if let Some((fq, fp)) = handle_recheck(recheck) {
                                                tracing::info!(symbol = %req.symbol, qty = fq, "취소 거부 후 체결 확인 — 정상 처리");
                                                let _ = sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                                    .bind(fq as i64).bind(fp.to_string()).bind(&ts).bind(&order_id).execute(db_pool).await;
                                                let _ = fill_tx
                                                    .send(FillInfo {
                                                        order_id: order_id.clone(),
                                                        symbol: req.symbol.clone(),
                                                        filled_qty: fq,
                                                        filled_price: fp,
                                                        exchange_code: req.exchange_code.clone(),
                                                        atr: req.atr,
                                                        fatal: false,
                                                    })
                                                    .await;
                                            } else {
                                                let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?").bind(&ts).bind(&order_id).execute(db_pool).await;
                                                let _ = fill_tx.try_send(FillInfo {
                                                    order_id: order_id.clone(),
                                                    symbol: req.symbol.clone(),
                                                    filled_qty: 0,
                                                    filled_price: Decimal::ZERO,
                                                    exchange_code: req.exchange_code.clone(),
                                                    atr: req.atr,
                                                    fatal: false,
                                                });
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(symbol = %req.symbol, "StillOpen 취소 API 오류({e}) — 체결 여부 재확인 (VTS 반영 대기)");
                                            // VTS는 체결 내역 API 반영에 수십 초가 걸릴 수 있음 — 단계적으로 재확인
                                            let delays = [15u64, 25, 35];
                                            let mut maybe_fill = None;
                                            for delay in delays {
                                                tokio::time::sleep(std::time::Duration::from_secs(
                                                    delay,
                                                ))
                                                .await;
                                                let recheck = adapter
                                                    .poll_order_status(
                                                        &res.broker_id,
                                                        &req.symbol,
                                                        req.qty,
                                                    )
                                                    .await;
                                                tracing::info!(symbol = %req.symbol, delay, "40330000 재확인: {:?}", recheck);
                                                if let Some(fill) = handle_recheck(recheck) {
                                                    maybe_fill = Some(fill);
                                                    break;
                                                }
                                            }
                                            if let Some((fq, fp)) = maybe_fill {
                                                tracing::info!(symbol = %req.symbol, qty = fq, "취소 오류 후 체결 확인 — 정상 처리");
                                                let _ = sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                                    .bind(fq as i64).bind(fp.to_string()).bind(&ts).bind(&order_id).execute(db_pool).await;
                                                let _ = fill_tx
                                                    .send(FillInfo {
                                                        order_id: order_id.clone(),
                                                        symbol: req.symbol.clone(),
                                                        filled_qty: fq,
                                                        filled_price: fp,
                                                        exchange_code: req.exchange_code.clone(),
                                                        atr: req.atr,
                                                        fatal: false,
                                                    })
                                                    .await;
                                            } else {
                                                // balance API fallback: inquire_daily_ccld보다 즉각 반영
                                                let balance_check =
                                                    adapter.balance().await.ok().and_then(|b| {
                                                        b.positions
                                                            .into_iter()
                                                            .find(|p| p.symbol == req.symbol)
                                                    });
                                                if let Some(pos) = balance_check {
                                                    let fq = pos
                                                        .qty
                                                        .to_u64()
                                                        .unwrap_or(req.qty)
                                                        .min(req.qty);
                                                    let fp = if pos.avg_price > Decimal::ZERO {
                                                        pos.avg_price
                                                    } else {
                                                        pos.current_price
                                                    };
                                                    tracing::info!(symbol = %req.symbol, qty = fq, price = %fp, "balance fallback 체결 확인 — 정상 처리");
                                                    let _ = sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ?, updated_at = ? WHERE id = ?")
                                                        .bind(fq as i64).bind(fp.to_string()).bind(&ts).bind(&order_id).execute(db_pool).await;
                                                    let _ = fill_tx
                                                        .send(FillInfo {
                                                            order_id: order_id.clone(),
                                                            symbol: req.symbol.clone(),
                                                            filled_qty: fq,
                                                            filled_price: fp,
                                                            exchange_code: req
                                                                .exchange_code
                                                                .clone(),
                                                            atr: req.atr,
                                                            fatal: false,
                                                        })
                                                        .await;
                                                } else {
                                                    tracing::warn!(symbol = %req.symbol, "취소 오류 + 체결 미확인 — Failed 처리 (balance sync에서 orphaned 복구 대기)");
                                                    let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?").bind(&ts).bind(&order_id).execute(db_pool).await;
                                                    let _ = fill_tx.try_send(FillInfo {
                                                        order_id: order_id.clone(),
                                                        symbol: req.symbol.clone(),
                                                        filled_qty: 0,
                                                        filled_price: Decimal::ZERO,
                                                        exchange_code: req.exchange_code.clone(),
                                                        atr: req.atr,
                                                        fatal: false,
                                                    });
                                                }
                                            }
                                        }
                                    }
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
            let market_label = adapter.market_id().label();
            let ts = chrono::Utc::now().to_rfc3339();
            if let Err(db_err) =
                sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                    .bind(&ts)
                    .bind(&order_id)
                    .execute(db_pool)
                    .await
            {
                tracing::error!(order_id = %order_id, "Place order failed cleanup: DB update to 'Failed' failed: {db_err}");
            }
            sqlx::query(
                "INSERT INTO audit_log (event_type, market, symbol, detail, created_at) VALUES ('order_failed', ?, ?, ?, ?)",
            )
            .bind(market_label)
            .bind(&req.symbol)
            .bind(format!("place_order error: {e}"))
            .bind(&ts)
            .execute(db_pool)
            .await
            .unwrap_or_else(|e| {
                tracing::error!(order_id = %order_id, "Place order failed audit log DB insert failed: {e}");
                Default::default()
            });
            tracing::error!(
                symbol = %req.symbol,
                side = %req.side,
                qty = req.qty,
                "place_order 실패: {e}"
            );
            alert.warn(format!("Order placement failed for {}: {}", req.symbol, e));
            // 브로커 잔고 없음 등 재시도 불가 오류 → position task에 fatal 신호 전송
            let err_str = e.to_string();
            let is_fatal = err_str.contains("잔고내역이 없습니다")
                || err_str.contains("40240000")
                || err_str.contains("잔고가 부족")
                || err_str.contains("주문가능수량이 없");
            if is_fatal && req.side == Side::Sell {
                tracing::error!(symbol = %req.symbol, "매도 불가 치명적 오류 — position task에 강제 제거 신호 전송");
                let _ = fill_tx
                    .send(FillInfo {
                        order_id: order_id.clone(),
                        symbol: req.symbol.clone(),
                        filled_qty: 0,
                        filled_price: Decimal::ZERO,
                        exchange_code: req.exchange_code.clone(),
                        atr: req.atr,
                        fatal: true,
                    })
                    .await;
            }
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
    let rows = match sqlx::query("SELECT id, broker_order_id, symbol, qty, atr, exchange_code FROM orders WHERE state = 'Submitted'")
        .fetch_all(db_pool).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!("reconcile: Submitted 주문 조회 실패 — 복구 건너뜀: {e}");
            return;
        }
    };

    for row in rows {
        let order_id: String = row.get("id");
        let broker_id: String = row.get("broker_order_id");
        let symbol: String = row.get("symbol");
        // broker_id 공백 = DB 저장 실패한 주문 → 추적 불가, Failed 처리
        if broker_id.is_empty() {
            tracing::warn!(order_id = %order_id, symbol = %symbol, "reconcile: broker_id 공백 — Failed 처리 후 스킵");
            let _ = sqlx::query("UPDATE orders SET state = 'Failed', updated_at = ? WHERE id = ?")
                .bind(chrono::Utc::now().to_rfc3339())
                .bind(&order_id)
                .execute(db_pool)
                .await;
            continue;
        }
        let qty: i64 = row.get::<String, _>("qty").parse().unwrap_or(0);
        let atr: Option<Decimal> = row.try_get::<Option<String>, _>("atr").ok().flatten().and_then(|s| {
            s.parse().map_err(|_| tracing::warn!(order_id = %order_id, "ATR parse failed, defaulting to None")).ok()
        });
        let ex_code: Option<String> = row.get("exchange_code");

        let _permit = poll_sem.acquire().await.expect("poll semaphore closed");
        match adapter
            .poll_order_status(&broker_id, &symbol, qty as u64)
            .await
        {
            Ok(outcome) => match outcome {
                PollOutcome::Filled {
                    filled_qty,
                    filled_price,
                } => {
                    if let Err(e) = sqlx::query("UPDATE orders SET state = 'Filled', filled_qty = ?, filled_price = ? WHERE id = ?")
                            .bind(filled_qty as i64).bind(filled_price.to_string()).bind(&order_id).execute(db_pool).await {
                        tracing::error!(order_id = %order_id, "reconcile: Filled 상태 DB 업데이트 실패: {e}");
                    }
                    // 이미 포지션이 DB에 존재하면 fill을 보내지 않음 (balance 복구와 중복 방지)
                    let pos_exists = sqlx::query_scalar::<_, i64>(
                        "SELECT COUNT(*) FROM positions WHERE symbol = ?",
                    )
                    .bind(&symbol)
                    .fetch_one(db_pool)
                    .await
                    .unwrap_or(0)
                        > 0;
                    if !pos_exists {
                        if let Err(e) = fill_tx
                            .send(FillInfo {
                                order_id: order_id.clone(),
                                symbol: symbol.clone(),
                                filled_qty,
                                filled_price,
                                atr,
                                exchange_code: ex_code,
                                fatal: false,
                            })
                            .await
                        {
                            tracing::error!(symbol = %symbol, "reconcile: fill_tx 전송 실패: {e}");
                        }
                    }
                }
                PollOutcome::Cancelled | PollOutcome::Failed { .. } => {
                    if let Err(e) =
                        sqlx::query("UPDATE orders SET state = 'Cancelled' WHERE id = ?")
                            .bind(&order_id)
                            .execute(db_pool)
                            .await
                    {
                        tracing::error!(order_id = %order_id, "Reconcile: 'Cancelled' 상태 DB 업데이트 실패: {e}");
                    }
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
        Err(e) => tracing::warn!("Failed to fetch unfilled orders on shutdown: {}", e),
    }
}
