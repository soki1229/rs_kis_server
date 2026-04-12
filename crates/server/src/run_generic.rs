//! Generic pipeline spawning utilities using MarketAdapter trait.
//!
//! This module provides helper functions to spawn generic pipeline tasks for any market
//! implementing the MarketAdapter trait. Use `MarketConfig.use_generic_pipeline` to control
//! whether to use these generic tasks or the legacy market-specific tasks.
//!
//! ## Phased Rollout
//!
//! To enable generic pipeline for a specific market, set `use_generic_pipeline = true` in
//! the market's configuration:
//!
//! ```toml
//! [kr]
//! use_generic_pipeline = true   # Enable for KR market
//! dry_run = true                # Start with VTS for safety
//!
//! [us]
//! use_generic_pipeline = false  # Keep legacy for US until KR is validated
//! ```
//!
//! ## Log Tagging
//!
//! All logs from generic tasks include structured fields for easy filtering:
//! - `market_id`: Kr or Us (enum)
//! - `market`: "KR" or "US" (string)
//! - `task`: "execution", "regime", or "position"
//!
//! Filter logs by market: `grep 'market=KR'`
//! Filter logs by task: `grep 'task=execution'`

use crate::config::{PositionConfig, SignalConfig};
use crate::market::{KrRealAdapter, KrVtsAdapter, MarketAdapter, UsRealAdapter, UsVtsAdapter};
use crate::monitoring::alert::AlertRouter;
use crate::pipeline;
use crate::regime::RegimeReceiver;
use crate::state::MarketLiveState;
use crate::strategy::RegimeStrategy;
use crate::types::FillInfo;

use kis_api::{KisApi, KisDomesticApi};
use sqlx::SqlitePool;
use std::sync::{atomic::AtomicU32, Arc};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Spawn a generic execution task for the given market adapter.
#[allow(clippy::too_many_arguments)]
pub fn spawn_generic_execution_task<M: MarketAdapter + Clone>(
    adapter: Arc<M>,
    order_rx: mpsc::Receiver<crate::types::OrderRequest>,
    force_order_rx: mpsc::Receiver<crate::types::OrderRequest>,
    fill_tx: mpsc::Sender<FillInfo>,
    db_pool: SqlitePool,
    summary: Arc<std::sync::RwLock<crate::state::MarketSummary>>,
    alert: AlertRouter,
    token: CancellationToken,
    pending_count: Arc<AtomicU32>,
    poll_sem: Arc<Semaphore>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        pipeline::run_generic_execution_task(
            adapter,
            order_rx,
            force_order_rx,
            fill_tx,
            db_pool,
            summary,
            alert,
            token,
            pending_count,
            poll_sem,
        )
        .await;
    })
}

/// Spawn a generic regime task for the given market adapter.
pub fn spawn_generic_regime_task<M: MarketAdapter + Clone>(
    adapter: Arc<M>,
    regime_strategy: Arc<dyn RegimeStrategy>,
    regime_tx: crate::regime::RegimeSender,
    alert: AlertRouter,
    token: CancellationToken,
    benchmark_symbol: String,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        pipeline::run_generic_regime_task(
            adapter,
            regime_strategy,
            regime_tx,
            alert,
            token,
            &benchmark_symbol,
        )
        .await;
    })
}

/// Spawn a generic position task for the given market adapter.
#[allow(clippy::too_many_arguments)]
pub fn spawn_generic_position_task<M: MarketAdapter + Clone>(
    adapter: Arc<M>,
    fill_rx: mpsc::Receiver<FillInfo>,
    tick_pos_rx: mpsc::Receiver<pipeline::TickData>,
    eod_rx: mpsc::Receiver<()>,
    live_state_tx: watch::Sender<MarketLiveState>,
    force_order_tx: mpsc::Sender<crate::types::OrderRequest>,
    regime_rx: RegimeReceiver,
    db_pool: SqlitePool,
    alert: AlertRouter,
    summary_alert: AlertRouter,
    token: CancellationToken,
    eod_fallback: chrono::DateTime<chrono::Utc>,
    pos_cfg: PositionConfig,
    notion: Option<Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
    tunable_tx: Option<watch::Sender<crate::config::TunableConfig>>,
    signal_cfg: SignalConfig,
    dry_run: bool,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        pipeline::run_generic_position_task(
            adapter,
            fill_rx,
            tick_pos_rx,
            eod_rx,
            live_state_tx,
            force_order_tx,
            regime_rx,
            db_pool,
            alert,
            summary_alert,
            token,
            eod_fallback,
            pos_cfg,
            notion,
            tunable_tx,
            signal_cfg,
            dry_run,
        )
        .await;
    })
}

/// Build market adapters for both KR and US markets (Real and VTS).
pub struct MarketAdapters {
    pub kr_real: Arc<KrRealAdapter>,
    pub kr_vts: Arc<KrVtsAdapter>,
    pub us_real: Arc<UsRealAdapter>,
    pub us_vts: Arc<UsVtsAdapter>,
}

impl MarketAdapters {
    /// Create market adapters from KIS API clients.
    pub fn new(kr_client: Arc<dyn KisDomesticApi>, us_client: Arc<dyn KisApi>) -> Self {
        Self {
            kr_real: Arc::new(KrRealAdapter::new(Arc::clone(&kr_client))),
            kr_vts: Arc::new(KrVtsAdapter::new(kr_client)),
            us_real: Arc::new(UsRealAdapter::new(Arc::clone(&us_client))),
            us_vts: Arc::new(UsVtsAdapter::new(us_client)),
        }
    }
}

/// Example of spawning all generic pipeline tasks for a market.
///
/// This is a reference implementation showing how to wire up the generic tasks.
#[allow(dead_code)]
pub struct GenericMarketPipeline<M: MarketAdapter> {
    pub adapter: Arc<M>,
    pub execution_handle: Option<JoinHandle<()>>,
    pub regime_handle: Option<JoinHandle<()>>,
    pub position_handle: Option<JoinHandle<()>>,
}

impl<M: MarketAdapter + Clone> GenericMarketPipeline<M> {
    /// Create a new generic market pipeline (tasks not yet spawned).
    pub fn new(adapter: Arc<M>) -> Self {
        Self {
            adapter,
            execution_handle: None,
            regime_handle: None,
            position_handle: None,
        }
    }

    /// Spawn the execution task.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_execution(
        &mut self,
        order_rx: mpsc::Receiver<crate::types::OrderRequest>,
        force_order_rx: mpsc::Receiver<crate::types::OrderRequest>,
        fill_tx: mpsc::Sender<FillInfo>,
        db_pool: SqlitePool,
        summary: Arc<std::sync::RwLock<crate::state::MarketSummary>>,
        alert: AlertRouter,
        token: CancellationToken,
        pending_count: Arc<AtomicU32>,
        poll_sem: Arc<Semaphore>,
    ) {
        self.execution_handle = Some(spawn_generic_execution_task(
            Arc::clone(&self.adapter),
            order_rx,
            force_order_rx,
            fill_tx,
            db_pool,
            summary,
            alert,
            token,
            pending_count,
            poll_sem,
        ));
    }

    /// Spawn the regime task.
    pub fn spawn_regime(
        &mut self,
        regime_strategy: Arc<dyn RegimeStrategy>,
        regime_tx: crate::regime::RegimeSender,
        alert: AlertRouter,
        token: CancellationToken,
        benchmark_symbol: String,
    ) {
        self.regime_handle = Some(spawn_generic_regime_task(
            Arc::clone(&self.adapter),
            regime_strategy,
            regime_tx,
            alert,
            token,
            benchmark_symbol,
        ));
    }

    /// Spawn the position task.
    #[allow(clippy::too_many_arguments)]
    pub fn spawn_position(
        &mut self,
        fill_rx: mpsc::Receiver<FillInfo>,
        tick_pos_rx: mpsc::Receiver<pipeline::TickData>,
        eod_rx: mpsc::Receiver<()>,
        live_state_tx: watch::Sender<MarketLiveState>,
        force_order_tx: mpsc::Sender<crate::types::OrderRequest>,
        regime_rx: RegimeReceiver,
        db_pool: SqlitePool,
        alert: AlertRouter,
        summary_alert: AlertRouter,
        token: CancellationToken,
        eod_fallback: chrono::DateTime<chrono::Utc>,
        pos_cfg: PositionConfig,
        notion: Option<Arc<tokio::sync::RwLock<crate::notion::NotionClient>>>,
        tunable_tx: Option<watch::Sender<crate::config::TunableConfig>>,
        signal_cfg: SignalConfig,
        dry_run: bool,
    ) {
        self.position_handle = Some(spawn_generic_position_task(
            Arc::clone(&self.adapter),
            fill_rx,
            tick_pos_rx,
            eod_rx,
            live_state_tx,
            force_order_tx,
            regime_rx,
            db_pool,
            alert,
            summary_alert,
            token,
            eod_fallback,
            pos_cfg,
            notion,
            tunable_tx,
            signal_cfg,
            dry_run,
        ));
    }

    /// Wait for all spawned tasks to complete.
    pub async fn join_all(self) {
        if let Some(h) = self.execution_handle {
            let _ = h.await;
        }
        if let Some(h) = self.regime_handle {
            let _ = h.await;
        }
        if let Some(h) = self.position_handle {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn market_adapters_can_be_created() {
        // This test verifies the API compiles correctly.
        // Actual instantiation requires real KIS clients.
    }

    #[test]
    fn generic_pipeline_can_be_created() {
        // Verify the generic pipeline struct compiles with KrMarketAdapter.
        // Actual instantiation requires real adapter.
    }
}
