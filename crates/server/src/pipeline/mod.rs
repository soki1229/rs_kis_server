pub mod generic_execution;
pub mod generic_position;
pub mod generic_regime;
pub mod scheduler;
pub mod signal;
pub mod stream;
pub mod tick;

pub use generic_execution::run_generic_execution_task;
pub use generic_position::run_generic_position_task;
pub use generic_regime::run_generic_regime_task;

use crate::monitoring::alert::AlertRouter;
use crate::types::{FillInfo, OrderRequest, QuoteSnapshot};
use rust_decimal::Decimal;
use tokio::sync::{broadcast, mpsc, watch};

/// 틱 데이터 (TickTask → SignalTask, PositionTask)
#[derive(Debug, Clone)]
pub struct TickData {
    pub symbol: String,
    pub price: Decimal,
    pub volume: u64,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

#[allow(dead_code)]
pub struct MarketPipeline {
    pub db_path: String,

    // 틱 배포
    pub tick_tx: broadcast::Sender<TickData>,
    pub tick_pos_tx: mpsc::Sender<TickData>,
    pub tick_pos_rx: mpsc::Receiver<TickData>,

    // watchlist 갱신
    pub watchlist_tx: watch::Sender<crate::types::WatchlistSet>,
    pub watchlist_rx: watch::Receiver<crate::types::WatchlistSet>,

    // 주문 흐름
    pub order_tx: mpsc::Sender<OrderRequest>,
    pub order_rx: mpsc::Receiver<OrderRequest>,
    pub force_order_tx: mpsc::Sender<OrderRequest>,
    pub force_order_rx: mpsc::Receiver<OrderRequest>,
    pub fill_tx: mpsc::Sender<FillInfo>,
    pub fill_rx: mpsc::Receiver<FillInfo>,

    // EOD 트리거 (mpsc for multi-day support — scheduler can send EOD each day)
    pub eod_tx: Option<tokio::sync::mpsc::Sender<()>>,
    pub eod_rx: Option<tokio::sync::mpsc::Receiver<()>>,

    // 호가 스냅샷 (SignalTask용)
    pub quote_tx: mpsc::Sender<QuoteSnapshot>,   // NEW
    pub quote_rx: mpsc::Receiver<QuoteSnapshot>, // NEW

    // 알림
    /// 기술적 상세 알림 — AlertBot 수신 (Critical / Warn / Info)
    pub alert: AlertRouter,
    /// 운영자용 핵심 요약 — MonitorBot 수신 (장 이벤트, 리포트, 시작/종료)
    pub summary_alert: AlertRouter,
}

impl MarketPipeline {
    pub fn new(db_path: impl Into<String>) -> Self {
        let (tick_tx, _) = broadcast::channel(4096);
        let (tick_pos_tx, tick_pos_rx) = mpsc::channel(4096);
        let (watchlist_tx, watchlist_rx) = watch::channel(crate::types::WatchlistSet::default());
        let (order_tx, order_rx) = mpsc::channel(64);
        let (force_order_tx, force_order_rx) = mpsc::channel(16);
        let (fill_tx, fill_rx) = mpsc::channel(64);
        let (quote_tx, quote_rx) = mpsc::channel(256);
        let (eod_s, eod_r) = tokio::sync::mpsc::channel(4);
        Self {
            db_path: db_path.into(),
            tick_tx,
            tick_pos_tx,
            tick_pos_rx,
            watchlist_tx,
            watchlist_rx,
            order_tx,
            order_rx,
            force_order_tx,
            force_order_rx,
            fill_tx,
            fill_rx,
            quote_tx,
            quote_rx,
            eod_tx: Some(eod_s),
            eod_rx: Some(eod_r),
            alert: AlertRouter::new(256),
            summary_alert: AlertRouter::new(64),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_channels_are_all_created() {
        let pipeline = MarketPipeline::new("/tmp/test.db");
        // tick_tx에서 subscribe 가능
        let _tick_rx = pipeline.tick_tx.subscribe();
        drop(pipeline);
    }

    #[test]
    fn pipeline_db_path_stored() {
        let p = MarketPipeline::new("/tmp/my.db");
        assert_eq!(p.db_path, "/tmp/my.db");
    }

    #[test]
    fn pipeline_has_quote_channels() {
        let p = MarketPipeline::new("/tmp/test.db");
        // quote_tx/rx are accessible
        drop(p.quote_tx);
        drop(p.quote_rx);
    }
}
