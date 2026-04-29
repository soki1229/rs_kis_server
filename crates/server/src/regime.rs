use crate::types::MarketRegime;

pub struct RegimeInput {
    pub ma5: f64,
    pub ma20: f64,
    pub daily_change_pct: f64,
    pub volume_ratio: f64,
    /// ADX-14. None when bars insufficient (<30).
    pub adx: Option<f64>,
}

pub type RegimeSender = tokio::sync::watch::Sender<MarketRegime>;
pub type RegimeReceiver = tokio::sync::watch::Receiver<MarketRegime>;

pub fn regime_channel(initial: MarketRegime) -> (RegimeSender, RegimeReceiver) {
    tokio::sync::watch::channel(initial)
}
