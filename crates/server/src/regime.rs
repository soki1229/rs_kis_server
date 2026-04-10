use crate::types::MarketRegime;

pub struct RegimeInput {
    pub ma5: f64,
    pub ma20: f64,
    pub daily_change_pct: f64,
    pub volume_ratio: f64,
}

#[cfg(test)]
pub(crate) fn classify_regime(input: &RegimeInput) -> MarketRegime {
    let abs_change = input.daily_change_pct.abs();

    if abs_change >= 1.5 {
        return MarketRegime::Volatile;
    }

    if abs_change < 0.3 && input.volume_ratio < 0.8 {
        return MarketRegime::Quiet;
    }

    MarketRegime::Trending
}

pub type RegimeSender = tokio::sync::watch::Sender<MarketRegime>;
pub type RegimeReceiver = tokio::sync::watch::Receiver<MarketRegime>;

pub fn regime_channel(initial: MarketRegime) -> (RegimeSender, RegimeReceiver) {
    tokio::sync::watch::channel(initial)
}
