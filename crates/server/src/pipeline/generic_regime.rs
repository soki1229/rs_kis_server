//! Generic regime task using MarketAdapter trait.

use crate::market::MarketAdapter;
use crate::monitoring::alert::AlertRouter;
use crate::regime::{RegimeInput, RegimeSender};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Build regime input from daily bars.
/// bars[0] = most recent day.
pub fn build_regime_input(bars: &[crate::market::UnifiedDailyBar]) -> Option<RegimeInput> {
    if bars.len() < 21 {
        return None;
    }

    let closes: Vec<Decimal> = bars.iter().map(|b| b.close).collect();
    let volumes: Vec<u64> = bars.iter().map(|b| b.volume).collect();

    let ma5 = closes[..5].iter().copied().sum::<Decimal>() / Decimal::from(5);
    let ma20 = closes[..20].iter().copied().sum::<Decimal>() / Decimal::from(20);
    let prev_close = closes[1];
    let daily_change_pct = if prev_close.is_zero() {
        0.0
    } else {
        ((closes[0] - prev_close) / prev_close * Decimal::from(100))
            .to_f64()
            .unwrap_or(0.0)
    };

    let avg_vol: u64 = volumes[1..21].iter().sum::<u64>() / 20;
    let volume_ratio = if avg_vol == 0 {
        1.0
    } else {
        volumes[0] as f64 / avg_vol as f64
    };

    Some(RegimeInput {
        ma5: ma5.to_f64().unwrap_or(0.0),
        ma20: ma20.to_f64().unwrap_or(0.0),
        daily_change_pct,
        volume_ratio,
    })
}

/// Generic regime task that works with any MarketAdapter.
pub async fn run_generic_regime_task(
    adapter: Arc<dyn MarketAdapter>,
    regime_strategy: Arc<dyn crate::strategy::RegimeStrategy>,
    regime_tx: RegimeSender,
    alert: AlertRouter,
    token: CancellationToken,
    benchmark_symbol: &str,
) {
    let market_id = adapter.market_id();
    let market_name = adapter.name();

    tracing::info!(
        market_id = ?market_id,
        market = %market_name,
        task = "regime",
        benchmark = %benchmark_symbol,
        "starting generic regime task"
    );

    loop {
        let timing = adapter.market_timing();

        // Only update regime during market hours (with buffer)
        let is_active_window = timing.is_open
            || (timing.mins_since_open < 60 && timing.mins_since_open != i64::MAX)
            || (timing.mins_until_close > -30 && timing.mins_until_close != i64::MAX);

        if is_active_window {
            match adapter.daily_chart(benchmark_symbol, 25).await {
                Ok(bars) => {
                    if let Some(input) = build_regime_input(&bars) {
                        let regime = regime_strategy.classify(&input);
                        tracing::info!(
                            market = %market_name,
                            task = "regime",
                            ?regime,
                            ma5 = input.ma5,
                            ma20 = input.ma20,
                            daily_change_pct = input.daily_change_pct,
                            volume_ratio = input.volume_ratio,
                            "regime classified"
                        );
                        let _ = regime_tx.send(regime);
                    } else {
                        tracing::warn!(market = %market_name, task = "regime", "bars insufficient — keeping last regime");
                        alert.warn(format!(
                            "[{}] {} bars insufficient — keeping last regime",
                            market_name, benchmark_symbol
                        ));
                    }
                }
                Err(e) => {
                    tracing::warn!(market = %market_name, task = "regime", error = %e, "benchmark fetch failed — keeping last regime");
                    alert.warn(format!(
                        "[{}] {} fetch failed ({}) — keeping last regime",
                        market_name, benchmark_symbol, e
                    ));
                }
            }
        } else {
            tracing::debug!(market = %market_name, task = "regime", "outside market window, skipping update");
        }

        // Wait 1 hour or cancellation
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!(market = %market_name, task = "regime", "shutdown");
                return;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(3600)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;

    fn make_bars(count: usize) -> Vec<crate::market::UnifiedDailyBar> {
        (0..count)
            .map(|i| {
                let day = ((10 + 28 - (i % 28)) % 28) as u32;
                let day = if day == 0 { 28 } else { day };
                crate::market::UnifiedDailyBar {
                    date: NaiveDate::from_ymd_opt(2026, 4, day).unwrap(),
                    open: dec!(100),
                    high: dec!(105),
                    low: dec!(95),
                    close: dec!(100) + Decimal::from(i as i64),
                    volume: 1000000,
                }
            })
            .collect()
    }

    #[test]
    fn build_regime_input_from_bars() {
        let bars = make_bars(25);
        let input = build_regime_input(&bars);
        assert!(input.is_some());
        let input = input.unwrap();
        assert!(input.ma5 > 0.0);
        assert!(input.ma20 > 0.0);
    }

    #[test]
    fn build_regime_input_insufficient_bars() {
        let bars = make_bars(10);
        let input = build_regime_input(&bars);
        assert!(input.is_none());
    }
}
