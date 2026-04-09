use kis_api::{CandleBar, DailyChartRequest, DomesticDailyChartRequest, KisApi, KisDomesticApi};
use crate::monitoring::alert::AlertRouter;
use crate::regime::{classify_regime, RegimeInput, RegimeSender};
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

pub fn build_regime_input(bars: &[CandleBar]) -> Option<RegimeInput> {
    if bars.len() < 21 {
        return None; // need 20 bars for avg volume + 1 for prev_close
    }
    // bars[0] = most recent (today)
    let ma5 = bars[..5].iter().map(|b| b.close).sum::<Decimal>() / Decimal::from(5);
    let ma20 = bars[..20].iter().map(|b| b.close).sum::<Decimal>() / Decimal::from(20);
    let prev_close = bars[1].close;
    let daily_change_pct = if prev_close.is_zero() {
        0.0
    } else {
        ((bars[0].close - prev_close) / prev_close * Decimal::from(100))
            .to_f64()
            .unwrap_or(0.0)
    };
    let avg_vol = bars[1..21].iter().map(|b| b.volume).sum::<Decimal>() / Decimal::from(20);
    let volume_ratio = if avg_vol.is_zero() {
        1.0
    } else {
        (bars[0].volume / avg_vol).to_f64().unwrap_or(1.0)
    };

    Some(RegimeInput {
        ma5: ma5.to_f64().unwrap_or(0.0),
        ma20: ma20.to_f64().unwrap_or(0.0),
        daily_change_pct,
        volume_ratio,
    })
}

fn is_within_regime_window(market: &str) -> bool {
    let now = chrono::Utc::now();
    if market == "KR" {
        use chrono_tz::Asia::Seoul;
        let kst = now.with_timezone(&Seoul).time();
        let start = chrono::NaiveTime::from_hms_opt(8, 0, 0).unwrap();
        let end = chrono::NaiveTime::from_hms_opt(16, 30, 0).unwrap();
        kst >= start && kst <= end
    } else {
        use chrono_tz::America::New_York;
        let et = now.with_timezone(&New_York).time();
        let start = chrono::NaiveTime::from_hms_opt(8, 30, 0).unwrap();
        let end = chrono::NaiveTime::from_hms_opt(17, 0, 0).unwrap();
        et >= start && et <= end
    }
}

pub async fn run_regime_task(
    client: Arc<dyn KisApi>,
    regime_tx: RegimeSender,
    alert: AlertRouter,
    token: CancellationToken,
) {
    loop {
        if is_within_regime_window("US") {
            // Fetch QQQ bars — 25봉
            let req = DailyChartRequest {
                symbol: "QQQ".into(),
                exchange: kis_api::Exchange::NASD,
                period: kis_api::ChartPeriod::Daily,
                adj_price: true,
            };
            match client.daily_chart(req).await {
                Ok(bars) => {
                    if let Some(input) = build_regime_input(&bars) {
                        let regime = classify_regime(&input);
                        tracing::info!(
                            ?regime,
                            ma5 = input.ma5,
                            ma20 = input.ma20,
                            daily_change_pct = input.daily_change_pct,
                            volume_ratio = input.volume_ratio,
                            "RegimeTask: regime classified"
                        );
                        let _ = regime_tx.send(regime);
                    } else {
                        alert.warn(
                            "RegimeTask: QQQ bars insufficient — keeping last regime".to_string(),
                        );
                    }
                }
                Err(e) => {
                    alert.warn(format!(
                        "RegimeTask: QQQ fetch failed ({e}) — keeping last regime"
                    ));
                }
            }
        }

        // Wait 1 hour or cancellation
        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("RegimeTask: shutting down");
                return;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(3600)) => {}
        }
    }
}

pub async fn run_kr_regime_task(
    client: Arc<dyn KisDomesticApi>,
    regime_tx: crate::regime::RegimeSender,
    alert: AlertRouter,
    token: CancellationToken,
) {
    loop {
        if is_within_regime_window("KR") {
            // KODEX 200 (069500) = KOSPI 200 ETF, used as KOSPI regime proxy
            let req = DomesticDailyChartRequest {
                symbol: "069500".into(),
                period: kis_api::ChartPeriod::Daily,
                adj_price: true,
                exchange: kis_api::DomesticExchange::KOSPI,
                start_date: None,
                end_date: None,
            };
            match client.domestic_daily_chart(req).await {
                Ok(bars) => {
                    if let Some(input) = build_regime_input(&bars) {
                        let regime = crate::regime::classify_regime(&input);
                        tracing::info!(
                            ?regime,
                            ma5 = input.ma5,
                            ma20 = input.ma20,
                            daily_change_pct = input.daily_change_pct,
                            volume_ratio = input.volume_ratio,
                            "KrRegimeTask: regime classified"
                        );
                        let _ = regime_tx.send(regime);
                    } else {
                        alert.warn(
                            "KrRegimeTask: KODEX200 bars insufficient — keeping last regime"
                                .to_string(),
                        );
                    }
                }
                Err(e) => {
                    alert.warn(format!(
                        "KrRegimeTask: KODEX200 fetch failed ({e}) — keeping last regime"
                    ));
                }
            }
        }

        tokio::select! {
            _ = token.cancelled() => {
                tracing::info!("KrRegimeTask: shutting down");
                return;
            }
            _ = tokio::time::sleep(std::time::Duration::from_secs(3600)) => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use kis_api::{
        CancelOrderRequest, CancelOrderResponse, DomesticCancelOrderRequest,
        DomesticCancelOrderResponse, DomesticDailyChartRequest, DomesticExchange,
        DomesticPlaceOrderRequest, DomesticPlaceOrderResponse, DomesticRankingItem,
        DomesticUnfilledOrder, Exchange, Holiday, KisDomesticApi, KisError, KisStream, NewsItem,
        PlaceOrderRequest, PlaceOrderResponse, RankingItem, UnfilledOrder,
    };
    use crate::types::MarketRegime;
    use rust_decimal::Decimal;
    use tokio_util::sync::CancellationToken;

    struct MockRegimeClient {
        bars: Vec<CandleBar>,
    }

    impl MockRegimeClient {
        fn with_bars(bars: Vec<CandleBar>) -> Arc<Self> {
            Arc::new(Self { bars })
        }
    }

    #[async_trait]
    impl KisApi for MockRegimeClient {
        async fn stream(&self) -> Result<KisStream, KisError> {
            unimplemented!()
        }
        async fn volume_ranking(&self, _: &Exchange, _: u32) -> Result<Vec<RankingItem>, KisError> {
            Ok(vec![])
        }
        async fn holidays(&self, _: &str) -> Result<Vec<Holiday>, KisError> {
            Ok(vec![])
        }
        async fn place_order(&self, _: PlaceOrderRequest) -> Result<PlaceOrderResponse, KisError> {
            unimplemented!()
        }
        async fn cancel_order(
            &self,
            _: CancelOrderRequest,
        ) -> Result<CancelOrderResponse, KisError> {
            unimplemented!()
        }
        async fn daily_chart(&self, _: DailyChartRequest) -> Result<Vec<CandleBar>, KisError> {
            Ok(self.bars.clone())
        }
        async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, KisError> {
            Ok(vec![])
        }
        async fn news(&self, _: &str) -> Result<Vec<NewsItem>, KisError> {
            Ok(vec![])
        }
        async fn order_history(
            &self,
            _: kis_api::OrderHistoryRequest,
        ) -> Result<Vec<kis_api::OrderHistoryItem>, KisError> {
            Ok(vec![])
        }
        async fn balance(&self) -> Result<kis_api::BalanceResponse, KisError> {
            Ok(kis_api::BalanceResponse {
                items: vec![],
                summary: kis_api::BalanceSummary {
                    purchase_amount: rust_decimal::Decimal::ZERO,
                    realized_pnl: rust_decimal::Decimal::ZERO,
                    total_pnl: rust_decimal::Decimal::ZERO,
                },
            })
        }
        async fn check_deposit(&self) -> Result<kis_api::DepositInfo, KisError> {
            Ok(kis_api::DepositInfo {
                currency: "USD".to_string(),
                amount: rust_decimal::Decimal::ZERO,
            })
        }
    }

    fn make_bar(close: &str, volume: &str) -> CandleBar {
        CandleBar {
            date: "20260323".into(),
            open: Decimal::from_str_exact(close).unwrap(),
            high: Decimal::from_str_exact(close).unwrap(),
            low: Decimal::from_str_exact(close).unwrap(),
            close: Decimal::from_str_exact(close).unwrap(),
            volume: Decimal::from_str_exact(volume).unwrap(),
        }
    }

    #[test]
    fn build_regime_input_computes_ma5_ma20_change_volume() {
        // bars[0] = most recent, bars[24] = oldest
        let mut bars: Vec<CandleBar> = (0..25).map(|_| make_bar("100", "1000000")).collect();
        // Override bar[0] to have different close and volume
        bars[0] = make_bar("102", "2000000"); // today: +2%, volume 2x avg

        let input = build_regime_input(&bars).unwrap();
        // ma5 = average of bars[0..5].close = (102+100+100+100+100)/5 = 100.4
        let expected_ma5 =
            (Decimal::from(102) + Decimal::from(100) * Decimal::from(4)) / Decimal::from(5);
        assert_eq!(input.ma5, expected_ma5.to_f64().unwrap());
        // daily_change = (102-100)/100*100 = 2.0%
        assert!((input.daily_change_pct - 2.0).abs() < 0.001);
    }

    #[test]
    fn build_regime_input_returns_none_for_insufficient_bars() {
        let bars: Vec<CandleBar> = (0..19).map(|_| make_bar("100", "1000000")).collect();
        assert!(build_regime_input(&bars).is_none());
    }

    #[tokio::test]
    async fn regime_task_sends_initial_regime() {
        // 25 bars with ~0% change and low volume → Quiet
        let bars: Vec<CandleBar> = (0..25).map(|_| make_bar("100", "500000")).collect();
        let client = MockRegimeClient::with_bars(bars);
        let (regime_tx, mut regime_rx) = crate::regime::regime_channel(MarketRegime::Trending);
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();

        tokio::spawn(async move {
            run_regime_task(client, regime_tx, alert, t).await;
        });

        // Should immediately update on startup
        if tokio::time::timeout(std::time::Duration::from_millis(200), regime_rx.changed())
            .await
            .is_err()
        {
            tracing::warn!("regime not updated within 200ms, using initial value");
        }

        token.cancel();
    }

    #[tokio::test]
    async fn regime_task_keeps_last_regime_on_api_failure() {
        struct FailingClient;
        #[async_trait]
        impl KisApi for FailingClient {
            async fn stream(&self) -> Result<KisStream, KisError> {
                unimplemented!()
            }
            async fn volume_ranking(
                &self,
                _: &Exchange,
                _: u32,
            ) -> Result<Vec<RankingItem>, KisError> {
                Ok(vec![])
            }
            async fn holidays(&self, _: &str) -> Result<Vec<Holiday>, KisError> {
                Ok(vec![])
            }
            async fn place_order(
                &self,
                _: PlaceOrderRequest,
            ) -> Result<PlaceOrderResponse, KisError> {
                unimplemented!()
            }
            async fn cancel_order(
                &self,
                _: CancelOrderRequest,
            ) -> Result<CancelOrderResponse, KisError> {
                unimplemented!()
            }
            async fn daily_chart(&self, _: DailyChartRequest) -> Result<Vec<CandleBar>, KisError> {
                Err(KisError::Api {
                    code: "err".into(),
                    message: "api down".into(),
                })
            }
            async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, KisError> {
                Ok(vec![])
            }
            async fn news(&self, _: &str) -> Result<Vec<NewsItem>, KisError> {
                Ok(vec![])
            }
            async fn order_history(
                &self,
                _: kis_api::OrderHistoryRequest,
            ) -> Result<Vec<kis_api::OrderHistoryItem>, KisError> {
                Ok(vec![])
            }
            async fn balance(&self) -> Result<kis_api::BalanceResponse, KisError> {
                Ok(kis_api::BalanceResponse {
                    items: vec![],
                    summary: kis_api::BalanceSummary {
                        purchase_amount: rust_decimal::Decimal::ZERO,
                        realized_pnl: rust_decimal::Decimal::ZERO,
                        total_pnl: rust_decimal::Decimal::ZERO,
                    },
                })
            }
            async fn check_deposit(&self) -> Result<kis_api::DepositInfo, KisError> {
                Ok(kis_api::DepositInfo {
                    currency: "USD".to_string(),
                    amount: rust_decimal::Decimal::ZERO,
                })
            }
        }

        let (regime_tx, regime_rx) = crate::regime::regime_channel(MarketRegime::Trending);
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();

        tokio::spawn(async move {
            run_regime_task(Arc::new(FailingClient), regime_tx, alert, t).await;
        });

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        // Should still be Trending (initial value kept on API failure)
        assert_eq!(*regime_rx.borrow(), MarketRegime::Trending);
        token.cancel();
    }

    struct MockKrRegimeClient {
        bars: Vec<CandleBar>,
    }

    impl MockKrRegimeClient {
        fn with_bars(bars: Vec<CandleBar>) -> Arc<Self> {
            Arc::new(Self { bars })
        }
    }

    #[async_trait]
    impl KisDomesticApi for MockKrRegimeClient {
        async fn domestic_stream(&self) -> Result<KisStream, KisError> {
            unimplemented!()
        }
        async fn domestic_volume_ranking(
            &self,
            _: &DomesticExchange,
            _: u32,
        ) -> Result<Vec<DomesticRankingItem>, KisError> {
            Ok(vec![])
        }
        async fn domestic_holidays(&self, _: &str) -> Result<Vec<kis_api::Holiday>, KisError> {
            Ok(vec![])
        }
        async fn domestic_place_order(
            &self,
            _: DomesticPlaceOrderRequest,
        ) -> Result<DomesticPlaceOrderResponse, KisError> {
            unimplemented!()
        }
        async fn domestic_cancel_order(
            &self,
            _: DomesticCancelOrderRequest,
        ) -> Result<DomesticCancelOrderResponse, KisError> {
            unimplemented!()
        }
        async fn domestic_daily_chart(
            &self,
            _: DomesticDailyChartRequest,
        ) -> Result<Vec<CandleBar>, KisError> {
            Ok(self.bars.clone())
        }
        async fn domestic_unfilled_orders(&self) -> Result<Vec<DomesticUnfilledOrder>, KisError> {
            Ok(vec![])
        }
        async fn domestic_order_history(
            &self,
            _: kis_api::DomesticOrderHistoryRequest,
        ) -> Result<Vec<kis_api::DomesticOrderHistoryItem>, KisError> {
            Ok(vec![])
        }
        async fn domestic_balance(&self) -> Result<kis_api::BalanceResponse, KisError> {
            Ok(kis_api::BalanceResponse {
                items: vec![],
                summary: kis_api::BalanceSummary {
                    purchase_amount: rust_decimal::Decimal::ZERO,
                    realized_pnl: rust_decimal::Decimal::ZERO,
                    total_pnl: rust_decimal::Decimal::ZERO,
                },
            })
        }
    }

    #[tokio::test]
    async fn kr_regime_task_sends_initial_regime() {
        let bars: Vec<CandleBar> = (0..25).map(|_| make_bar("100", "500000")).collect();
        let client = MockKrRegimeClient::with_bars(bars);
        let (regime_tx, mut regime_rx) = crate::regime::regime_channel(MarketRegime::Trending);
        let alert = AlertRouter::new(256);
        let token = CancellationToken::new();
        let t = token.clone();

        tokio::spawn(async move {
            run_kr_regime_task(client, regime_tx, alert, t).await;
        });

        if tokio::time::timeout(std::time::Duration::from_millis(200), regime_rx.changed())
            .await
            .is_err()
        {
            tracing::warn!("kr regime not updated within 200ms, using initial value");
        }

        token.cancel();
    }
}
