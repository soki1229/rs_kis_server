use async_trait::async_trait;
use kis_api::{
    BalanceResponse, CancelOrderRequest, CancelOrderResponse, CandleBar, DailyChartRequest,
    DepositInfo, DomesticCancelOrderRequest, DomesticCancelOrderResponse,
    DomesticDailyChartRequest, DomesticExchange, DomesticOrderHistoryItem,
    DomesticOrderHistoryRequest, DomesticPlaceOrderRequest, DomesticPlaceOrderResponse,
    DomesticRankingItem, DomesticUnfilledOrder, Exchange, Holiday, KisApi, KisClient, KisConfig,
    KisDomesticApi, KisDomesticClient, KisError, KisStream, NewsItem, OrderHistoryItem,
    OrderHistoryRequest, PlaceOrderRequest, PlaceOrderResponse, RankingItem, UnfilledOrder,
};

/// Pre-built client pair for dual-token (dry_run) mode.
///
/// For each app-key (real / VTS), exactly one `KisClient` is created,
/// and its `TokenManager`, `ApprovalKeyManager`, `RateLimiter` and
/// `reqwest::Client` are shared with the corresponding `KisDomesticClient`
/// via `KisDomesticClient::from_client()`.
///
/// This prevents the KIS 1-request-per-minute token issuance rate limit
/// (`EGW00133`) from triggering when both overseas and domestic clients
/// try to obtain a token for the same app-key simultaneously.
pub struct DualClients {
    pub overseas: DualKisClient,
    pub domestic: DualKisDomesticClient,
}

impl DualClients {
    pub fn new(data_config: KisConfig, trade_config: KisConfig) -> Self {
        let data_overseas = KisClient::new(data_config);
        let trade_overseas = KisClient::new(trade_config);
        let data_domestic = KisDomesticClient::from_client(&data_overseas);
        let trade_domestic = KisDomesticClient::from_client(&trade_overseas);

        Self {
            overseas: DualKisClient {
                data: data_overseas,
                trade: trade_overseas,
            },
            domestic: DualKisDomesticClient {
                data: data_domestic,
                trade: trade_domestic,
            },
        }
    }
}

/// Overseas (US) dual-client: routes data APIs through the real-market client
/// and order/account APIs through the trade client (VTS or real depending on mode).
pub struct DualKisClient {
    data: KisClient,
    trade: KisClient,
}

impl DualKisClient {
    /// Create a US-only dual client sharing the given data/trade `KisClient` instances.
    /// Used when only US dry_run is needed (KR uses a separate real client).
    pub fn from_clients(data: KisClient, trade: KisClient) -> Self {
        Self { data, trade }
    }
}

#[async_trait]
impl KisApi for DualKisClient {
    async fn stream(&self) -> Result<KisStream, KisError> {
        self.data.stream().await
    }

    async fn volume_ranking(
        &self,
        exchange: &Exchange,
        count: u32,
    ) -> Result<Vec<RankingItem>, KisError> {
        self.data.volume_ranking(exchange, count).await
    }

    async fn holidays(&self, country: &str) -> Result<Vec<Holiday>, KisError> {
        self.data.holidays(country).await
    }

    async fn daily_chart(&self, req: DailyChartRequest) -> Result<Vec<CandleBar>, KisError> {
        self.data.daily_chart(req).await
    }

    async fn news(&self, symbol: &str) -> Result<Vec<NewsItem>, KisError> {
        self.data.news(symbol).await
    }

    async fn place_order(&self, req: PlaceOrderRequest) -> Result<PlaceOrderResponse, KisError> {
        self.trade.place_order(req).await
    }

    async fn cancel_order(&self, req: CancelOrderRequest) -> Result<CancelOrderResponse, KisError> {
        self.trade.cancel_order(req).await
    }

    async fn unfilled_orders(&self) -> Result<Vec<UnfilledOrder>, KisError> {
        self.trade.unfilled_orders().await
    }

    async fn order_history(
        &self,
        req: OrderHistoryRequest,
    ) -> Result<Vec<OrderHistoryItem>, KisError> {
        self.trade.order_history(req).await
    }

    async fn balance(&self) -> Result<BalanceResponse, KisError> {
        self.trade.balance().await
    }

    async fn check_deposit(&self) -> Result<DepositInfo, KisError> {
        self.trade.check_deposit().await
    }
}

/// Domestic (KR) dual-client: routes data APIs through the real-market client
/// and order/account APIs through the trade client (VTS or real depending on mode).
pub struct DualKisDomesticClient {
    data: KisDomesticClient,
    trade: KisDomesticClient,
}

impl DualKisDomesticClient {
    /// Create a KR-only dual client sharing the given data/trade `KisDomesticClient` instances.
    /// Used when only KR dry_run is needed (US uses a separate real client).
    pub fn from_clients(data: KisDomesticClient, trade: KisDomesticClient) -> Self {
        Self { data, trade }
    }
}

#[async_trait]
impl KisDomesticApi for DualKisDomesticClient {
    async fn domestic_stream(&self) -> Result<KisStream, KisError> {
        self.data.domestic_stream().await
    }

    async fn domestic_volume_ranking(
        &self,
        exchange: &DomesticExchange,
        count: u32,
    ) -> Result<Vec<DomesticRankingItem>, KisError> {
        self.data.domestic_volume_ranking(exchange, count).await
    }

    async fn domestic_holidays(&self, date: &str) -> Result<Vec<Holiday>, KisError> {
        self.data.domestic_holidays(date).await
    }

    async fn domestic_daily_chart(
        &self,
        req: DomesticDailyChartRequest,
    ) -> Result<Vec<CandleBar>, KisError> {
        self.data.domestic_daily_chart(req).await
    }

    async fn domestic_place_order(
        &self,
        req: DomesticPlaceOrderRequest,
    ) -> Result<DomesticPlaceOrderResponse, KisError> {
        self.trade.domestic_place_order(req).await
    }

    async fn domestic_cancel_order(
        &self,
        req: DomesticCancelOrderRequest,
    ) -> Result<DomesticCancelOrderResponse, KisError> {
        self.trade.domestic_cancel_order(req).await
    }

    async fn domestic_unfilled_orders(&self) -> Result<Vec<DomesticUnfilledOrder>, KisError> {
        self.trade.domestic_unfilled_orders().await
    }

    async fn domestic_order_history(
        &self,
        req: DomesticOrderHistoryRequest,
    ) -> Result<Vec<DomesticOrderHistoryItem>, KisError> {
        self.trade.domestic_order_history(req).await
    }

    async fn domestic_balance(&self) -> Result<BalanceResponse, KisError> {
        self.trade.domestic_balance().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dual_clients_shares_token_managers() {
        let real_cfg = KisConfig::builder()
            .app_key("test_real_key")
            .app_secret("test_real_secret")
            .account_num("12345678-01")
            .build()
            .unwrap();
        let vts_cfg = KisConfig::builder()
            .app_key("test_vts_key")
            .app_secret("test_vts_secret")
            .account_num("88888888-01")
            .mock(true)
            .build()
            .unwrap();

        let clients = DualClients::new(real_cfg, vts_cfg);
        let _us: &dyn KisApi = &clients.overseas;
        let _kr: &dyn KisDomesticApi = &clients.domestic;
    }
}
