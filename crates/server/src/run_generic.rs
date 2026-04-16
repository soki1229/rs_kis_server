//! Generic pipeline spawning utilities and adapter management.
//!
//! All market-specific pipelines have been unified into generic tasks that work
//! with any implementation of the MarketAdapter trait.

use crate::market::{KrRealAdapter, KrVtsAdapter, UsRealAdapter, UsVtsAdapter};
use kis_api::KisClient;
use std::sync::Arc;

/// Build and manage market adapters for all supported environments.
///
/// This structure holds the 4-tier adapters (Real/VTS for both KR/US),
/// allowing the framework to dynamically inject the correct environment
/// based on configuration.
pub struct MarketAdapters {
    pub kr_real: Arc<KrRealAdapter>,
    pub kr_vts: Arc<KrVtsAdapter>,
    pub us_real: Arc<UsRealAdapter>,
    pub us_vts: Arc<UsVtsAdapter>,
}

impl MarketAdapters {
    /// Create market adapters from KIS API clients.
    ///
    /// Loads four distinct clients to support both production and mock environments
    /// simultaneously if needed.
    pub fn new(
        kr_real: KisClient,
        us_real: KisClient,
        kr_vts: KisClient,
        us_vts: KisClient,
    ) -> Self {
        Self {
            kr_real: Arc::new(KrRealAdapter::new(kr_real)),
            kr_vts: Arc::new(KrVtsAdapter::new(kr_vts)),
            us_real: Arc::new(UsRealAdapter::new(us_real)),
            us_vts: Arc::new(UsVtsAdapter::new(us_vts)),
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
}
