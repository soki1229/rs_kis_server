//! Market abstraction layer for unified trading operations across multiple markets.
//!
//! This module implements the Ports and Adapters (Hexagonal Architecture) pattern:
//! - `MarketAdapter` trait defines the port (interface)
//! - `KrMarketAdapter` and `UsMarketAdapter` are the adapters (implementations)

mod adapter;
mod kr;
mod types;
mod us;

pub use adapter::MarketAdapter;
pub use kr::KrMarketAdapter;
pub use types::*;
pub use us::UsMarketAdapter;
