//! Market abstraction layer for unified trading operations across multiple markets.
//!
//! This module implements the Ports and Adapters (Hexagonal Architecture) pattern:
//! - `MarketAdapter` trait defines the port (interface)
//! - `KrRealAdapter`, `KrVtsAdapter`, `UsRealAdapter`, `UsVtsAdapter` are the adapters (implementations)

mod adapter;
mod kr;
mod types;
mod us;

pub use adapter::{MarketAdapter, ReadOnlyAdapter};
pub use kr::{KrRealAdapter, KrVtsAdapter};
pub use types::*;
pub use us::{UsRealAdapter, UsVtsAdapter};
