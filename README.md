# rs_kis_server

KIS (Korea Investment & Securities) trading bot framework library.

Provides infrastructure (DB, kill-switch, order execution, monitoring, pipeline orchestration) and **Strategy traits** that downstream binaries implement.

## Architecture

```
rs_kis              ← Pure KIS OpenAPI client library
rs_kis_server       ← This repo: bot framework + Strategy traits
rs_kis_pilot        ← Strategy implementations (binary)
```

`rs_kis_server` is a **library crate**. It does not run on its own.
A binary crate (e.g. `rs_kis_pilot`) implements `StrategyBundle` and calls `kis_server::run()`.

### Strategy Traits

Five async traits defined in `strategy.rs`:

| Trait | Responsibility |
|---|---|
| `DiscoveryStrategy` | Build watchlists |
| `RegimeStrategy` | Classify market regime |
| `SignalStrategy` | Generate trade signals |
| `QualificationStrategy` | Validate / filter signals |
| `RiskStrategy` | Position sizing / risk management |

### Modules

```
crates/server/src/
  run.rs             Entry point: run(config, strategies)
  strategy.rs        StrategyBundle + trait definitions
  types.rs           Shared domain types (Market, Position, OrderRequest, ...)
  config.rs          ServerConfig, MarketConfig, RiskConfig, ...
  pipeline/          tick, signal, execution, position, scheduler, regime, tuner, review
  db/                SQLite persistence (sqlx)
  control/           Kill-switch, recovery
  execution/         Order execution, OrderState
  monitoring/        Alerts, strategy monitor
  position/          Position management
  shared/            Telegram, REST API, control task, activity log
```

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
kis_server = { git = "https://github.com/soki1229/rs_kis_server.git", branch = "master" }
```

```rust
use kis_server::{ServerConfig, StrategyBundle, run};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = ServerConfig::from_file("config.toml")?;
    let strategies = StrategyBundle { /* ... */ };
    run(config, strategies).await
}
```

## Build & Test

```bash
cargo build
cargo test
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

## License

Private repository.
