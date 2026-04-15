use kis_server::types::*;

#[test]
fn market_regime_serde_roundtrip() {
    let r = MarketRegime::Trending;
    let json = serde_json::to_string(&r).unwrap();
    let back: MarketRegime = serde_json::from_str(&json).unwrap();
    assert_eq!(r, back);
}

#[test]
fn order_state_partially_filled() {
    let s = OrderState::PartiallyFilled { filled_qty: 10 };
    let json = serde_json::to_string(&s).unwrap();
    let back: OrderState = serde_json::from_str(&json).unwrap();
    assert!(matches!(back, OrderState::PartiallyFilled { .. }));
}

#[test]
fn kill_switch_mode_debug() {
    let m = KillSwitchMode::Hard;
    assert_eq!(format!("{m:?}"), "Hard");
}

#[test]
fn watchlist_all_unique_deduplicates() {
    let wl = WatchlistSet {
        stable: vec!["NVDA".into(), "AAPL".into()],
        aggressive: vec!["AAPL".into(), "TSLA".into()],
    };
    let unique = wl.all_unique();
    assert_eq!(unique.len(), 3);
}
