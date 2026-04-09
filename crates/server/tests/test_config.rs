use kis_server::config::{ProfileName, RiskConfig, ServerConfig, SignalConfig};

#[test]
fn risk_config_default_passes_validation() {
    RiskConfig::default().validate().unwrap();
}

#[test]
fn risk_config_rejects_excessive_risk() {
    use rust_decimal_macros::dec;
    let cfg = RiskConfig {
        daily_loss_limit_pct: dec!(2.0),
        ..Default::default()
    };
    assert!(cfg.validate().is_err());
}

#[test]
fn signal_config_has_defaults() {
    let cfg = SignalConfig::default();
    assert_eq!(cfg.setup_score_min, 50);
    assert!(cfg.llm_enabled);
}

#[test]
fn profile_name_serde_roundtrip() {
    let p = ProfileName::Conservative;
    let json = serde_json::to_string(&p).unwrap();
    let back: ProfileName = serde_json::from_str(&json).unwrap();
    assert_eq!(p, back);
}
