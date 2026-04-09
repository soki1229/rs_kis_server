use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn cancellation_token_completes_immediately() {
    let token = CancellationToken::new();
    let t = token.clone();
    token.cancel();
    let result = tokio::time::timeout(std::time::Duration::from_secs(1), t.cancelled()).await;
    assert!(result.is_ok(), "cancelled() should resolve immediately after cancel()");
}
