use axum::{
    body::Body,
    http::{Request, StatusCode},
    middleware::Next,
    response::Response,
};

/// REST POST 엔드포인트용 X-Admin-Token 미들웨어.
/// GET은 이 미들웨어를 통과하지 않음 (loopback 바인딩으로 외부 차단).
pub async fn require_admin_token(
    req: Request<Body>,
    next: Next,
    expected_token: String,
) -> Result<Response, StatusCode> {
    match req.headers().get("X-Admin-Token") {
        None => Err(StatusCode::UNAUTHORIZED),
        Some(v) if v.to_str().unwrap_or("") == expected_token => Ok(next.run(req).await),
        Some(_) => Err(StatusCode::FORBIDDEN),
    }
}

/// Telegram chat_id 화이트리스트 검증.
#[cfg(test)]
pub fn is_authorized_chat(chat_id: i64, whitelist: &[i64]) -> bool {
    whitelist.contains(&chat_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use axum::routing::post;
    use axum::Router;
    use axum_test::{TestResponse, TestServer};

    async fn dummy_handler() -> &'static str {
        "ok"
    }

    fn app(token: &str) -> Router {
        let admin_token = token.to_string();
        Router::new()
            .route("/command", post(dummy_handler))
            .layer(axum::middleware::from_fn(move |req, next| {
                let t = admin_token.clone();
                async move { require_admin_token(req, next, t).await }
            }))
    }

    #[tokio::test]
    async fn missing_token_returns_401() {
        let server = TestServer::new(app("secret"));
        let resp: TestResponse = server.post("/command").await;
        assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn wrong_token_returns_403() {
        let server = TestServer::new(app("secret"));
        let resp: TestResponse = server
            .post("/command")
            .add_header("X-Admin-Token", "wrong")
            .await;
        assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn correct_token_passes() {
        let server = TestServer::new(app("secret"));
        let resp: TestResponse = server
            .post("/command")
            .add_header("X-Admin-Token", "secret")
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);
    }

    #[test]
    fn valid_chat_id_passes() {
        assert!(is_authorized_chat(12345, &[12345, 99999]));
    }

    #[test]
    fn unknown_chat_id_rejected() {
        assert!(!is_authorized_chat(11111, &[12345, 99999]));
    }
}
