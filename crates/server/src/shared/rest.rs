use crate::auth::require_admin_token;
use crate::state::PipelineState;
use crate::types::{BotCommand, WatchlistPatch, WatchlistSet};
use axum::{
    extract::State as AxumState,
    http::StatusCode,
    middleware,
    response::Json,
    routing::{get, post},
    Router,
};
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};
use std::time::Instant;
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct AppState {
    kr: PipelineState,
    us: PipelineState,
    kr_cmd_tx: mpsc::Sender<BotCommand>,
    us_cmd_tx: mpsc::Sender<BotCommand>,
    kr_watchlist_rx: watch::Receiver<WatchlistSet>,
    us_watchlist_rx: watch::Receiver<WatchlistSet>,
    started_at: Instant,
}

#[derive(Deserialize)]
struct CommandBody {
    command: String,
}

#[derive(Deserialize)]
struct WatchlistPatchBody {
    #[serde(default)]
    add: Vec<String>,
    #[serde(default)]
    remove: Vec<String>,
}

pub fn build_router(
    kr: PipelineState,
    us: PipelineState,
    kr_cmd_tx: mpsc::Sender<BotCommand>,
    us_cmd_tx: mpsc::Sender<BotCommand>,
    kr_watchlist_rx: watch::Receiver<WatchlistSet>,
    us_watchlist_rx: watch::Receiver<WatchlistSet>,
    admin_token: String,
) -> Router {
    let state = AppState {
        kr,
        us,
        kr_cmd_tx: kr_cmd_tx.clone(),
        us_cmd_tx: us_cmd_tx.clone(),
        kr_watchlist_rx,
        us_watchlist_rx,
        started_at: Instant::now(),
    };

    let auth_layer = |token: String| {
        middleware::from_fn(move |req, next| {
            let t = token.clone();
            async move { require_admin_token(req, next, t).await }
        })
    };

    let token_read = admin_token.clone();
    let token_cmd_kr = admin_token.clone();
    let token_cmd_us = admin_token.clone();
    let token_wl_kr = admin_token.clone();
    let token_wl_us = admin_token.clone();

    Router::new()
        // Public: health check only (no sensitive data)
        .route("/health", get(health_handler))
        // Protected: all status/positions/stats endpoints require auth
        .route(
            "/kr/status",
            get(kr_status_handler).layer(auth_layer(token_read.clone())),
        )
        .route(
            "/kr/positions",
            get(kr_positions_handler).layer(auth_layer(token_read.clone())),
        )
        .route(
            "/kr/stats",
            get(kr_stats_handler).layer(auth_layer(token_read.clone())),
        )
        .route(
            "/us/status",
            get(us_status_handler).layer(auth_layer(token_read.clone())),
        )
        .route(
            "/us/positions",
            get(us_positions_handler).layer(auth_layer(token_read.clone())),
        )
        .route(
            "/us/stats",
            get(us_stats_handler).layer(auth_layer(token_read)),
        )
        .route(
            "/kr/command",
            post(kr_command_handler).layer(auth_layer(token_cmd_kr)),
        )
        .route(
            "/us/command",
            post(us_command_handler).layer(auth_layer(token_cmd_us)),
        )
        // Watchlist runtime patch: GET(조회), POST(추가/제거), DELETE(패치 초기화)
        .route(
            "/kr/watchlist",
            get(kr_watchlist_get_handler)
                .post(kr_watchlist_post_handler)
                .delete(kr_watchlist_delete_handler)
                .layer(auth_layer(token_wl_kr)),
        )
        .route(
            "/us/watchlist",
            get(us_watchlist_get_handler)
                .post(us_watchlist_post_handler)
                .delete(us_watchlist_delete_handler)
                .layer(auth_layer(token_wl_us)),
        )
        .with_state(state)
}

async fn health_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    let uptime_secs = s.started_at.elapsed().as_secs();
    // Only expose minimal info publicly; bot states require authentication
    Json(serde_json::json!({
        "status": "ok",
        "uptime_secs": uptime_secs,
    }))
}

async fn kr_status_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    let summary = s.kr.summary.read().unwrap().clone();
    let live = s.kr.live_state_rx.borrow().clone();
    Json(serde_json::json!({ "summary": summary, "live": live }))
}
async fn kr_positions_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!(s
        .kr
        .live_state_rx
        .borrow()
        .positions
        .clone()))
}
async fn kr_stats_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!(s
        .kr
        .summary
        .read()
        .unwrap()
        .stats
        .clone()))
}
async fn us_status_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    let summary = s.us.summary.read().unwrap().clone();
    let live = s.us.live_state_rx.borrow().clone();
    Json(serde_json::json!({ "summary": summary, "live": live }))
}
async fn us_positions_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!(s
        .us
        .live_state_rx
        .borrow()
        .positions
        .clone()))
}
async fn us_stats_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!(s
        .us
        .summary
        .read()
        .unwrap()
        .stats
        .clone()))
}

async fn kr_command_handler(
    AxumState(s): AxumState<AppState>,
    Json(body): Json<CommandBody>,
) -> StatusCode {
    dispatch_command(&s.kr_cmd_tx, &body.command).await
}
async fn us_command_handler(
    AxumState(s): AxumState<AppState>,
    Json(body): Json<CommandBody>,
) -> StatusCode {
    dispatch_command(&s.us_cmd_tx, &body.command).await
}

// ── Watchlist Handlers ────────────────────────────────────────────────────

fn watchlist_get(
    watchlist_rx: &watch::Receiver<WatchlistSet>,
    patch: &Arc<RwLock<WatchlistPatch>>,
) -> Json<serde_json::Value> {
    let current = watchlist_rx.borrow().clone();
    let p = patch.read().unwrap().clone();
    Json(serde_json::json!({
        "current": current.all_unique(),
        "patch": { "add": p.add, "remove": p.remove },
    }))
}

fn watchlist_post(patch: &Arc<RwLock<WatchlistPatch>>, body: WatchlistPatchBody) -> StatusCode {
    let mut p = patch.write().unwrap();
    for sym in &body.add {
        p.remove.retain(|s| s != sym);
        if !p.add.contains(sym) {
            p.add.push(sym.clone());
        }
    }
    for sym in &body.remove {
        p.add.retain(|s| s != sym);
        if !p.remove.contains(sym) {
            p.remove.push(sym.clone());
        }
    }
    StatusCode::OK
}

async fn kr_watchlist_get_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    watchlist_get(&s.kr_watchlist_rx, &s.kr.watchlist_patch)
}
async fn us_watchlist_get_handler(AxumState(s): AxumState<AppState>) -> Json<serde_json::Value> {
    watchlist_get(&s.us_watchlist_rx, &s.us.watchlist_patch)
}

async fn kr_watchlist_post_handler(
    AxumState(s): AxumState<AppState>,
    Json(body): Json<WatchlistPatchBody>,
) -> StatusCode {
    watchlist_post(&s.kr.watchlist_patch, body)
}
async fn us_watchlist_post_handler(
    AxumState(s): AxumState<AppState>,
    Json(body): Json<WatchlistPatchBody>,
) -> StatusCode {
    watchlist_post(&s.us.watchlist_patch, body)
}

async fn kr_watchlist_delete_handler(AxumState(s): AxumState<AppState>) -> StatusCode {
    *s.kr.watchlist_patch.write().unwrap() = WatchlistPatch::default();
    StatusCode::OK
}
async fn us_watchlist_delete_handler(AxumState(s): AxumState<AppState>) -> StatusCode {
    *s.us.watchlist_patch.write().unwrap() = WatchlistPatch::default();
    StatusCode::OK
}

async fn dispatch_command(tx: &mpsc::Sender<BotCommand>, cmd: &str) -> StatusCode {
    let command = match cmd {
        "start" => BotCommand::Start,
        "stop" => BotCommand::Stop,
        "pause" => BotCommand::Pause,
        "status" => BotCommand::QueryStatus,
        _ => return StatusCode::BAD_REQUEST,
    };
    tx.send(command)
        .await
        .map(|_| StatusCode::OK)
        .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR)
}

/// RestApiTask 진입점. 반드시 127.0.0.1(loopback)에 바인딩.
/// ⚠️ 0.0.0.0 금지 (배포 체크리스트)
#[allow(clippy::too_many_arguments)]
pub async fn run_rest_task(
    port: u16,
    kr: PipelineState,
    us: PipelineState,
    kr_cmd_tx: mpsc::Sender<BotCommand>,
    us_cmd_tx: mpsc::Sender<BotCommand>,
    kr_watchlist_rx: watch::Receiver<WatchlistSet>,
    us_watchlist_rx: watch::Receiver<WatchlistSet>,
    admin_token: String,
    token: CancellationToken,
) -> anyhow::Result<()> {
    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    let router = build_router(
        kr,
        us,
        kr_cmd_tx,
        us_cmd_tx,
        kr_watchlist_rx,
        us_watchlist_rx,
        admin_token,
    );
    let socket = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&addr.into())?;
    socket.listen(128)?;
    let listener = tokio::net::TcpListener::from_std(std::net::TcpListener::from(socket))?;
    tracing::info!("REST API listening on {}", addr);
    axum::serve(listener, router)
        .with_graceful_shutdown(async move { token.cancelled().await })
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::PipelineState;
    use axum::http::StatusCode;
    use axum_test::TestServer;
    use tokio::sync::mpsc;

    fn make_router(
        admin_token: &str,
    ) -> (
        axum::Router,
        mpsc::Receiver<BotCommand>,
        mpsc::Receiver<BotCommand>,
    ) {
        let kr = PipelineState::new_for_test();
        let us = PipelineState::new_for_test();
        let (kr_tx, kr_rx) = mpsc::channel(8);
        let (us_tx, us_rx) = mpsc::channel(8);
        let (kr_wl_tx, kr_wl_rx) = tokio::sync::watch::channel(WatchlistSet::default());
        let (us_wl_tx, us_wl_rx) = tokio::sync::watch::channel(WatchlistSet::default());
        drop((kr_wl_tx, us_wl_tx));
        (
            build_router(
                kr,
                us,
                kr_tx,
                us_tx,
                kr_wl_rx,
                us_wl_rx,
                admin_token.to_string(),
            ),
            kr_rx,
            us_rx,
        )
    }

    #[tokio::test]
    async fn health_endpoint_is_public() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server.get("/health").await;
        assert_eq!(resp.status_code(), StatusCode::OK);
        let body: serde_json::Value = resp.json();
        assert_eq!(body["status"], "ok");
        assert!(body["uptime_secs"].is_number());
    }

    #[tokio::test]
    async fn get_kr_status_without_auth_is_401() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server.get("/kr/status").await;
        assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn get_kr_status_with_auth_is_ok() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server
            .get("/kr/status")
            .add_header("X-Admin-Token", "secret")
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_kr_positions_without_auth_is_401() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server.get("/kr/positions").await;
        assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn post_kr_command_without_token_is_401() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server
            .post("/kr/command")
            .json(&serde_json::json!({"command": "start"}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn post_kr_command_with_wrong_token_is_403() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server
            .post("/kr/command")
            .add_header("X-Admin-Token", "bad")
            .json(&serde_json::json!({"command": "start"}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn post_kr_command_with_correct_token_is_ok() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server
            .post("/kr/command")
            .add_header("X-Admin-Token", "secret")
            .json(&serde_json::json!({"command": "start"}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);
    }

    #[tokio::test]
    async fn get_kr_watchlist_without_auth_is_401() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);
        let resp = server.get("/kr/watchlist").await;
        assert_eq!(resp.status_code(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn kr_watchlist_patch_add_remove_roundtrip() {
        let (router, _kr_rx, _us_rx) = make_router("secret");
        let server = TestServer::new(router);

        // 종목 추가
        let resp = server
            .post("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .json(&serde_json::json!({"add": ["AAAA", "BBBB"]}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);

        // 조회 — patch에 반영 확인
        let resp = server
            .get("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);
        let body: serde_json::Value = resp.json();
        assert!(body["patch"]["add"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("AAAA")));

        // AAAA 제거
        let resp = server
            .post("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .json(&serde_json::json!({"remove": ["AAAA"]}))
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);

        let resp = server
            .get("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .await;
        let body: serde_json::Value = resp.json();
        assert!(!body["patch"]["add"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("AAAA")));
        assert!(body["patch"]["remove"]
            .as_array()
            .unwrap()
            .contains(&serde_json::json!("AAAA")));

        // DELETE로 패치 초기화
        let resp = server
            .delete("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .await;
        assert_eq!(resp.status_code(), StatusCode::OK);

        let resp = server
            .get("/kr/watchlist")
            .add_header("X-Admin-Token", "secret")
            .await;
        let body: serde_json::Value = resp.json();
        assert!(body["patch"]["add"].as_array().unwrap().is_empty());
        assert!(body["patch"]["remove"].as_array().unwrap().is_empty());
    }
}
