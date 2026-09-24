use std::sync::Arc;
use std::time::Instant;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use kohaku_pir_provider::PirRouter;
use serde_json::{Value, json};
use tracing::info;

pub type AppState = Arc<PirRouter>;

/// Handle a JSON-RPC POST body (single object or batch array).
pub async fn handle_rpc(
    State(router): State<AppState>,
    body: Result<Json<Value>, axum::extract::rejection::JsonRejection>,
) -> impl IntoResponse {
    let Json(body) = match body {
        Ok(j) => j,
        Err(e) => {
            return (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32700, "message": format!("parse error: {e}") }
                })),
            )
                .into_response();
        }
    };

    if let Some(arr) = body.as_array() {
        let mut out = Vec::with_capacity(arr.len());
        for item in arr {
            out.push(dispatch_one(&router, item).await);
        }
        return (StatusCode::OK, Json(Value::Array(out))).into_response();
    }

    (StatusCode::OK, Json(dispatch_one(&router, &body).await)).into_response()
}

async fn dispatch_one(router: &PirRouter, req: &Value) -> Value {
    let id = req.get("id").cloned().unwrap_or(Value::Null);
    let method = match req.get("method").and_then(|m| m.as_str()) {
        Some(m) => m,
        None => {
            return json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": -32600, "message": "invalid request: missing method" }
            });
        }
    };
    let params = req.get("params").cloned().unwrap_or(Value::Array(vec![]));

    let route = router.routes().classify(method, &params);
    let start = Instant::now();
    let result = router.request(method, params).await;
    let elapsed_ms = start.elapsed().as_millis();

    match result {
        Ok(value) => {
            info!(
                method,
                ?route,
                elapsed_ms,
                "ok"
            );
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "result": value
            })
        }
        Err(e) => {
            info!(
                method,
                ?route,
                elapsed_ms,
                error = %e,
                "err"
            );
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": {
                    "code": e.rpc_code(),
                    "message": e.to_string()
                }
            })
        }
    }
}
