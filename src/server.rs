use std::borrow::Cow;
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::rpc::json_rpc::{
    ErrorPayload, Id, Request, RequestPacket, Response, ResponsePacket, ResponsePayload,
};
use alloy::transports::TransportError;
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::Json;
use kohaku_pir_rpc::{CallPath, CallTrace, PirTransport};
use kohaku_privacy_rpc::PrivacyTransport;
use serde_json::{json, Value};
use tracing::info;

/// JSON-RPC backend: Tor+PIR orchestrator or clearnet pir-rpc.
#[derive(Clone)]
pub enum Backend {
    /// All egress via Tor ([`PrivacyTransport`]).
    Tor(PrivacyTransport),
    /// Clearnet PIR + clearnet fallback RPC ([`PirTransport`]).
    Clearnet(PirTransport),
}

impl Backend {
    async fn call(
        &self,
        packet: RequestPacket,
    ) -> Result<(ResponsePacket, Vec<CallTrace>), TransportError> {
        match self {
            Self::Tor(t) => t.call_logged(packet).await,
            Self::Clearnet(t) => t.call_logged(packet).await,
        }
    }
}

pub type AppState = Arc<Backend>;

pub async fn handle_rpc(
    State(backend): State<AppState>,
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

    let packet = match json_to_packet(&body) {
        Ok(p) => p,
        Err(msg) => {
            return (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32600, "message": msg }
                })),
            )
                .into_response();
        }
    };

    let start = Instant::now();
    let (resp, traces) = match backend.call(packet).await {
        Ok(out) => out,
        Err(e) => {
            let elapsed = start.elapsed();
            info!("error  {}  {e}", format_elapsed(elapsed));
            return (
                StatusCode::OK,
                Json(json!({
                    "jsonrpc": "2.0",
                    "id": null,
                    "error": { "code": -32603, "message": e.to_string() }
                })),
            )
                .into_response();
        }
    };

    log_traces(&traces, start.elapsed());

    (StatusCode::OK, Json(packet_to_json(resp))).into_response()
}

fn log_traces(traces: &[CallTrace], elapsed: Duration) {
    if traces.len() <= 1 {
        for trace in traces {
            info!(
                "{}  {}  {}",
                trace.method,
                path_word(trace.path),
                format_elapsed(elapsed)
            );
        }
        return;
    }
    info!("batch {}  {}", traces.len(), format_elapsed(elapsed));
    for trace in traces {
        info!("  {}  {}", trace.method, path_word(trace.path));
    }
}

fn path_word(path: CallPath) -> &'static str {
    match path {
        CallPath::Pir => "pir",
        CallPath::Fallback => "fallback",
        CallPath::Local => "local",
    }
}

/// `312ms` under a second, otherwise one decimal second (`1.4s`).
fn format_elapsed(elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64();
    if secs < 1.0 {
        format!("{:.0}ms", secs * 1000.0)
    } else {
        format!("{secs:.1}s")
    }
}

fn json_to_packet(body: &Value) -> Result<RequestPacket, String> {
    if let Some(arr) = body.as_array() {
        let mut reqs = Vec::with_capacity(arr.len());
        for item in arr {
            reqs.push(json_to_serialized(item)?);
        }
        return Ok(RequestPacket::Batch(reqs));
    }
    Ok(RequestPacket::Single(json_to_serialized(body)?))
}

fn json_to_serialized(obj: &Value) -> Result<alloy::rpc::json_rpc::SerializedRequest, String> {
    let method = obj
        .get("method")
        .and_then(Value::as_str)
        .ok_or_else(|| "missing method".to_string())?;
    let id = parse_id(obj.get("id").unwrap_or(&Value::Null))?;
    let params = obj.get("params").cloned().unwrap_or(Value::Array(vec![]));
    let params_raw = serde_json::value::to_raw_value(&params).map_err(|e| e.to_string())?;
    Request::new(Cow::Owned(method.to_string()), id, params_raw)
        .serialize()
        .map_err(|e| e.to_string())
}

fn parse_id(v: &Value) -> Result<Id, String> {
    match v {
        Value::Null => Ok(Id::None),
        Value::Number(n) => n
            .as_u64()
            .map(Id::Number)
            .ok_or_else(|| "id number out of range".to_string()),
        Value::String(s) => Ok(Id::String(s.clone().into())),
        _ => Err("invalid json-rpc id".into()),
    }
}

fn packet_to_json(packet: ResponsePacket) -> Value {
    match packet {
        ResponsePacket::Single(r) => response_to_json(r),
        ResponsePacket::Batch(rs) => Value::Array(rs.into_iter().map(response_to_json).collect()),
    }
}

fn response_to_json(r: Response) -> Value {
    let id = id_to_json(&r.id);
    match r.payload {
        ResponsePayload::Success(raw) => {
            let result: Value = serde_json::from_str(raw.get()).unwrap_or(Value::Null);
            json!({ "jsonrpc": "2.0", "id": id, "result": result })
        }
        ResponsePayload::Failure(ErrorPayload { code, message, .. }) => {
            json!({
                "jsonrpc": "2.0",
                "id": id,
                "error": { "code": code, "message": message.as_ref() }
            })
        }
    }
}

fn id_to_json(id: &Id) -> Value {
    match id {
        Id::None => Value::Null,
        Id::Number(n) => json!(n),
        Id::String(s) => Value::String(s.to_string()),
    }
}
