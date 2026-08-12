use std::fmt;

use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use serde_json::{Map, Value};
use thiserror::Error;

use crate::limits::MAX_JSONL_LINE_BYTES;

/// An opaque request identifier accepted by Codex app-server.
#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
#[serde(untagged)]
pub enum RequestId {
    String(String),
    Integer(i64),
}

/// The structured error object returned for a failed RPC request.
#[derive(Clone, Deserialize, PartialEq, Serialize)]
pub struct RpcErrorObject {
    pub code: i64,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl fmt::Debug for RpcErrorObject {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RpcErrorObject")
            .field("code", &self.code)
            .field("message", &"[redacted]")
            .field("has_data", &self.data.is_some())
            .finish()
    }
}

/// A message received from Codex app-server.
#[derive(Clone, PartialEq)]
pub enum InboundMessage {
    Response {
        id: RequestId,
        result: Value,
    },
    ErrorResponse {
        id: RequestId,
        error: RpcErrorObject,
    },
    Request {
        id: RequestId,
        method: String,
        params: Option<Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
}

impl fmt::Debug for InboundMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Response { id, .. } => formatter
                .debug_struct("Response")
                .field("id", id)
                .field("result", &"[redacted]")
                .finish(),
            Self::ErrorResponse { id, error } => formatter
                .debug_struct("ErrorResponse")
                .field("id", id)
                .field("error", error)
                .finish(),
            Self::Request { id, method, params } => formatter
                .debug_struct("Request")
                .field("id", id)
                .field("method", method)
                .field("has_params", &params.is_some())
                .finish(),
            Self::Notification { method, params } => formatter
                .debug_struct("Notification")
                .field("method", method)
                .field("has_params", &params.is_some())
                .finish(),
        }
    }
}

/// A message sent to Codex app-server.
#[derive(Clone, PartialEq)]
pub enum OutboundMessage {
    Request {
        id: RequestId,
        method: String,
        params: Option<Value>,
    },
    Notification {
        method: String,
        params: Option<Value>,
    },
    Response {
        id: RequestId,
        result: Value,
    },
    ErrorResponse {
        id: RequestId,
        error: RpcErrorObject,
    },
}

impl fmt::Debug for OutboundMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Request { id, method, params } => formatter
                .debug_struct("Request")
                .field("id", id)
                .field("method", method)
                .field("has_params", &params.is_some())
                .finish(),
            Self::Notification { method, params } => formatter
                .debug_struct("Notification")
                .field("method", method)
                .field("has_params", &params.is_some())
                .finish(),
            Self::Response { id, .. } => formatter
                .debug_struct("Response")
                .field("id", id)
                .field("result", &"[redacted]")
                .finish(),
            Self::ErrorResponse { id, error } => formatter
                .debug_struct("ErrorResponse")
                .field("id", id)
                .field("error", error)
                .finish(),
        }
    }
}

/// A safe protocol parsing or encoding failure. Raw wire contents are never retained.
#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("JSONL message is {length} bytes; maximum is {maximum} bytes")]
    LineTooLong { length: usize, maximum: usize },
    #[error("protocol line is not valid JSON")]
    InvalidJson(#[source] serde_json::Error),
    #[error("protocol message must be a JSON object")]
    ExpectedObject,
    #[error("invalid protocol envelope: {0}")]
    InvalidEnvelope(&'static str),
    #[error("protocol message could not be encoded")]
    Encode(#[source] serde_json::Error),
}

/// Decodes and classifies one app-server JSONL record.
///
/// Missing params become `None`; explicit `params: null` remains `Some(Value::Null)`.
/// A successful response may contain `result: null`, which remains distinct from a
/// missing result.
///
/// # Errors
///
/// Returns [`ProtocolError`] for oversized, invalid, or ambiguous records.
pub fn decode_line(line: &[u8]) -> Result<InboundMessage, ProtocolError> {
    let line = line.strip_suffix(b"\n").unwrap_or(line);
    let line = line.strip_suffix(b"\r").unwrap_or(line);
    if line.len() > MAX_JSONL_LINE_BYTES {
        return Err(ProtocolError::LineTooLong {
            length: line.len(),
            maximum: MAX_JSONL_LINE_BYTES,
        });
    }

    if line
        .iter()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| *byte != b'{')
    {
        return Err(ProtocolError::ExpectedObject);
    }

    let envelope: RawEnvelope = serde_json::from_slice(line).map_err(ProtocolError::InvalidJson)?;
    classify(envelope)
}

fn classify(envelope: RawEnvelope) -> Result<InboundMessage, ProtocolError> {
    let has_id = envelope.id.is_present();
    let has_method = envelope.method.is_present();
    let has_params = envelope.params.is_present();
    let has_result = envelope.result.is_present();
    let has_error = envelope.error.is_present();

    if has_method {
        if has_result || has_error {
            return Err(ProtocolError::InvalidEnvelope(
                "method cannot be combined with result or error",
            ));
        }
        let method_value = envelope
            .method
            .into_value()
            .ok_or(ProtocolError::InvalidEnvelope("missing method"))?;
        let method = method_value
            .as_str()
            .filter(|method| !method.is_empty())
            .ok_or(ProtocolError::InvalidEnvelope(
                "method must be a non-empty string",
            ))?
            .to_owned();
        let params = envelope.params.into_option();

        return if has_id {
            Ok(InboundMessage::Request {
                id: decode_id(envelope.id.into_value())?,
                method,
                params,
            })
        } else {
            Ok(InboundMessage::Notification { method, params })
        };
    }

    if has_params {
        return Err(ProtocolError::InvalidEnvelope(
            "params requires a request or notification method",
        ));
    }
    if !has_id {
        return Err(ProtocolError::InvalidEnvelope("response is missing an id"));
    }
    if has_result == has_error {
        return Err(ProtocolError::InvalidEnvelope(
            "response must contain exactly one of result or error",
        ));
    }

    let id = decode_id(envelope.id.into_value())?;
    if has_result {
        return Ok(InboundMessage::Response {
            id,
            result: envelope
                .result
                .into_value()
                .ok_or(ProtocolError::InvalidEnvelope("missing response result"))?,
        });
    }

    let error_value = envelope
        .error
        .into_value()
        .ok_or(ProtocolError::InvalidEnvelope("missing response error"))?;
    if error_value.is_null() {
        return Err(ProtocolError::InvalidEnvelope(
            "error response contains null error",
        ));
    }
    let error = serde_json::from_value(error_value)
        .map_err(|_| ProtocolError::InvalidEnvelope("invalid error object"))?;
    Ok(InboundMessage::ErrorResponse { id, error })
}

fn decode_id(value: Option<Value>) -> Result<RequestId, ProtocolError> {
    serde_json::from_value(value.ok_or(ProtocolError::InvalidEnvelope("missing request id"))?)
        .map_err(|_| ProtocolError::InvalidEnvelope("id must be a string or signed integer"))
}

enum Presence<T> {
    Missing,
    Present(T),
}

impl<T> Presence<T> {
    fn is_present(&self) -> bool {
        matches!(self, Self::Present(_))
    }

    fn into_value(self) -> Option<T> {
        match self {
            Self::Missing => None,
            Self::Present(value) => Some(value),
        }
    }

    fn into_option(self) -> Option<T> {
        self.into_value()
    }
}

struct RawEnvelope {
    id: Presence<Value>,
    method: Presence<Value>,
    params: Presence<Value>,
    result: Presence<Value>,
    error: Presence<Value>,
}

impl<'de> Deserialize<'de> for RawEnvelope {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct EnvelopeVisitor;

        impl<'de> Visitor<'de> for EnvelopeVisitor {
            type Value = RawEnvelope;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an app-server protocol object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut envelope = RawEnvelope {
                    id: Presence::Missing,
                    method: Presence::Missing,
                    params: Presence::Missing,
                    result: Presence::Missing,
                    error: Presence::Missing,
                };

                while let Some(key) = map.next_key::<String>()? {
                    let slot = match key.as_str() {
                        "id" => Some(&mut envelope.id),
                        "method" => Some(&mut envelope.method),
                        "params" => Some(&mut envelope.params),
                        "result" => Some(&mut envelope.result),
                        "error" => Some(&mut envelope.error),
                        _ => None,
                    };
                    if let Some(slot) = slot {
                        if slot.is_present() {
                            return Err(serde::de::Error::custom(
                                "duplicate protocol envelope field",
                            ));
                        }
                        *slot = Presence::Present(map.next_value()?);
                    } else {
                        let _: Value = map.next_value()?;
                    }
                }
                Ok(envelope)
            }
        }

        deserializer.deserialize_map(EnvelopeVisitor)
    }
}

/// Encodes one app-server message and appends exactly one line feed.
///
/// # Errors
///
/// Returns [`ProtocolError`] when JSON serialization fails or the encoded record
/// exceeds the configured JSONL line limit.
pub fn encode_line(message: &OutboundMessage) -> Result<Vec<u8>, ProtocolError> {
    let value = outbound_value(message).map_err(ProtocolError::Encode)?;
    let mut bytes = serde_json::to_vec(&value).map_err(ProtocolError::Encode)?;
    if bytes.len() > MAX_JSONL_LINE_BYTES {
        return Err(ProtocolError::LineTooLong {
            length: bytes.len(),
            maximum: MAX_JSONL_LINE_BYTES,
        });
    }
    bytes.push(b'\n');
    Ok(bytes)
}

fn outbound_value(message: &OutboundMessage) -> Result<Value, serde_json::Error> {
    let mut object = Map::new();
    match message {
        OutboundMessage::Request { id, method, params } => {
            object.insert("id".to_owned(), id_value(id));
            object.insert("method".to_owned(), Value::String(method.clone()));
            if let Some(params) = params {
                object.insert("params".to_owned(), params.clone());
            }
        }
        OutboundMessage::Notification { method, params } => {
            object.insert("method".to_owned(), Value::String(method.clone()));
            if let Some(params) = params {
                object.insert("params".to_owned(), params.clone());
            }
        }
        OutboundMessage::Response { id, result } => {
            object.insert("id".to_owned(), id_value(id));
            object.insert("result".to_owned(), result.clone());
        }
        OutboundMessage::ErrorResponse { id, error } => {
            object.insert("id".to_owned(), id_value(id));
            object.insert("error".to_owned(), serde_json::to_value(error)?);
        }
    }
    Ok(Value::Object(object))
}

fn id_value(id: &RequestId) -> Value {
    match id {
        RequestId::String(id) => Value::String(id.clone()),
        RequestId::Integer(id) => Value::Number((*id).into()),
    }
}
