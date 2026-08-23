//! pbbp2 protobuf wire messages with pure prost derives (no `build.rs`, no
//! `protoc`).
//!
//! Field numbers and wire types were extracted from the reference SDK's
//! generated pbbp2 encoder: `SeqID`=1/`LogID`=2 (`uint64`), `service`=3 and
//! `method`=4 (`int32`), `headers`=5 (repeated `Header`), `payloadEncoding`=6
//! and `payloadType`=7 (optional strings), `payload`=8 (optional bytes), and
//! `LogIDNew`=9 (optional string). The reference's proto2-style encoder writes
//! fields 1–4 unconditionally, so the wire structs below model them as
//! `optional` and the encoder always fills `Some(...)` (even when zero),
//! keeping our outbound bytes identical to the reference's; decoding defaults
//! a missing field to zero.
//!
//! Redaction: `Frame`'s `Debug` never prints payload bytes (length only) and
//! `FrameHeaders`'s `Debug` redacts `handshake-msg`, which is free-form server
//! text.

use std::fmt;

use bytes::Bytes;

use super::error::LarkError;

/// Private prost-derived wire types. Kept private because prost 0.14 derives
/// its own `Debug` (which would print payload bytes); the public types below
/// own the redacted `Debug` implementations. Fields 1–4 are `optional` so the
/// encoder always emits them exactly like the reference encoder (see the
/// module docs).
mod wire {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Header {
        #[prost(string, tag = "1")]
        pub key: String,
        #[prost(string, tag = "2")]
        pub value: String,
    }

    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Frame {
        #[prost(uint64, optional, tag = "1")]
        pub seq_id: Option<u64>,
        #[prost(uint64, optional, tag = "2")]
        pub log_id: Option<u64>,
        #[prost(int32, optional, tag = "3")]
        pub service: Option<i32>,
        #[prost(int32, optional, tag = "4")]
        pub method: Option<i32>,
        #[prost(message, repeated, tag = "5")]
        pub headers: Vec<Header>,
        #[prost(string, optional, tag = "6")]
        pub payload_encoding: Option<String>,
        #[prost(string, optional, tag = "7")]
        pub payload_type: Option<String>,
        #[prost(bytes, optional, tag = "8")]
        pub payload: Option<Vec<u8>>,
        #[prost(string, optional, tag = "9")]
        pub log_id_new: Option<String>,
    }
}

impl From<&Header> for wire::Header {
    fn from(header: &Header) -> Self {
        Self {
            key: header.key.clone(),
            value: header.value.clone(),
        }
    }
}

impl From<wire::Header> for Header {
    fn from(header: wire::Header) -> Self {
        Self {
            key: header.key,
            value: header.value,
        }
    }
}

impl From<&Frame> for wire::Frame {
    fn from(frame: &Frame) -> Self {
        Self {
            // Fields 1–4 are always written, even when zero, to match the
            // reference encoder byte for byte.
            seq_id: Some(frame.seq_id),
            log_id: Some(frame.log_id),
            service: Some(frame.service),
            method: Some(frame.method),
            headers: frame.headers.iter().map(wire::Header::from).collect(),
            payload_encoding: frame.payload_encoding.clone(),
            payload_type: frame.payload_type.clone(),
            payload: frame.payload.as_ref().map(|bytes| bytes.to_vec()),
            log_id_new: frame.log_id_new.clone(),
        }
    }
}

impl From<wire::Frame> for Frame {
    fn from(frame: wire::Frame) -> Self {
        Self {
            seq_id: frame.seq_id.unwrap_or_default(),
            log_id: frame.log_id.unwrap_or_default(),
            service: frame.service.unwrap_or_default(),
            method: frame.method.unwrap_or_default(),
            headers: frame.headers.into_iter().map(Header::from).collect(),
            payload_encoding: frame.payload_encoding,
            payload_type: frame.payload_type,
            payload: frame.payload.map(Bytes::from),
            log_id_new: frame.log_id_new,
        }
    }
}

/// Header keys used by the pbbp2 protocol.
pub mod header_key {
    /// Message discriminator (`event`/`card`/`ping`/`pong`).
    pub const TYPE: &str = "type";
    /// Fragmented message identifier.
    pub const MESSAGE_ID: &str = "message_id";
    /// Total fragment count of a message.
    pub const SUM: &str = "sum";
    /// Zero-based fragment index.
    pub const SEQ: &str = "seq";
    /// Server trace identifier.
    pub const TRACE_ID: &str = "trace_id";
    /// Business processing time appended to receipts (milliseconds).
    pub const BIZ_RT: &str = "biz_rt";
    /// Handshake outcome status, present on handshake failure frames.
    pub const HANDSHAKE_STATUS: &str = "handshake-status";
    /// Handshake outcome message (free-form server text; redacted in `Debug`).
    pub const HANDSHAKE_MSG: &str = "handshake-msg";
    /// Handshake authentication error code; presence is fatal to the session.
    pub const HANDSHAKE_AUTHERRCODE: &str = "handshake-autherrcode";
}

/// One pbbp2 header entry.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct Header {
    /// Header key (see [`header_key`]).
    pub key: String,
    /// Header value.
    pub value: String,
}

/// One pbbp2 frame as carried inside a WebSocket binary message.
///
/// `Debug` is manual and prints only the payload length, never its bytes.
#[derive(Clone, PartialEq, Default)]
pub struct Frame {
    /// Wire `SeqID` (0 for control frames).
    pub seq_id: u64,
    /// Wire `LogID` (0 for control frames).
    pub log_id: u64,
    /// Service identifier from the endpoint bootstrap query string.
    pub service: i32,
    /// Frame method: 0 = control, 1 = data (see [`FrameMethod`]).
    pub method: i32,
    /// Frame headers.
    pub headers: Vec<Header>,
    /// Optional payload encoding hint (wire `payloadEncoding`).
    pub payload_encoding: Option<String>,
    /// Optional payload type hint (wire `payloadType`).
    pub payload_type: Option<String>,
    /// Frame payload bytes; never printed by `Debug`.
    pub payload: Option<Bytes>,
    /// Wire `LogIDNew`.
    pub log_id_new: Option<String>,
}

impl Frame {
    /// Decodes one WebSocket binary message into a frame.
    ///
    /// # Errors
    ///
    /// Returns a protocol violation when the bytes are not a valid pbbp2 frame.
    pub fn decode_bytes(bytes: &[u8]) -> Result<Self, LarkError> {
        use prost::Message as _;
        wire::Frame::decode(bytes)
            .map(Self::from)
            .map_err(|_| LarkError::protocol("decoding a pbbp2 frame"))
    }

    /// Encodes the frame into the bytes of one WebSocket binary message.
    #[must_use]
    pub fn encode_to_vec(&self) -> Vec<u8> {
        prost::Message::encode_to_vec(&wire::Frame::from(self))
    }

    /// Returns an owned snapshot of this frame's headers.
    #[must_use]
    pub fn frame_headers(&self) -> FrameHeaders {
        FrameHeaders::new(self.headers.clone())
    }

    /// Builds a control ping frame for the given service id (`SeqID`/`LogID` 0).
    #[must_use]
    pub fn ping(service_id: i32) -> Self {
        Self {
            seq_id: 0,
            log_id: 0,
            service: service_id,
            method: FrameMethod::Control.as_wire(),
            headers: vec![Header {
                key: header_key::TYPE.to_owned(),
                value: MessageType::Ping.as_str().to_owned(),
            }],
            payload_encoding: None,
            payload_type: None,
            payload: None,
            log_id_new: None,
        }
    }
}

impl fmt::Debug for Frame {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Frame")
            .field("seq_id", &self.seq_id)
            .field("log_id", &self.log_id)
            .field("service", &self.service)
            .field("method", &self.method)
            .field("headers", &self.headers)
            .field("payload_encoding", &self.payload_encoding)
            .field("payload_type", &self.payload_type)
            .field("payload_len", &self.payload.as_ref().map(Bytes::len))
            .field("log_id_new", &self.log_id_new)
            .finish()
    }
}

/// The frame `method` field on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMethod {
    /// Control plane (ping/pong).
    Control,
    /// Data plane (event/card).
    Data,
}

impl FrameMethod {
    /// Wire value of the control method.
    pub const CONTROL_WIRE: i32 = 0;
    /// Wire value of the data method.
    pub const DATA_WIRE: i32 = 1;

    /// Parses the wire value; unknown values yield `None`.
    #[must_use]
    pub fn from_wire(value: i32) -> Option<Self> {
        match value {
            Self::CONTROL_WIRE => Some(Self::Control),
            Self::DATA_WIRE => Some(Self::Data),
            _ => None,
        }
    }

    /// Returns the wire value.
    #[must_use]
    pub fn as_wire(self) -> i32 {
        match self {
            Self::Control => Self::CONTROL_WIRE,
            Self::Data => Self::DATA_WIRE,
        }
    }
}

/// Values of the `type` header.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    /// Event callback payload.
    Event,
    /// Card action payload.
    Card,
    /// Liveness ping (outbound).
    Ping,
    /// Liveness pong carrying live `ClientConfig` (inbound).
    Pong,
}

impl MessageType {
    /// Returns the wire string.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Card => "card",
            Self::Ping => "ping",
            Self::Pong => "pong",
        }
    }

    /// Parses the wire string; unknown values yield `None`.
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "event" => Some(Self::Event),
            "card" => Some(Self::Card),
            "ping" => Some(Self::Ping),
            "pong" => Some(Self::Pong),
            _ => None,
        }
    }
}

/// An owned, cloneable snapshot of a frame's headers with typed accessors.
///
/// Owned (not borrowed) so it can cross the async handler boundary and be
/// reused for receipts. `Debug` redacts `handshake-msg`, which carries
/// free-form server text.
#[derive(Clone, PartialEq, Eq, Default)]
pub struct FrameHeaders {
    entries: Vec<Header>,
}

impl FrameHeaders {
    /// Wraps raw header entries.
    #[must_use]
    pub fn new(entries: Vec<Header>) -> Self {
        Self { entries }
    }

    /// Returns the raw entries in wire order.
    #[must_use]
    pub fn entries(&self) -> &[Header] {
        &self.entries
    }

    /// Consumes into the raw entries.
    #[must_use]
    pub fn into_entries(self) -> Vec<Header> {
        self.entries
    }

    /// Returns the first value for `key`.
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|header| header.key == key)
            .map(|header| header.value.as_str())
    }

    /// The parsed `type` header.
    #[must_use]
    pub fn ty(&self) -> Option<MessageType> {
        self.get(header_key::TYPE).and_then(MessageType::parse)
    }

    /// The `message_id` header.
    #[must_use]
    pub fn message_id(&self) -> Option<&str> {
        self.get(header_key::MESSAGE_ID)
    }

    /// The `trace_id` header.
    #[must_use]
    pub fn trace_id(&self) -> Option<&str> {
        self.get(header_key::TRACE_ID)
    }

    /// The `biz_rt` header (present on receipts).
    #[must_use]
    pub fn biz_rt(&self) -> Option<&str> {
        self.get(header_key::BIZ_RT)
    }

    /// The parsed `sum` header (total fragment count).
    #[must_use]
    pub fn sum(&self) -> Option<u32> {
        self.get(header_key::SUM).and_then(|raw| raw.parse().ok())
    }

    /// The parsed `seq` header (zero-based fragment index).
    #[must_use]
    pub fn seq(&self) -> Option<u32> {
        self.get(header_key::SEQ).and_then(|raw| raw.parse().ok())
    }

    /// The `handshake-status` header.
    #[must_use]
    pub fn handshake_status(&self) -> Option<&str> {
        self.get(header_key::HANDSHAKE_STATUS)
    }

    /// Whether a `handshake-msg` header is present; the value itself is
    /// free-form server text and deliberately not exposed for logging.
    #[must_use]
    pub fn has_handshake_msg(&self) -> bool {
        self.get(header_key::HANDSHAKE_MSG).is_some()
    }

    /// The `handshake-autherrcode` header; its presence marks the session as
    /// permanently unauthenticated.
    #[must_use]
    pub fn handshake_autherrcode(&self) -> Option<&str> {
        self.get(header_key::HANDSHAKE_AUTHERRCODE)
    }
}

impl fmt::Debug for FrameHeaders {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut list = formatter.debug_list();
        for header in &self.entries {
            if header.key == header_key::HANDSHAKE_MSG {
                list.entry(&format_args!("{}=<redacted>", header.key));
            } else {
                list.entry(&format_args!("{}={}", header.key, header.value));
            }
        }
        list.finish()
    }
}
