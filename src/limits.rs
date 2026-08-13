use std::time::Duration;

pub const MAX_JSONL_LINE_BYTES: usize = 32 * 1024 * 1024;
/// Conservative structural guard before allocating a `serde_json::Value`.
pub const MAX_JSON_STRUCTURAL_TOKENS: usize = 64 * 1024;
pub const MAX_JSON_NESTING: usize = 128;
/// Wire bytes accepted for a generic value before `serde_json::to_value`.
/// This intentionally leaves a wide margin for compact JSON expanding into a
/// tree of `Value` nodes; it is not allocator accounting.
pub const MAX_OUTBOUND_VALUE_WIRE_BYTES: usize = MAX_JSONL_LINE_BYTES / 32;
pub const RPC_HIGH_CAPACITY: usize = 64;
pub const RPC_NORMAL_CAPACITY: usize = 256;
pub const RPC_INFLIGHT_CAPACITY: usize = RPC_HIGH_CAPACITY + RPC_NORMAL_CAPACITY;
pub const RPC_TOTAL_PENDING_CAPACITY: usize = RPC_INFLIGHT_CAPACITY + RPC_HIGH_CAPACITY;
pub const RPC_SERVER_REQUEST_CAPACITY: usize = RPC_HIGH_CAPACITY;
pub const EVENT_CAPACITY: usize = 1024;
pub const RPC_RELIABLE_EVENT_CAPACITY: usize = RPC_SERVER_REQUEST_CAPACITY * 2;
pub const THREAD_EVENT_CAPACITY: usize = 256;
pub const THREAD_TERMINAL_CAPACITY: usize = 64;
pub const CLIENT_COMMAND_CAPACITY: usize = 256;
pub const CLIENT_CONTROL_CAPACITY: usize = 64;
pub const CLIENT_CONTROL_BYTE_BUDGET: usize = 8 * 1024 * 1024;
pub const CLIENT_CONTROL_EVENT_BYTE_LIMIT: usize = 4 * 1024 * 1024;
pub const CLIENT_PROJECTION_CAPACITY: usize = 64;
pub const CLIENT_SUBSCRIBER_CAPACITY: usize = 64;
pub const THREAD_SUBSCRIBER_CAPACITY: usize = 4;
pub const THREAD_MAILBOX_BYTE_BUDGET: usize = 4 * 1024 * 1024;
pub const THREAD_DELTA_BYTE_LIMIT: usize = 512 * 1024;
pub const THREAD_OUTCOME_CAPACITY: usize = 64;
pub const THREAD_PROJECTION_BYTE_BUDGET: usize = 4 * 1024 * 1024;
pub const ROUTING_ID_BYTE_LIMIT: usize = 1024;
pub const MAX_STDERR_LINE_BYTES: usize = 64 * 1024;
pub const MAX_VERSION_OUTPUT_BYTES: usize = 64 * 1024;
pub const HIGH_PRIORITY_BURST: usize = 8;
pub const TRANSPORT_BYTE_BUDGET: usize = 64 * 1024 * 1024;
pub const TRANSPORT_HIGH_BYTE_BUDGET: usize = TRANSPORT_BYTE_BUDGET;
pub const RPC_BYTE_BUDGET: usize = 64 * 1024 * 1024;
pub const RPC_HIGH_BYTE_BUDGET: usize = RPC_BYTE_BUDGET;
pub const RPC_RELIABLE_EVENT_BYTE_BUDGET: usize = RPC_BYTE_BUDGET;

/// Hard cap on a single Lark HTTP response body, enforced before JSON
/// parsing and mid-stream for close-delimited bodies.
pub const LARK_MAX_HTTP_BODY_BYTES: usize = 4 * 1024 * 1024;
/// Refresh the tenant access token this long before its server-declared
/// expiry (matches the reference SDK's early-expiry margin).
pub const TOKEN_REFRESH_SKEW: Duration = Duration::from_secs(3 * 60);

/// Hard cap on one outbound Lark message/card request body (serialized JSON,
/// envelope included). Oversize sends are refused before any request I/O.
pub const LARK_MAX_SEND_BODY_BYTES: usize = 256 * 1024;
/// Hard cap on one downloaded Lark message resource (image/file). The
/// download stream is aborted mid-body once the cap is exceeded instead of
/// buffering an unbounded response.
pub const LARK_MAX_RESOURCE_BYTES: usize = 20 * 1024 * 1024;
/// Hard cap on one uploaded Lark image/file. Oversize inputs are refused
/// before any upload I/O (the reference uploader relies on server-side
/// rejection only; the design deliberately tightens this).
pub const LARK_MAX_UPLOAD_BYTES: usize = 20 * 1024 * 1024;

pub const INITIALIZE_TIMEOUT: Duration = Duration::from_secs(10);
pub const SUPERVISOR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CONTROL_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const INTERRUPT_TIMEOUT: Duration = Duration::from_secs(10);
pub const VERSION_PROBE_TIMEOUT: Duration = Duration::from_secs(10);
/// Shared timeout for every Lark HTTP request (matches the reference
/// bootstrap request timeout).
pub const LARK_HTTP_TIMEOUT: Duration = Duration::from_secs(15);
/// Overall deadline for one QR registration session (matches the reference
/// QR session TTL).
pub const LARK_REGISTER_TIMEOUT: Duration = Duration::from_secs(20 * 60);

/// Total bytes buffered across all in-flight fragmented Lark messages.
pub const LARK_FRAGMENT_TOTAL_BYTES: usize = 8 * 1024 * 1024;
/// Maximum reassembled payload size of a single Lark message; also the WebSocket
/// binary message cap, since one fragment can never legally exceed it.
pub const LARK_FRAGMENT_MESSAGE_BYTES: usize = 1024 * 1024;
/// Maximum fragment count of one Lark message; `sum` headers above this are
/// rejected instead of allocating a huge slot vector.
pub const LARK_FRAGMENT_MESSAGE_MAX_FRAGMENTS: usize = 64;
/// Maximum distinct fragmented Lark messages buffered concurrently.
pub const LARK_FRAGMENT_MAX_IN_FLIGHT: usize = 64;
/// Time-to-live of a partially reassembled Lark message, swept on ingest
/// (deliberately tighter than the reference SDK's 10 s interval sweep).
pub const LARK_FRAGMENT_TTL: Duration = Duration::from_secs(5);

/// After the transport sends a ping, any inbound frame within this window
/// proves liveness; otherwise the socket is dropped to trigger a reconnect.
pub const LARK_PONG_TIMEOUT: Duration = Duration::from_secs(10);
/// Upper bound for one inbound frame handler invocation. A handler that
/// exceeds it is treated as failed (`{code: 500}` receipt) so a stuck handler
/// cannot stall the ping loop, the liveness watchdog, or shutdown. 60 s is
/// generous for in-memory normalization plus a bounded channel push, while
/// staying well below any plausible server-side receipt deadline.
pub const LARK_HANDLER_TIMEOUT: Duration = Duration::from_secs(60);
/// Fallback ping interval when the server bootstrap does not provide a
/// positive `PingInterval`.
pub const LARK_DEFAULT_PING_INTERVAL: Duration = Duration::from_secs(30);
/// Timeout for one WebSocket connect/handshake attempt.
pub const LARK_WS_CONNECT_TIMEOUT: Duration = Duration::from_secs(15);
/// Bounded grace for the Lark WebSocket actor to close the socket on shutdown.
pub const LARK_TRANSPORT_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);
/// Count bound of the normalizer's `chat_id → ChatMode` cache. Overflow
/// evicts the oldest entry.
pub const LARK_CHAT_MODE_CACHE_CAPACITY: usize = 256;
/// TTL of one cached chat-mode entry. Admins can convert a plain group into a
/// topic group, so entries must expire even when no message-level `thread_id`
/// ever contradicts them.
pub const LARK_CHAT_MODE_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
/// Byte cap on a `chat_id` eligible for caching; longer IDs are looked up on
/// every message instead of growing the cache key space.
pub const LARK_CHAT_MODE_CACHE_KEY_BYTES: usize = 128;
/// Upper bound on one raw inbound event payload handed to the normalizer.
/// The fragment reassembler already enforces the same cap; the normalizer
/// re-checks at its own boundary so direct callers are bounded too.
pub const LARK_MAX_EVENT_PAYLOAD_BYTES: usize = LARK_FRAGMENT_MESSAGE_BYTES;

/// Count bound of the transport observation channel (state/messages/anomalies).
pub const LARK_TRANSPORT_EVENT_CAPACITY: usize = 64;
/// Byte budget for message payloads parked in the transport observation
/// channel; permits are held until the receiver dequeues.
pub const LARK_TRANSPORT_EVENT_BYTE_BUDGET: usize = 8 * 1024 * 1024;

/// Count bound of the normalized inbound event channel handed from the Lark
/// bridge wiring to the scope runtime. A full channel fails the inbound
/// handler so the frame receipt honestly reports `{code: 500}` instead of
/// silently dropping the event.
pub const LARK_INBOUND_EVENT_CAPACITY: usize = 256;
/// Byte budget for events parked in the inbound event channel: legacy bridge
/// startup accounts raw wire payloads, while durable-runtime startup accounts
/// exact persisted normalized payload bytes. Permits are held until drop.
pub const LARK_INBOUND_EVENT_BYTE_BUDGET: usize = 8 * 1024 * 1024;

/// Count bound of the single-writer store command channel. Every store
/// request (reads included) travels this channel to the one blocking writer
/// task; a full channel fails the caller fast instead of growing an
/// unbounded backlog in front of the database.
pub const STORE_WRITER_CAPACITY: usize = 256;
/// Total bytes retained by requests waiting for the single store writer.
pub const STORE_WRITER_BYTE_BUDGET: usize = 8 * 1024 * 1024;
/// Maximum bytes of identifiers and metadata captured by one store request.
pub const STORE_REQUEST_MAX_BYTES: usize = 3 * 1024 * 1024;
/// `PRAGMA busy_timeout` for the store connection.
pub const STORE_BUSY_TIMEOUT: Duration = Duration::from_secs(5);
/// Hard cap on one serialized outbox payload, enforced before the row is
/// enqueued. Matches the Lark send body cap so anything persisted can also
/// be sent.
pub const STORE_OUTBOX_PAYLOAD_MAX_BYTES: usize = LARK_MAX_SEND_BODY_BYTES;
/// Upper clamp for one atomic outbox claim batch.
pub const STORE_OUTBOX_CLAIM_MAX_BATCH: u32 = 64;
/// Total payload bytes materialized by one claimed outbox batch.
pub const STORE_OUTBOX_CLAIM_MAX_BYTES: usize = 1024 * 1024;
/// Maximum durable pending or sending outbox records.
pub const STORE_OUTBOX_MAX_ROWS: u64 = 1024;
/// Maximum durable payload bytes in pending and sending outbox records.
pub const STORE_OUTBOX_MAX_QUEUED_BYTES: u64 = 8 * 1024 * 1024;
/// Maximum durable inbound dedup records before producers must be swept.
pub const STORE_INBOUND_MAX_ROWS: u64 = 65_536;
/// Maximum variable bytes retained by all inbound marker rows.
pub const STORE_INBOUND_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum rows that may retain replayable received payloads.
pub const STORE_INBOUND_RECEIVED_MAX_ROWS: u64 = 256;
/// Maximum serialized payload bytes retained by received rows.
pub const STORE_INBOUND_RECEIVED_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Maximum bytes in one inbound identifier.
pub const STORE_INBOUND_ID_MAX_BYTES: usize = 4 * 1024;
/// Maximum bytes in a serialized inbound scope.
pub const STORE_INBOUND_SCOPE_MAX_BYTES: usize = 12 * 1024;
/// Maximum bytes in an open message-type string.
pub const STORE_INBOUND_MESSAGE_TYPE_MAX_BYTES: usize = 256;
/// Maximum normalized message text bytes.
pub const STORE_INBOUND_TEXT_MAX_BYTES: usize = 1024 * 1024;
/// Maximum number of resource descriptors on one event.
pub const STORE_INBOUND_RESOURCE_MAX_COUNT: usize = 64;
/// Maximum bytes in one resource key.
pub const STORE_INBOUND_RESOURCE_KEY_MAX_BYTES: usize = 4 * 1024;
/// Maximum aggregate resource-key bytes on one event.
pub const STORE_INBOUND_RESOURCE_KEY_MAX_TOTAL_BYTES: usize = 256 * 1024;
/// Maximum bytes in one strict serialized inbound payload.
pub const STORE_INBOUND_PAYLOAD_MAX_BYTES: usize = 2 * 1024 * 1024;
/// Maximum input rows for one atomic begin-and-claim transaction.
pub const STORE_INBOUND_BEGIN_MAX_KEYS: usize = 64;
/// Maximum tenant/event key bytes for one begin-and-claim transaction.
pub const STORE_INBOUND_BEGIN_MAX_KEY_BYTES: usize = 256 * 1024;
/// Maximum terminal rows deleted by one deterministic sweep.
pub const DEDUP_SWEEP_BATCH: u32 = 256;
/// Dedup terminal-marker retention window.
pub const DEDUP_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);
/// Interval between bounded terminal-marker sweeps.
pub const DEDUP_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Interval for cancellation-aware received-row rescans.
pub const INBOUND_REPLAY_INTERVAL: Duration = Duration::from_secs(30);
/// Maximum durable attachment cache entries tracked by the store.
pub const STORE_ATTACHMENT_MAX_ROWS: u64 = 4096;
/// Maximum durable attachment bytes tracked by the store.
pub const STORE_ATTACHMENT_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum live (`starting`/`running`/`uncertain`) turns retained for crash
/// recovery. Terminal turns are historical rows and do not occupy this set.
pub const STORE_RECOVERY_TURN_MAX_ROWS: usize = 32;
/// Maximum identifier bytes materialized by one crash-recovery turn scan.
pub const STORE_RECOVERY_TURN_MAX_BYTES: usize = 1024 * 1024;
/// Send attempts after which an outbox row is marked terminally `failed`.
pub const STORE_OUTBOX_MAX_ATTEMPTS: u32 = 8;
/// Maximum length of a stored inbound-event rejection reason; reasons are
/// operator-facing classifications, never message content.
pub const STORE_REJECTION_REASON_MAX_BYTES: usize = 128;

/// Maximum distinct owner open IDs retained in the runtime configuration.
pub const MAX_CONFIG_OWNERS: usize = 256;
/// Aggregate UTF-8 byte budget for configured owner IDs.
pub const MAX_CONFIG_OWNER_BYTES: usize = 32 * 1024;
/// Maximum canonical workspace roots retained in the runtime configuration.
pub const MAX_CONFIG_ALLOW_ROOTS: usize = 64;
/// Aggregate encoded-path byte budget for configured workspace roots.
pub const MAX_CONFIG_ALLOW_ROOT_BYTES: usize = 16 * 1024;
/// Maximum distinct canonical protected roots retained by the platform policy.
pub const MAX_PLATFORM_PROTECTED_ROOTS: usize = 64;
/// Aggregate encoded-path byte budget for canonical protected roots.
pub const MAX_PLATFORM_PROTECTED_ROOT_BYTES: usize = 16 * 1024;
/// Maximum bytes parsed from the freedesktop user-directory configuration.
pub const MAX_XDG_USER_DIRS_BYTES: usize = 16 * 1024;
/// Default concurrent active Codex turns.
pub const DEFAULT_ACTIVE_TURN_PERMITS: usize = 4;
/// Default number of independently serialized scope actors.
pub const DEFAULT_MAX_SCOPE_ACTORS: usize = 256;

/// Count bound of commands waiting for the scope router task.
pub const ROUTER_COMMAND_CAPACITY: usize = 256;
/// Aggregate exact persisted inbound bytes waiting in the router command queue.
pub const ROUTER_COMMAND_BYTE_BUDGET: usize = 8 * 1024 * 1024;
/// Count bound of transiently failed route decisions waiting for retry.
pub const ROUTER_RETRY_CAPACITY: usize = 256;
/// Aggregate exact persisted inbound bytes retained by the router retry lane.
pub const ROUTER_RETRY_BYTE_BUDGET: usize = 8 * 1024 * 1024;
/// Count bound of high-priority runtime controls such as turn interruption.
pub const ROUTER_CONTROL_CAPACITY: usize = 64;
/// Aggregate serialized scope-key bytes retained by high-priority controls.
pub const ROUTER_CONTROL_BYTE_BUDGET: usize = 768 * 1024;
/// Hard upper bound accepted for configured concurrent active turns.
pub const ROUTER_ACTIVE_TURN_HARD_LIMIT: usize = 64;
/// Hard upper bound accepted for configured resident scope actors.
pub const ROUTER_SCOPE_ACTOR_HARD_LIMIT: usize = DEFAULT_MAX_SCOPE_ACTORS;
/// Count bound of one scope actor's totally ordered mailbox.
pub const SCOPE_MAILBOX_CAPACITY: usize = 64;
/// Exact persisted inbound bytes parked in one scope actor mailbox.
pub const SCOPE_MAILBOX_BYTE_BUDGET: usize = 8 * 1024 * 1024;
/// Maximum durable inbound rows claimed into one Codex turn.
pub const TURN_BATCH_MAX_MESSAGES: usize = 64;
/// Maximum normalized text bytes assembled into one Codex turn request.
pub const TURN_BATCH_TEXT_BYTE_BUDGET: usize = 768 * 1024;
/// Maximum bytes parsed as one recognized first-stage bridge command.
pub const BRIDGE_COMMAND_MAX_BYTES: usize = 16 * 1024;
