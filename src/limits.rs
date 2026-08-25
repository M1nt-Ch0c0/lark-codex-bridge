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
/// Maximum UTF-8 bytes in one configured external Codex WebSocket endpoint.
pub const MAX_EXTERNAL_ENDPOINT_BYTES: usize = 2 * 1024;
/// Maximum encoded bytes in a configured external credential-source path.
pub const MAX_EXTERNAL_SECRET_PATH_BYTES: usize = 4 * 1024;
/// Maximum bytes read from an external bearer-token file.
pub const MAX_EXTERNAL_AUTH_TOKEN_BYTES: usize = 4 * 1024;
/// One-shot external gate frame/message bound; the gate never transfers thread history.
pub const EXTERNAL_GATE_MESSAGE_BYTES: usize = 256 * 1024;
/// Aggregate inbound bytes accepted while waiting for one external-gate response.
pub const EXTERNAL_GATE_TOTAL_BYTES: usize = 1024 * 1024;
/// Inbound frames accepted while waiting for one external-gate response.
pub const EXTERNAL_GATE_MAX_MESSAGES: usize = 64;
/// Maximum assembled text message and individual frame size on a long-running external Codex
/// WebSocket connection. This matches the existing JSONL protocol record limit.
pub const EXTERNAL_WS_MESSAGE_BYTES: usize = MAX_JSONL_LINE_BYTES;
/// Deadline for one WebSocket write or control-frame flush.
pub const EXTERNAL_WS_IO_TIMEOUT: Duration = Duration::from_secs(10);
/// Maximum time spent waiting for the peer half of a WebSocket close handshake.
pub const EXTERNAL_WS_CLOSE_TIMEOUT: Duration = Duration::from_secs(2);
/// Maximum number of persisted thread subscriptions managed by one external endpoint.
pub const EXTERNAL_MANAGED_THREAD_CAPACITY: usize = 64;
/// Maximum terminal/status events buffered for one thread while its authoritative snapshot is
/// being reconciled.
pub const EXTERNAL_RECONCILE_EVENT_CAPACITY: usize = THREAD_EVENT_CAPACITY;
/// Maximum retained bytes in one thread's reconciliation mailbox.
pub const EXTERNAL_RECONCILE_MAILBOX_BYTES: usize = THREAD_MAILBOX_BYTE_BUDGET;
/// Maximum turn pages and item pages read for one thread in a reconciliation pass.
pub const EXTERNAL_RECONCILE_PAGE_CAPACITY: usize = 32;
/// Page size requested from the exact promoted turn/item list APIs.
pub const EXTERNAL_RECONCILE_PAGE_SIZE: u32 = 100;
/// Maximum number of turns or items materialized for one thread in a reconciliation pass.
pub const EXTERNAL_RECONCILE_ENTRY_CAPACITY: usize = 3_200;
/// Maximum typed response bytes materialized while reconciling one thread.
pub const EXTERNAL_RECONCILE_THREAD_BYTES: usize = 32 * 1024 * 1024;
/// Maximum typed response bytes materialized across one endpoint reconciliation pass.
pub const EXTERNAL_RECONCILE_ENDPOINT_BYTES: usize = 64 * 1024 * 1024;
/// Initial and maximum reconnect delays for an unavailable external endpoint.
pub const EXTERNAL_RECONNECT_INITIAL_DELAY: Duration = Duration::from_millis(500);
pub const EXTERNAL_RECONNECT_MAX_DELAY: Duration = Duration::from_secs(30);
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
/// Exact serialized-byte cap for one Card 2.0 Markdown element. Lark's Card
/// 2.0 contract documents this as approximately 30 KiB; the bridge treats
/// 30*1024 bytes as a hard element-object wire bound after JSON escaping.
pub const LARK_CARD_MARKDOWN_ELEMENT_MAX_BYTES: usize = 30 * 1024;
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
/// Whole admission deadline for authentication, initialize/version validation, and list canary.
pub const EXTERNAL_GATE_TIMEOUT: Duration = Duration::from_secs(15);
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

/// Maximum bytes before the newline of one Rust↔Node sidecar frame. This is
/// deliberately no larger than one native reassembled event.
pub const CHANNEL_SIDECAR_FRAME_BYTES: usize = LARK_FRAGMENT_MESSAGE_BYTES;
/// Events admitted by the Rust wire reader but not yet decided by the durable
/// intake hook. Saturation returns an explicit negative ack.
pub const CHANNEL_SIDECAR_EVENT_CAPACITY: usize = 64;
/// Frames waiting for the child stdin writer.
pub const CHANNEL_SIDECAR_WRITE_CAPACITY: usize = 128;
/// Deadline for protocol/version/capability configuration.
pub const CHANNEL_SIDECAR_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
/// Deadline for the official SDK to report its first live connection after
/// protocol configuration succeeds.
pub const CHANNEL_SIDECAR_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
/// A process epoch must remain continuously connected for this long before a
/// later crash starts a fresh restart-backoff sequence.
pub const CHANNEL_SIDECAR_HEALTHY_UPTIME: Duration = Duration::from_secs(30);
/// Deadline shared with the Node handler before upstream must receive failure.
pub const CHANNEL_SIDECAR_HANDLER_TIMEOUT: Duration = LARK_HANDLER_TIMEOUT;
/// Extra time for a negative handler-timeout ack to reach Node before Node
/// independently rejects the pending SDK handler.
pub const CHANNEL_SIDECAR_ACK_GRACE: Duration = Duration::from_secs(5);
/// Grace for a correlated shutdown response and clean child exit.
pub const CHANNEL_SIDECAR_SHUTDOWN_GRACE: Duration = Duration::from_secs(5);

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
/// Hard cap on one downloaded attachment before it is written to disk.
/// Reuses the Lark resource download cap so an already-bounded download can
/// never exceed the cache's per-item budget.
pub const ATTACHMENT_MAX_BYTES: usize = LARK_MAX_RESOURCE_BYTES;
/// Maximum distinct resource descriptors accepted for one message/turn.
pub const ATTACHMENT_MAX_PER_MESSAGE: usize = 16;
/// Aggregate byte cap across all attachments leased by one turn.
pub const ATTACHMENT_TURN_TOTAL_BYTES: u64 = 64 * 1024 * 1024;
/// Maximum content files retained on disk in the attachment cache (mirrors
/// [`STORE_ATTACHMENT_MAX_ROWS`]).
pub const ATTACHMENT_CACHE_MAX_FILES: usize = 4096;
/// Maximum total content bytes retained on disk in the attachment cache
/// (mirrors [`STORE_ATTACHMENT_MAX_BYTES`]).
pub const ATTACHMENT_CACHE_MAX_BYTES: u64 = 256 * 1024 * 1024;
/// Maximum bytes of a display file name retained as metadata only (never a
/// disk-path component).
pub const ATTACHMENT_FILE_NAME_MAX_BYTES: usize = 255;
/// Maximum bytes of one resource MIME type string.
pub const ATTACHMENT_MIME_MAX_BYTES: usize = 128;
/// Maximum bytes of one resource key.
pub const ATTACHMENT_RESOURCE_KEY_MAX_BYTES: usize = 4 * 1024;
/// Age after which an unleased attachment becomes eligible for GC.
pub const ATTACHMENT_GC_AGE: Duration = Duration::from_secs(24 * 60 * 60);
/// Interval between periodic GC sweeps (the periodic runner is out of scope
/// for the attachment cache core).
pub const ATTACHMENT_GC_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Maximum victims examined and evicted by one GC pass.
pub const ATTACHMENT_GC_BATCH: usize = 256;
/// Maximum directory entries scanned by one reconciliation pass.
pub const ATTACHMENT_RECONCILE_BATCH: usize = 4096;
/// In-cache temp-file name prefix (never a valid SHA-256 name).
pub const ATTACHMENT_TEMP_PREFIX: &str = ".tmp-";
/// Cache-directory marker file name proving the directory is a dedicated
/// attachment cache. Never a valid SHA-256 name, and the reconciliation
/// scanner deliberately skips it.
pub const ATTACHMENT_CACHE_MARKER: &str = ".attachment-cache";
/// Cache-directory instance lock file name used for an OS-released exclusive
/// advisory lock. Never a valid SHA-256 name, and the reconciliation scanner
/// deliberately skips it.
pub const ATTACHMENT_INSTANCE_LOCK: &str = ".attachment-instance.lock";
/// Default maximum duration of one audio part that may be sent to the local
/// ASR sidecar (10 minutes). Longer clips fail closed as `too_long`.
pub const ASR_MAX_DURATION_MS: u64 = 10 * 60 * 1000;
/// Non-configurable upper bound for local ASR work. Operator configuration may
/// lower this limit but can never raise it.
pub const ASR_ABSOLUTE_MAX_DURATION_MS: u64 = ASR_MAX_DURATION_MS;
/// PCM byte rate produced by the fixed ffmpeg projection (16 kHz, mono, s16).
pub const ASR_DECODED_PCM_BYTES_PER_SECOND: u64 = 16_000 * 2;
/// Maximum decoded WAV bytes, including a conservative bounded header. This is
/// enforced while ffmpeg is running and verified again before the sidecar.
pub const ASR_DECODED_WAV_MAX_BYTES: u64 =
    ASR_DECODED_PCM_BYTES_PER_SECOND * (ASR_ABSOLUTE_MAX_DURATION_MS / 1_000) + 64 * 1024;
/// Maximum transcript bytes accepted from inbound recognition text or sidecar
/// stdout.
pub const ASR_TRANSCRIPT_MAX_BYTES: usize = 32 * 1024;
/// Maximum extra arguments forwarded to the ASR sidecar.
pub const ASR_MAX_ARGS: usize = 32;
/// Maximum bytes of one ASR sidecar argument.
pub const ASR_MAX_ARG_BYTES: usize = 4 * 1024;
/// Deadline for one ffmpeg decode of inbound audio.
pub const ASR_FFMPEG_TIMEOUT: Duration = Duration::from_secs(30);
/// Deadline for one local ASR sidecar invocation.
pub const ASR_SIDECAR_TIMEOUT: Duration = Duration::from_secs(60);
/// Maximum live attachment acquisitions. Every fetch owns an independent token
/// so overlapping reads cannot release one another's GC protection.
pub const STORE_ATTACHMENT_LEASE_MAX_ROWS: u64 = 65_536;
/// Maximum UTF-8 bytes in an internal attachment acquisition token.
pub const STORE_ATTACHMENT_LEASE_TOKEN_MAX_BYTES: usize = 64;
/// Maximum live (`starting`/`running`/`uncertain`) turns retained for crash
/// recovery. Terminal turns are historical rows and do not occupy this set.
pub const STORE_RECOVERY_TURN_MAX_ROWS: usize = 32;
/// Maximum identifier bytes materialized by one crash-recovery turn scan.
pub const STORE_RECOVERY_TURN_MAX_BYTES: usize = 1024 * 1024;
/// Send attempts after which an outbox row is marked terminally `failed`.
pub const STORE_OUTBOX_MAX_ATTEMPTS: u32 = 8;
/// Receipt-write attempts for one outbox row transition. A transient writer
/// queue overflow or `SQLite` failure must not strand a `sending` row
/// in-process: the pump retries this many times before warning and leaving the
/// row `sending` for startup `recover_sending_outbox` to reconcile.
pub const STORE_RECEIPT_WRITE_ATTEMPTS: u32 = 3;
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
/// Maximum distinct allowed sender open IDs retained in the runtime
/// configuration.
pub const MAX_CONFIG_ALLOWED_SENDERS: usize = 256;
/// Aggregate UTF-8 byte budget for configured allowed sender IDs.
pub const MAX_CONFIG_ALLOWED_SENDER_BYTES: usize = 32 * 1024;
/// Maximum distinct allowed group chat IDs retained in the runtime
/// configuration.
pub const MAX_CONFIG_ALLOWED_GROUPS: usize = 256;
/// Aggregate UTF-8 byte budget for configured allowed group chat IDs.
pub const MAX_CONFIG_ALLOWED_GROUP_BYTES: usize = 32 * 1024;
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
/// Maximum opaque pagination cursor accepted by the persisted-thread command surface.
pub const THREAD_DISCOVERY_CURSOR_MAX_BYTES: usize = 512;
/// Maximum stable thread selector accepted by an explicit adoption request.
pub const THREAD_ADOPTION_SELECTOR_MAX_BYTES: usize = 128;
/// Maximum candidate summaries a future enabled discovery page may expose.
pub const THREAD_DISCOVERY_MAX_RESULTS: usize = 20;
/// Maximum encoded bytes a future enabled discovery page may expose.
pub const THREAD_DISCOVERY_MAX_PAGE_BYTES: usize = 16 * 1024;

/// Lifetime of attachment descriptors staged by one direct-message scope.
/// Bytes are never downloaded while a descriptor is pending.
pub const PENDING_MEDIA_TTL: Duration = Duration::from_secs(10 * 60);
/// Maximum attachment messages staged by one direct-message scope.
pub const PENDING_MEDIA_MAX_COUNT: usize = 16;
/// Aggregate variable metadata bytes retained by one pending-media queue.
pub const PENDING_MEDIA_MAX_METADATA_BYTES: usize = 256 * 1024;
/// Maximum serialized content accepted from a directly quoted Lark message.
pub const QUOTE_CONTENT_MAX_BYTES: usize = 256 * 1024;
/// Maximum typed parts accepted from one directly quoted message.
pub const QUOTE_MAX_PARTS: usize = 16;

/// Maximum characters (Unicode scalar values) in one projected reply message
/// before deterministic splitting. A part never exceeds this bound.
pub const REPLY_MESSAGE_MAX_CHARS: usize = 4000;
/// Maximum split parts for one projected reply; any remainder is truncated
/// with an explicit marker instead of producing an unbounded part count.
pub const REPLY_MAX_SPLITS: usize = 8;
/// Deterministic truncation marker appended to the final split part.
pub const REPLY_TRUNCATION_MARKER: &str = "…[truncated]";
/// Minimum interval between two progress upserts of the same turn.
pub const REPLY_UPDATE_MIN_INTERVAL: Duration = Duration::from_millis(1500);
/// Minimum newly accumulated characters before the next progress upsert.
pub const REPLY_UPDATE_MIN_CHARS: usize = 200;

/// Base delay of the outbox pump's deterministic exponential backoff.
pub const OUTBOX_RETRY_BASE: Duration = Duration::from_millis(500);
/// Upper bound of one outbox pump retry delay.
pub const OUTBOX_RETRY_MAX: Duration = Duration::from_secs(30);
/// Poll cadence for discovering newly enqueued rows while the transport is
/// connected and no rows are due yet.
pub const OUTBOX_POLL_INTERVAL: Duration = Duration::from_millis(250);
/// Retention window for automatically sweepable terminal (`sent`/`failed`)
/// outbox rows, in milliseconds. `uncertain_delivery` evidence is retained
/// until explicit operator resolution; the all-state hard caps still bound
/// the durable table and fail producers closed.
pub const OUTBOX_TERMINAL_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1000;
/// Maximum terminal outbox rows deleted by one bounded sweep.
pub const OUTBOX_SWEEP_BATCH: u32 = 256;
/// Interval between bounded outbox terminal sweeps.
pub const OUTBOX_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Hard upper bound on the total outbox row count across **all** states
/// (`pending`, `sending`, `sent`, `failed`, `uncertain_delivery`). Counting
/// every state means no state transition can ever push the table past the
/// bound (a row only moves between states, never changing the total). The
/// periodic sweep only frees `sent`/`failed` rows past the retention window,
/// so a burst of rows can outrun it; enqueue fails closed (after one bounded
/// inline sweep) once this cap is reached instead of letting the table grow
/// without bound.
pub const OUTBOX_TERMINAL_MAX_ROWS: u64 = 65_536;
/// Hard upper bound on payload bytes retained by the outbox table across all
/// states (see [`OUTBOX_TERMINAL_MAX_ROWS`]).
pub const OUTBOX_TERMINAL_MAX_BYTES: u64 = 256 * 1024 * 1024;
