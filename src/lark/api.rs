//! Tenant-token-aware `OpenAPI` client for messages, cards, media, bot info,
//! and chat info.
//!
//! Wire paths are extracted from the bundled `@larksuiteoapi/node-sdk`
//! codegen: sends use `POST /open-apis/im/v1/messages?receive_id_type=chat_id`,
//! replies use `POST /open-apis/im/v1/messages/{message_id}/reply`
//! (`reply_in_thread: true` for topic replies), card updates use
//! `PATCH /open-apis/im/v1/messages/{message_id}`, resources download through
//! `GET /open-apis/im/v1/messages/{message_id}/resources/{file_key}?type=…`,
//! and uploads use the multipart `POST /open-apis/im/v1/images` /
//! `POST /open-apis/im/v1/files` endpoints.
//!
//! Every call attaches `Authorization: Bearer <tenant token>`. A response
//! classified as token-invalid (HTTP 401 or the 99991663-class Lark codes)
//! triggers exactly one forced token refresh followed by exactly one retry;
//! a second failure propagates as-is. Message and card content is never
//! logged and never appears in `Debug` output or error messages.

use std::fmt;
use std::future::Future;

use bytes::Bytes;
use secrecy::SecretString;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::error::{LarkError, check_code};
use super::http::LarkHttp;
use super::token::TenantTokenProvider;
use crate::limits::{LARK_MAX_RESOURCE_BYTES, LARK_MAX_SEND_BODY_BYTES, LARK_MAX_UPLOAD_BYTES};

const MESSAGES_PATH: &str = "/open-apis/im/v1/messages";
const IMAGES_PATH: &str = "/open-apis/im/v1/images";
const FILES_PATH: &str = "/open-apis/im/v1/files";
const BOT_INFO_PATH: &str = "/open-apis/bot/v3/info";
const APPLICATION_PATH: &str = "/open-apis/application/v6/applications";

/// Lark `code` range covering invalid or expired tenant/app access tokens.
/// Unlike the wider permanent-auth range, these specifically mean the bearer
/// token itself is stale, so one forced refresh plus one retry can succeed.
const TOKEN_INVALID_CODES: std::ops::RangeInclusive<i64> = 99_991_663..=99_991_668;

/// Reference to a message accepted by Lark.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageRef {
    /// Server-assigned `message_id` (`om_…`).
    pub message_id: String,
}

/// Sanitized bot identity returned by `GET /open-apis/bot/v3/info`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BotInfo {
    /// Bot display name (`app_name` on the wire).
    pub app_name: Option<String>,
    /// Bot `open_id`.
    pub open_id: Option<String>,
}

/// Conversation mode of a chat, from `GET /open-apis/im/v1/chats/{chat_id}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChatMode {
    /// Direct one-to-one chat (`p2p`).
    P2p,
    /// Plain group chat (`group`).
    Group,
    /// Topic (thread) group (`topic`).
    Topic,
}

/// Kind of a message resource, selecting the `type` query parameter on
/// download.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResourceKind {
    /// An image resource (`type=image`).
    Image,
    /// A file resource (`type=file`).
    File,
}

impl ResourceKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Image => "image",
            Self::File => "file",
        }
    }
}

/// A downloaded message resource.
///
/// `Debug` prints only the byte length, never the content.
#[derive(Clone, PartialEq, Eq)]
pub struct ResourceData {
    /// Raw resource bytes, capped at [`LARK_MAX_RESOURCE_BYTES`].
    pub bytes: Bytes,
}

impl fmt::Debug for ResourceData {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResourceData")
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// Raw message fields returned by `GET /open-apis/im/v1/messages/{id}`.
///
/// The raw item keeps `thread_id` even when the receive event dropped it,
/// which normalization relies on for topic backfill. The optional body is
/// retained only for the authorized, one-hop quote resolver. Its custom
/// `Debug` implementation exposes lengths and flags, never message content.
#[derive(Clone, PartialEq, Eq)]
pub struct RawMessage {
    /// `message_id` (`om_…`).
    pub message_id: String,
    /// Owning `chat_id` (`oc_…`).
    pub chat_id: String,
    /// Wire `chat_type` (`p2p`/`group`), kept as an open string.
    pub chat_type: String,
    /// Sender identifier returned for the fetched item.
    pub sender_id: Option<String>,
    /// Open sender kind (`user`/`app`/…), used to fail closed on non-humans.
    pub sender_type: Option<String>,
    /// Wire `msg_type` (`text`/`image`/…), kept as an open string.
    pub message_type: String,
    /// Reply-chain root `message_id`, when the message is a reply.
    pub root_id: Option<String>,
    /// Immediate parent `message_id`, when the message is a reply.
    pub parent_id: Option<String>,
    /// Topic `thread_id` (`omt_…`) for messages inside a topic thread.
    pub thread_id: Option<String>,
    /// Whether Lark marks the message deleted.
    pub deleted: bool,
    /// Serialized message body content, when returned by Lark.
    pub content: Option<String>,
}

impl fmt::Debug for RawMessage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("RawMessage")
            .field("message_id_len", &self.message_id.len())
            .field("chat_id_len", &self.chat_id.len())
            .field("chat_type_len", &self.chat_type.len())
            .field(
                "sender_id_len",
                &self.sender_id.as_deref().map_or(0, str::len),
            )
            .field(
                "sender_type_len",
                &self.sender_type.as_deref().map_or(0, str::len),
            )
            .field("message_type_len", &self.message_type.len())
            .field("has_root", &self.root_id.is_some())
            .field("has_parent", &self.parent_id.is_some())
            .field("has_thread", &self.thread_id.is_some())
            .field("deleted", &self.deleted)
            .field(
                "content_bytes",
                &self.content.as_deref().map_or(0, str::len),
            )
            .finish_non_exhaustive()
    }
}

/// `OpenAPI` client bound to one tenant's endpoints and token cache.
///
/// Cloning shares the underlying HTTP client and token cache.
#[derive(Clone)]
pub struct LarkApi {
    http: LarkHttp,
    tokens: TenantTokenProvider,
}

impl LarkApi {
    /// Creates a client over the shared HTTP core and token provider.
    #[must_use]
    pub fn new(http: LarkHttp, tokens: TenantTokenProvider) -> Self {
        Self { http, tokens }
    }

    /// Sends a text message to a chat.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn send_text(&self, chat_id: &str, text: &str) -> Result<MessageRef, LarkError> {
        let body = SendBody {
            receive_id: chat_id,
            msg_type: "text",
            content: text_content(text)?,
        };
        self.send_message(&body).await
    }

    /// Sends an interactive card to a chat.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn send_card(&self, chat_id: &str, card: Value) -> Result<MessageRef, LarkError> {
        let body = SendBody {
            receive_id: chat_id,
            msg_type: "interactive",
            content: card_content(&card)?,
        };
        self.send_message(&body).await
    }

    /// Replies to a message with text.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn reply_text(&self, message_id: &str, text: &str) -> Result<MessageRef, LarkError> {
        self.reply(message_id, "text", text_content(text)?, false)
            .await
    }

    /// Replies to a message with text inside its topic thread.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn reply_text_in_thread(
        &self,
        message_id: &str,
        text: &str,
    ) -> Result<MessageRef, LarkError> {
        self.reply(message_id, "text", text_content(text)?, true)
            .await
    }

    /// Replies to a message with an interactive card.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn reply_card(&self, message_id: &str, card: Value) -> Result<MessageRef, LarkError> {
        self.reply(message_id, "interactive", card_content(&card)?, false)
            .await
    }

    /// Replies to a message with an interactive card inside its topic thread.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn reply_card_in_thread(
        &self,
        message_id: &str,
        card: Value,
    ) -> Result<MessageRef, LarkError> {
        self.reply(message_id, "interactive", card_content(&card)?, true)
            .await
    }

    /// Updates an interactive card in place (`PATCH`).
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the serialized body exceeds
    /// [`LARK_MAX_SEND_BODY_BYTES`].
    pub async fn update_card(&self, message_id: &str, card: Value) -> Result<(), LarkError> {
        #[derive(Serialize)]
        struct UpdateBody {
            content: String,
        }

        let body = UpdateBody {
            content: card_content(&card)?,
        };
        check_send_body(&body)?;
        check_path_segment(message_id)?;
        let path = format!("{MESSAGES_PATH}/{message_id}");
        self.with_auth_retry(|token| {
            let path = path.clone();
            let body = &body;
            async move {
                let envelope: Envelope<Value> =
                    self.http.patch_json_bearer(&path, body, &token).await?;
                check_code(envelope.code, "updating an interactive card")
            }
        })
        .await
    }

    /// Fetches a message by ID, keeping the raw `thread_id` even when the
    /// receive event omitted it (topic backfill).
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, and
    /// `ProtocolViolation` when the response carries no message item.
    pub async fn get_message(&self, message_id: &str) -> Result<RawMessage, LarkError> {
        #[derive(Deserialize)]
        struct MessageData {
            items: Option<Vec<MessageItem>>,
        }
        #[derive(Deserialize)]
        struct MessageItem {
            message_id: Option<String>,
            chat_id: Option<String>,
            chat_type: Option<String>,
            sender: Option<MessageSender>,
            msg_type: Option<String>,
            root_id: Option<String>,
            parent_id: Option<String>,
            thread_id: Option<String>,
            #[serde(default)]
            deleted: bool,
            body: Option<MessageBody>,
        }
        #[derive(Deserialize)]
        struct MessageBody {
            content: Option<String>,
        }
        #[derive(Deserialize)]
        struct MessageSender {
            id: Option<String>,
            sender_type: Option<String>,
        }

        check_path_segment(message_id)?;
        let path = format!("{MESSAGES_PATH}/{message_id}");
        let data: MessageData = self
            .with_auth_retry(|token| {
                let path = path.clone();
                async move { self.checked_get(&path, &token, "fetching a message").await }
            })
            .await?;
        let item = data
            .items
            .and_then(|mut items| {
                if items.is_empty() {
                    None
                } else {
                    Some(items.swap_remove(0))
                }
            })
            .ok_or_else(|| LarkError::protocol("message response missing the items array"))?;
        let (sender_id, sender_type) = item
            .sender
            .map_or((None, None), |sender| (sender.id, sender.sender_type));
        Ok(RawMessage {
            message_id: item
                .message_id
                .ok_or_else(|| LarkError::protocol("message item missing message_id"))?,
            chat_id: item
                .chat_id
                .ok_or_else(|| LarkError::protocol("message item missing chat_id"))?,
            chat_type: item.chat_type.unwrap_or_default(),
            sender_id,
            sender_type,
            message_type: item.msg_type.unwrap_or_default(),
            root_id: item.root_id,
            parent_id: item.parent_id,
            thread_id: item.thread_id,
            deleted: item.deleted,
            content: item.body.and_then(|body| body.content),
        })
    }

    /// Fetches the conversation mode of a chat.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, and
    /// `ProtocolViolation` for an unknown `chat_mode` value.
    pub async fn get_chat_mode(&self, chat_id: &str) -> Result<ChatMode, LarkError> {
        #[derive(Deserialize)]
        struct ChatData {
            chat_mode: Option<String>,
        }

        check_path_segment(chat_id)?;
        let path = format!("/open-apis/im/v1/chats/{chat_id}");
        let data: ChatData = self
            .with_auth_retry(|token| {
                let path = path.clone();
                async move { self.checked_get(&path, &token, "fetching chat info").await }
            })
            .await?;
        match data.chat_mode.as_deref() {
            Some("p2p") => Ok(ChatMode::P2p),
            Some("group") => Ok(ChatMode::Group),
            Some("topic") => Ok(ChatMode::Topic),
            _ => Err(LarkError::protocol(
                "chat response has an unknown chat_mode",
            )),
        }
    }

    /// Downloads a message resource (image or file), aborting the stream
    /// mid-body once [`LARK_MAX_RESOURCE_BYTES`] is exceeded.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token, transport, or server failure, or
    /// [`LarkError::Exhausted`] when the resource exceeds the byte cap.
    pub async fn download_message_resource(
        &self,
        message_id: &str,
        file_key: &str,
        kind: ResourceKind,
    ) -> Result<ResourceData, LarkError> {
        check_path_segment(message_id)?;
        check_path_segment(file_key)?;
        let path = format!(
            "{MESSAGES_PATH}/{message_id}/resources/{file_key}?type={}",
            kind.as_str()
        );
        self.with_auth_retry(|token| {
            let path = path.clone();
            async move {
                let bytes = self
                    .http
                    .get_bytes_bearer(&path, &token, LARK_MAX_RESOURCE_BYTES)
                    .await?;
                Ok(ResourceData { bytes })
            }
        })
        .await
    }

    /// Uploads image bytes, returning the `image_key`.
    ///
    /// # Errors
    ///
    /// Returns [`LarkError::Exhausted`] before any I/O when the input exceeds
    /// [`LARK_MAX_UPLOAD_BYTES`], or a classified error on token, transport,
    /// or server failure.
    pub async fn upload_image(&self, bytes: Bytes) -> Result<String, LarkError> {
        #[derive(Deserialize)]
        struct ImageData {
            image_key: Option<String>,
        }

        check_upload_size(bytes.len())?;
        let data: ImageData = self
            .with_auth_retry(|token| {
                let bytes = bytes.clone();
                async move {
                    let form = reqwest::multipart::Form::new()
                        .text("image_type", "message")
                        .part(
                            "image",
                            reqwest::multipart::Part::bytes(bytes.to_vec())
                                .file_name("image")
                                .mime_str("application/octet-stream")
                                .map_err(|_| LarkError::protocol("building the image upload"))?,
                        );
                    self.checked_multipart(IMAGES_PATH, form, &token, "uploading an image")
                        .await
                }
            })
            .await?;
        data.image_key
            .filter(|key| !key.is_empty())
            .ok_or_else(|| LarkError::protocol("image upload response missing image_key"))
    }

    /// Uploads file bytes under `name`, returning the `file_key`.
    ///
    /// # Errors
    ///
    /// Returns [`LarkError::Exhausted`] before any I/O when the input exceeds
    /// [`LARK_MAX_UPLOAD_BYTES`], or a classified error on token, transport,
    /// or server failure.
    pub async fn upload_file(&self, name: &str, bytes: Bytes) -> Result<String, LarkError> {
        #[derive(Deserialize)]
        struct FileData {
            file_key: Option<String>,
        }

        check_upload_size(bytes.len())?;
        let data: FileData = self
            .with_auth_retry(|token| {
                let bytes = bytes.clone();
                async move {
                    let form = reqwest::multipart::Form::new()
                        .text("file_type", "stream")
                        .text("file_name", name.to_owned())
                        .part(
                            "file",
                            reqwest::multipart::Part::bytes(bytes.to_vec())
                                .file_name(name.to_owned())
                                .mime_str("application/octet-stream")
                                .map_err(|_| LarkError::protocol("building the file upload"))?,
                        );
                    self.checked_multipart(FILES_PATH, form, &token, "uploading a file")
                        .await
                }
            })
            .await?;
        data.file_key
            .filter(|key| !key.is_empty())
            .ok_or_else(|| LarkError::protocol("file upload response missing file_key"))
    }

    /// Fetches the application creator (owner) `open_id` for the current app,
    /// returning `None` when the app reports no creator identifier.
    ///
    /// The creator is the user who owns the application, never the bot
    /// identity, and the `user_id_type=open_id` query scopes the returned
    /// identifier to this app.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token exchange, transport, or server
    /// failure.
    pub async fn app_creator_id(&self, app_id: &str) -> Result<Option<String>, LarkError> {
        #[derive(Deserialize)]
        struct AppInfoResponse {
            code: i64,
            data: Option<AppInfoData>,
        }
        #[derive(Deserialize)]
        struct AppInfoData {
            app: Option<AppInfoDto>,
        }
        #[derive(Deserialize)]
        struct AppInfoDto {
            creator_id: Option<String>,
        }

        check_path_segment(app_id)?;
        let path = format!("{APPLICATION_PATH}/{app_id}?lang=zh_cn&user_id_type=open_id");
        self.with_auth_retry(|token| {
            let path = path.clone();
            async move {
                let response: AppInfoResponse = self.http.get_json(&path, Some(&token)).await?;
                check_code(response.code, "fetching the application creator")?;
                Ok(response
                    .data
                    .and_then(|data| data.app)
                    .and_then(|app| app.creator_id)
                    .filter(|id| !id.is_empty()))
            }
        })
        .await
    }

    /// Fetches the sanitized bot identity.
    ///
    /// # Errors
    ///
    /// Returns a classified error on token exchange or bot-info failure.
    pub async fn bot_info(&self) -> Result<BotInfo, LarkError> {
        #[derive(Deserialize)]
        struct BotInfoResponse {
            code: i64,
            bot: Option<BotInfoDto>,
        }
        #[derive(Deserialize)]
        struct BotInfoDto {
            app_name: Option<String>,
            open_id: Option<String>,
        }

        self.with_auth_retry(|token| async move {
            let response: BotInfoResponse = self.http.get_json(BOT_INFO_PATH, Some(&token)).await?;
            check_code(response.code, "fetching bot info")?;
            let bot = response
                .bot
                .ok_or_else(|| LarkError::protocol("bot info response missing the bot object"))?;
            Ok(BotInfo {
                app_name: bot.app_name,
                open_id: bot.open_id,
            })
        })
        .await
    }

    async fn send_message(&self, body: &SendBody<'_>) -> Result<MessageRef, LarkError> {
        #[derive(Deserialize)]
        struct SendData {
            message_id: Option<String>,
        }

        check_send_body(body)?;
        let data: SendData = self
            .with_auth_retry(|token| async move {
                self.checked_post(
                    "/open-apis/im/v1/messages?receive_id_type=chat_id",
                    body,
                    &token,
                    "sending a message",
                )
                .await
            })
            .await?;
        let message_id = data
            .message_id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| LarkError::protocol("send response missing message_id"))?;
        Ok(MessageRef { message_id })
    }

    async fn reply(
        &self,
        message_id: &str,
        msg_type: &'static str,
        content: String,
        in_thread: bool,
    ) -> Result<MessageRef, LarkError> {
        #[derive(Serialize)]
        struct ReplyBody {
            msg_type: &'static str,
            content: String,
            #[serde(skip_serializing_if = "is_false")]
            reply_in_thread: bool,
        }
        #[derive(Deserialize)]
        struct ReplyData {
            message_id: Option<String>,
        }

        let body = ReplyBody {
            msg_type,
            content,
            reply_in_thread: in_thread,
        };
        check_send_body(&body)?;
        check_path_segment(message_id)?;
        let path = format!("{MESSAGES_PATH}/{message_id}/reply");
        let data: ReplyData = self
            .with_auth_retry(|token| {
                let path = path.clone();
                let body = &body;
                async move {
                    self.checked_post(&path, body, &token, "replying to a message")
                        .await
                }
            })
            .await?;
        let message_id = data
            .message_id
            .filter(|id| !id.is_empty())
            .ok_or_else(|| LarkError::protocol("reply response missing message_id"))?;
        Ok(MessageRef { message_id })
    }

    async fn checked_post<P, R>(
        &self,
        path: &str,
        body: &P,
        token: &SecretString,
        context: &'static str,
    ) -> Result<R, LarkError>
    where
        P: Serialize + Sync,
        R: DeserializeOwned,
    {
        let envelope: Envelope<R> = self.http.post_json_bearer(path, body, token).await?;
        check_code(envelope.code, context)?;
        envelope
            .data
            .ok_or_else(|| LarkError::protocol("OpenAPI response missing the data object"))
    }

    async fn checked_get<R>(
        &self,
        path: &str,
        token: &SecretString,
        context: &'static str,
    ) -> Result<R, LarkError>
    where
        R: DeserializeOwned,
    {
        let envelope: Envelope<R> = self.http.get_json(path, Some(token)).await?;
        check_code(envelope.code, context)?;
        envelope
            .data
            .ok_or_else(|| LarkError::protocol("OpenAPI response missing the data object"))
    }

    async fn checked_multipart<R>(
        &self,
        path: &str,
        form: reqwest::multipart::Form,
        token: &SecretString,
        context: &'static str,
    ) -> Result<R, LarkError>
    where
        R: DeserializeOwned,
    {
        let envelope: Envelope<R> = self.http.post_multipart_bearer(path, form, token).await?;
        check_code(envelope.code, context)?;
        envelope
            .data
            .ok_or_else(|| LarkError::protocol("OpenAPI response missing the data object"))
    }

    /// Runs `call` with a cached token; on a token-invalid failure, forces
    /// exactly one refresh and retries exactly once.
    async fn with_auth_retry<T, F, Fut>(&self, call: F) -> Result<T, LarkError>
    where
        F: Fn(SecretString) -> Fut,
        Fut: Future<Output = Result<T, LarkError>>,
    {
        let token = self.tokens.token().await?;
        match call(token).await {
            Err(error) if is_token_invalid(&error) => {
                let fresh = self.tokens.force_refresh().await?;
                call(fresh).await
            }
            result => result,
        }
    }
}

impl fmt::Debug for LarkApi {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LarkApi")
            .field("http", &self.http)
            .field("tokens", &self.tokens)
            .finish_non_exhaustive()
    }
}

/// Lark response envelope; server-provided `msg` is deliberately discarded.
#[derive(Deserialize)]
struct Envelope<T> {
    code: i64,
    data: Option<T>,
}

#[derive(Serialize)]
struct SendBody<'a> {
    receive_id: &'a str,
    msg_type: &'static str,
    content: String,
}

// serde's `skip_serializing_if` always passes the field by reference.
#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(value: &bool) -> bool {
    !*value
}

fn text_content(text: &str) -> Result<String, LarkError> {
    serde_json::to_string(&serde_json::json!({ "text": text }))
        .map_err(|_| LarkError::protocol("serializing a text message"))
}

fn card_content(card: &Value) -> Result<String, LarkError> {
    serde_json::to_string(card).map_err(|_| LarkError::protocol("serializing a card"))
}

fn check_send_body(body: &impl Serialize) -> Result<(), LarkError> {
    let len = serde_json::to_vec(body)
        .map_err(|_| LarkError::protocol("serializing an outbound body"))?
        .len();
    if len > LARK_MAX_SEND_BODY_BYTES {
        return Err(LarkError::exhausted(
            "outbound message body exceeds the byte cap",
            LARK_MAX_SEND_BODY_BYTES as u64,
        ));
    }
    Ok(())
}

fn check_upload_size(len: usize) -> Result<(), LarkError> {
    if len > LARK_MAX_UPLOAD_BYTES {
        return Err(LarkError::exhausted(
            "upload exceeds the byte cap",
            LARK_MAX_UPLOAD_BYTES as u64,
        ));
    }
    Ok(())
}

/// Server-issued IDs (`om_…`/`oc_…`/`ou_…`) use a URL-safe alphabet; reject
/// anything else before it is interpolated into a request path, so a hostile
/// or corrupted ID can never rewrite the request target.
fn check_path_segment(id: &str) -> Result<(), LarkError> {
    if !id.is_empty()
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        return Ok(());
    }
    Err(LarkError::protocol(
        "server-issued ID contains unsafe characters",
    ))
}

fn is_token_invalid(error: &LarkError) -> bool {
    matches!(
        error,
        LarkError::PermanentAuth {
            code: Some(code),
            ..
        } if *code == i64::from(401) || TOKEN_INVALID_CODES.contains(code)
    )
}
