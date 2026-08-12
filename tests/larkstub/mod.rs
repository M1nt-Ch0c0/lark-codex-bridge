//! Minimal hand-rolled HTTP/1.1 stub server shared by the Lark client tests.
//!
//! No external dev-dependencies: a `tokio::net::TcpListener` accepts
//! connections, parses one request at a time (keep-alive aware), records it,
//! and answers through the test-supplied handler.

#![allow(dead_code)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

const MAX_STUB_REQUEST_BYTES: usize = 1024 * 1024;
const READ_CHUNK_BYTES: usize = 8192;

/// One HTTP request captured by the stub.
#[derive(Debug, Clone)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl RecordedRequest {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    #[must_use]
    pub fn body_text(&self) -> String {
        String::from_utf8_lossy(&self.body).into_owned()
    }
}

/// One canned HTTP response.
#[derive(Debug, Clone)]
pub struct StubResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    pub delay: Duration,
    pub close_delimited: bool,
}

impl StubResponse {
    #[must_use]
    pub fn json(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "application/json".to_owned())],
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
            close_delimited: false,
        }
    }

    #[must_use]
    pub fn text(status: u16, body: &str) -> Self {
        Self {
            status,
            headers: vec![("content-type".to_owned(), "text/plain".to_owned())],
            body: body.as_bytes().to_vec(),
            delay: Duration::ZERO,
            close_delimited: false,
        }
    }

    /// Delays the response, e.g. to prove single-flight behavior.
    #[must_use]
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }

    /// Omits `content-length` and closes the connection to delimit the body,
    /// exercising the client's streaming byte cap.
    #[must_use]
    pub fn close_delimited(mut self) -> Self {
        self.close_delimited = true;
        self
    }
}

pub type Handler = Arc<dyn Fn(&RecordedRequest) -> StubResponse + Send + Sync>;

/// Runs the stub until dropped.
pub struct StubServer {
    addr: SocketAddr,
    requests: Arc<Mutex<Vec<RecordedRequest>>>,
    task: JoinHandle<()>,
}

impl StubServer {
    pub async fn start(handler: Handler) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("stub listener should bind");
        let addr = listener
            .local_addr()
            .expect("stub listener should have an address");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let task = tokio::spawn({
            let requests = Arc::clone(&requests);
            async move {
                loop {
                    let Ok((stream, _)) = listener.accept().await else {
                        break;
                    };
                    let handler = Arc::clone(&handler);
                    let requests = Arc::clone(&requests);
                    tokio::spawn(async move {
                        serve_connection(stream, &handler, &requests).await;
                    });
                }
            }
        });
        Self {
            addr,
            requests,
            task,
        }
    }

    #[must_use]
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    #[must_use]
    pub fn request_count(&self) -> usize {
        self.requests.lock().expect("requests lock").len()
    }

    #[must_use]
    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.requests.lock().expect("requests lock").clone()
    }
}

impl Drop for StubServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_connection(
    stream: TcpStream,
    handler: &Handler,
    requests: &Arc<Mutex<Vec<RecordedRequest>>>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let mut buffer: Vec<u8> = Vec::new();
    let mut chunk = [0_u8; READ_CHUNK_BYTES];
    loop {
        let (request, consumed) = loop {
            if let Some(parsed) = try_parse_request(&buffer) {
                break parsed;
            }
            let read = match reader.read(&mut chunk).await {
                Ok(0) | Err(_) => return,
                Ok(read) => read,
            };
            buffer.extend_from_slice(&chunk[..read]);
            if buffer.len() > MAX_STUB_REQUEST_BYTES {
                return;
            }
        };
        buffer.drain(..consumed);
        requests
            .lock()
            .expect("requests lock")
            .push(request.clone());
        let response = handler(&request);
        if !response.delay.is_zero() {
            tokio::time::sleep(response.delay).await;
        }
        if write_response(&mut writer, &response).await.is_err() || response.close_delimited {
            return;
        }
    }
}

fn try_parse_request(buffer: &[u8]) -> Option<(RecordedRequest, usize)> {
    let header_end = find_subslice(buffer, b"\r\n\r\n")? + 4;
    let head = std::str::from_utf8(&buffer[..header_end]).ok()?;
    let mut lines = head.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_owned();
    let path = parts.next()?.to_owned();
    let mut content_length = 0_usize;
    let mut headers = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':')?;
        let value = value.trim().to_owned();
        if name.eq_ignore_ascii_case("content-length") {
            content_length = value.parse().ok()?;
        }
        headers.push((name.to_owned(), value));
    }
    let total = header_end + content_length;
    if buffer.len() < total {
        return None;
    }
    let body = buffer[header_end..total].to_vec();
    Some((
        RecordedRequest {
            method,
            path,
            headers,
            body,
        },
        total,
    ))
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

async fn write_response(
    writer: &mut tokio::net::tcp::OwnedWriteHalf,
    response: &StubResponse,
) -> std::io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        502 => "Bad Gateway",
        503 => "Service Unavailable",
        _ => "Status",
    };
    let status = response.status;
    let mut head = format!("HTTP/1.1 {status} {reason}\r\n");
    for (name, value) in &response.headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    if response.close_delimited {
        head.push_str("connection: close\r\n\r\n");
    } else if !response
        .headers
        .iter()
        .any(|(name, _)| name.eq_ignore_ascii_case("content-length"))
    {
        let length = response.body.len();
        head.push_str("content-length: ");
        head.push_str(&length.to_string());
        head.push_str("\r\n\r\n");
    } else {
        head.push_str("\r\n");
    }
    writer.write_all(head.as_bytes()).await?;
    writer.write_all(&response.body).await?;
    writer.flush().await
}
