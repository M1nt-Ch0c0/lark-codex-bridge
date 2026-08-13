use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use lark_codex_bridge::lark::frame::{Frame, FrameMethod, Header, header_key};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Message;

const TEST_TIMEOUT: Duration = Duration::from_secs(10);
const WS_INCOMING_CAPACITY: usize = 8;

/// Bounded in-process WebSocket fixture shared by bridge integration tests.
pub struct TestWsServer {
    pub addr: SocketAddr,
    pub incoming: mpsc::Receiver<TestWsConn>,
    task: JoinHandle<()>,
}

/// One accepted test WebSocket connection.
pub struct TestWsConn {
    pub ws: WebSocketStream<TcpStream>,
}

impl TestWsServer {
    pub async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("ws listener binds");
        let addr = listener.local_addr().expect("ws addr");
        let (tx, incoming) = mpsc::channel(WS_INCOMING_CAPACITY);
        // Handshakes are deliberately sequential. This bounds both accepted
        // connections parked for tests and concurrent handshake work without
        // spawning an unrestricted task per TCP connection.
        let task = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let Ok(ws) = tokio_tungstenite::accept_async(stream).await else {
                    continue;
                };
                if tx.send(TestWsConn { ws }).await.is_err() {
                    return;
                }
            }
        });
        Self {
            addr,
            incoming,
            task,
        }
    }

    pub async fn accept(&mut self) -> TestWsConn {
        timeout(TEST_TIMEOUT, self.incoming.recv())
            .await
            .expect("a connection arrives")
            .expect("connection channel stays open")
    }
}

impl Drop for TestWsServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl TestWsConn {
    pub async fn recv_frame(&mut self) -> Frame {
        let message = timeout(TEST_TIMEOUT, self.ws.next())
            .await
            .expect("a frame arrives")
            .expect("socket stays open")
            .expect("frame decodes at the ws layer");
        let Message::Binary(bytes) = message else {
            panic!("expected a binary frame, got {message:?}");
        };
        Frame::decode_bytes(&bytes).expect("pbbp2 frame decodes")
    }

    pub async fn send_frame(&mut self, frame: &Frame) {
        self.ws
            .send(Message::Binary(frame.encode_to_vec().into()))
            .await
            .expect("frame sends");
    }

    pub async fn send_data(&mut self, ty: &str, message_id: &str, payload: &[u8]) {
        let mut frame = Frame::ping(7);
        frame.method = FrameMethod::Data.as_wire();
        frame.headers = vec![
            Header {
                key: header_key::TYPE.to_owned(),
                value: ty.to_owned(),
            },
            Header {
                key: header_key::MESSAGE_ID.to_owned(),
                value: message_id.to_owned(),
            },
            Header {
                key: header_key::SUM.to_owned(),
                value: "1".to_owned(),
            },
            Header {
                key: header_key::SEQ.to_owned(),
                value: "0".to_owned(),
            },
            Header {
                key: header_key::TRACE_ID.to_owned(),
                value: format!("tr-{message_id}"),
            },
        ];
        frame.payload = Some(Bytes::from(payload.to_vec()));
        self.send_frame(&frame).await;
    }

    pub async fn send_pong(&mut self, config_json: &str) {
        let mut frame = Frame::ping(7);
        frame.headers = vec![Header {
            key: header_key::TYPE.to_owned(),
            value: "pong".to_owned(),
        }];
        frame.payload = Some(Bytes::from(config_json.as_bytes().to_vec()));
        self.send_frame(&frame).await;
    }

    pub async fn recv_receipt(&mut self) -> (String, Value) {
        loop {
            let frame = self.recv_frame().await;
            let headers = frame.frame_headers();
            if headers.biz_rt().is_some() {
                let body: Value =
                    serde_json::from_slice(frame.payload.as_ref().expect("receipt payload"))
                        .expect("receipt payload is json");
                return (
                    headers.message_id().expect("receipt message_id").to_owned(),
                    body,
                );
            }
        }
    }
}
