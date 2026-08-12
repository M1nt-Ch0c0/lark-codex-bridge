//! Native Feishu/Lark `OpenAPI` and transport client.
//!
//! Secret hygiene: App Secrets and tenant access tokens are held in
//! [`secrecy::SecretString`], redacted from every `Debug` implementation, and
//! never appear in tracing output or error messages. Errors are classified
//! via [`error::LarkError`] so callers can distinguish permanent
//! authentication failures from retryable ones.

pub mod config;
pub mod credentials;
pub mod error;
pub mod fragments;
pub mod frame;
pub mod http;
pub mod register;
pub mod token;
