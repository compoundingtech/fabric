//! Options and the latest session failure for one local generic dial listener.
use std::sync::RwLock;

use anyhow::Error;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{
    config::DialOptions,
    control::DialLastError,
    error::{ErrorCode, code_in_text},
};

#[derive(Debug)]
pub(crate) struct DialListenerState {
    options: RwLock<DialOptions>,
    last_error: RwLock<Option<DialLastError>>,
}

impl DialListenerState {
    pub(crate) fn new(options: DialOptions) -> Self {
        Self {
            options: RwLock::new(options),
            last_error: RwLock::new(None),
        }
    }

    pub(crate) fn options(&self) -> DialOptions {
        *self.options.read().unwrap()
    }
    pub(crate) fn update(&self, options: DialOptions) {
        *self.options.write().unwrap() = options;
    }
    pub(crate) fn last_error(&self) -> Option<DialLastError> {
        self.last_error.read().unwrap().clone()
    }

    pub(crate) fn record(&self, error: &Error) {
        self.record_text(&crate::dial_queue::diagnostic(error));
    }

    pub(crate) fn record_text(&self, error: &str) {
        let code = code_in_text(error).unwrap_or(ErrorCode::Internal);
        *self.last_error.write().unwrap() = Some(DialLastError {
            code: code.as_str().into(),
            class: code.class().as_str().into(),
            at: time::OffsetDateTime::now_utc()
                .format(&time::format_description::well_known::Rfc3339)
                .expect("current UTC time is representable as RFC3339"),
        });
    }

    /// Admission failed before a tunnel exists, so this socket has no service
    /// output. A small bounded write carries the optional coded reason.
    pub(crate) async fn refuse<W: AsyncWrite + Unpin>(
        &self,
        local: &mut W,
        options: DialOptions,
        error: &Error,
    ) {
        self.record(error);
        if options.error_preface {
            let line = error_preface(&crate::dial_queue::diagnostic(error));
            let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
                local.write_all(&line).await?;
                local.shutdown().await
            })
            .await;
        }
    }
}

pub(crate) fn error_preface(error: &str) -> Vec<u8> {
    let code = code_in_text(error).unwrap_or(ErrorCode::Internal);
    let text = crate::error::without_tokens(error);
    let text = text.split_whitespace().collect::<Vec<_>>().join(" ");
    format!("{}\n", code.reason(&text)).into_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn error_preface_refusal_is_coded_single_line_and_optional() {
        use tokio::io::AsyncReadExt;
        let state = DialListenerState::new(DialOptions::default());
        assert!(state.last_error().is_none());
        let error: Error = crate::error::FabricError::new(
            ErrorCode::SessionLimitReached,
            "full\n queue\r\nsecond line",
        )
        .into();
        for enabled in [false, true] {
            let (mut write, mut read) = tokio::io::duplex(1024);
            let options = DialOptions {
                error_preface: enabled,
                ..DialOptions::default()
            };
            state.refuse(&mut write, options, &error).await;
            drop(write);
            let mut bytes = Vec::new();
            read.read_to_end(&mut bytes).await.unwrap();
            assert_eq!(
                bytes,
                if enabled {
                    b"fabric:session_limit_reached full queue second line\n".to_vec()
                } else {
                    vec![]
                }
            );
            let latest = state.last_error().unwrap();
            assert_eq!(latest.code, "session_limit_reached");
            assert_eq!(latest.class, "fault");
            assert!(
                time::OffsetDateTime::parse(
                    &latest.at,
                    &time::format_description::well_known::Rfc3339
                )
                .is_ok()
            );
        }
    }
}
