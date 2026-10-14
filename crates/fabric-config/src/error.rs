//! Stable error codes: the contract a program reads instead of fabric's text.
//!
//! docs/errors.md is the specification and a test keeps this file and that
//! table identical. A failure carries a [`FabricError`] inside its
//! `anyhow::Error`; [`report`] finds it anywhere in the chain and prints the
//! human line, the `--json` object and the exit status the code promises.
//! Text that arrived from another machine (a close reason, a denial, an Error
//! frame) carries the code as a `fabric:<code> ` token, which [`code_in_text`]
//! reads back, together with the texts peers older than the codes still send.

use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::json;

/// The token that starts a coded reason: `fabric:<code> <text>`.
pub const TOKEN_PREFIX: &str = "fabric:";

/// Exit status of a failure that has no specific code yet; fabric's status
/// for every failure before codes existed.
pub const EXIT_INTERNAL: i32 = 1;
/// Exit status of arguments fabric rejected, the same status clap uses.
pub const EXIT_USAGE: i32 = 2;
/// Exit status of a fault (sysexits `EX_UNAVAILABLE`).
pub const EXIT_FAULT: i32 = 69;
/// Exit status of a refusal (sysexits `EX_NOPERM`).
pub const EXIT_REFUSAL: i32 = 77;

/// What a caller should do about a failure. Authoritative for codes added later.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorClass {
    /// Trust or a grant said no. Retrying or another route will not change
    /// that; only a change to trust will.
    Refusal,
    /// It did not work this time. Another attempt, later or by another route,
    /// may.
    Fault,
    /// Fabric rejected the arguments. Fix the call.
    Usage,
}

impl ErrorClass {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Refusal => "refusal",
            Self::Fault => "fault",
            Self::Usage => "usage",
        }
    }
}

impl fmt::Display for ErrorClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

macro_rules! error_codes {
    ($($variant:ident => $name:literal, $class:ident;)+) => {
        /// A stable error code. Codes are added in minor releases and never
        /// removed, renamed or moved to another class within a major version.
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
        pub enum ErrorCode {
            $($variant,)+
        }

        impl ErrorCode {
            /// Every code, in the order docs/errors.md lists them.
            pub const ALL: &'static [ErrorCode] = &[$(ErrorCode::$variant,)+];

            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }

            pub const fn class(self) -> ErrorClass {
                match self {
                    $(Self::$variant => ErrorClass::$class,)+
                }
            }
        }

        impl FromStr for ErrorCode {
            type Err = UnknownErrorCode;

            fn from_str(name: &str) -> Result<Self, Self::Err> {
                match name {
                    $($name => Ok(Self::$variant),)+
                    _ => Err(UnknownErrorCode(name.to_string())),
                }
            }
        }
    };
}

error_codes! {
    PeerNotInAllowList => "peer_not_in_allow_list", Refusal;
    PeerNotPermittedForService => "peer_not_permitted_for_service", Refusal;
    UnknownPeer => "unknown_peer", Fault;
    PeerOffline => "peer_offline", Fault;
    Unreachable => "unreachable", Fault;
    Timeout => "timeout", Fault;
    RelayRateLimited => "relay_rate_limited", Fault;
    DaemonNotRunning => "daemon_not_running", Fault;
    DaemonStopTimeout => "daemon_stop_timeout", Fault;
    ServiceNotExposed => "service_not_exposed", Fault;
    ServiceUnavailable => "service_unavailable", Fault;
    SessionLimitReached => "session_limit_reached", Fault;
    SessionExpired => "session_expired", Fault;
    SpawnFailed => "spawn_failed", Fault;
    Internal => "internal", Fault;
    InvalidArgument => "invalid_argument", Usage;
}

impl ErrorCode {
    /// The process exit status of a command that fails with this code.
    pub const fn exit_status(self) -> i32 {
        match (self, self.class()) {
            (Self::Internal, _) => EXIT_INTERNAL,
            (_, ErrorClass::Refusal) => EXIT_REFUSAL,
            (_, ErrorClass::Fault) => EXIT_FAULT,
            (_, ErrorClass::Usage) => EXIT_USAGE,
        }
    }

    /// `fabric:<code> <text>`: the form a reason takes on the wire and on
    /// stderr, so a reader that matches the old text still finds it.
    pub fn reason(self, text: &str) -> String {
        format!("{TOKEN_PREFIX}{} {text}", self.as_str())
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnknownErrorCode(pub String);

impl fmt::Display for UnknownErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unknown fabric error code {:?}", self.0)
    }
}

impl std::error::Error for UnknownErrorCode {}

/// A failure with a stable code, carried inside an `anyhow::Error`.
///
/// Its `Display` is the text for people alone; [`report`] adds the token.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FabricError {
    code: ErrorCode,
    message: String,
    peer: Option<String>,
    service: Option<String>,
}

impl FabricError {
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            peer: None,
            service: None,
        }
    }

    /// The peer the failure concerns, as the caller named it.
    pub fn peer(mut self, peer: impl Into<String>) -> Self {
        self.peer = Some(peer.into());
        self
    }

    /// The service (protocol) the failure concerns.
    pub fn service(mut self, service: impl Into<String>) -> Self {
        self.service = Some(service.into());
        self
    }

    pub fn code(&self) -> ErrorCode {
        self.code
    }

    pub fn class(&self) -> ErrorClass {
        self.code.class()
    }

    pub fn message(&self) -> &str {
        &self.message
    }

    pub fn peer_name(&self) -> Option<&str> {
        self.peer.as_deref()
    }

    pub fn service_name(&self) -> Option<&str> {
        self.service.as_deref()
    }

    /// `fabric:<code> <message>`, for a close reason, a denial or an Error frame.
    pub fn reason(&self) -> String {
        self.code.reason(&self.message)
    }
}

impl fmt::Display for FabricError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for FabricError {}

/// The first [`FabricError`] in the chain, outermost first.
pub fn find(error: &anyhow::Error) -> Option<&FabricError> {
    error
        .downcast_ref()
        .or_else(|| error.chain().find_map(|cause| cause.downcast_ref()))
}

/// Each `fabric:<code> ` token in a text, as (start, end, code name), with
/// `end` just past the space. A `fabric://` URL is not a token.
fn tokens(text: &str) -> Vec<(usize, usize, &str)> {
    let mut found = Vec::new();
    let mut from = 0;
    while let Some(at) = text[from..].find(TOKEN_PREFIX) {
        let start = from + at;
        let name_start = start + TOKEN_PREFIX.len();
        let after = &text[name_start..];
        let name_len = after
            .bytes()
            .take_while(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || *b == b'_')
            .count();
        let starts_with_letter = after.bytes().next().is_some_and(|b| b.is_ascii_lowercase());
        if starts_with_letter && after[name_len..].starts_with(' ') {
            found.push((start, name_start + name_len + 1, &after[..name_len]));
            from = name_start + name_len + 1;
        } else {
            from = name_start;
        }
    }
    found
}

/// The code a text carries: the first `fabric:<code> ` token in it that this
/// build knows, else one of the refusal texts peers sent before codes existed.
/// `None` for text that carries neither.
pub fn code_in_text(text: &str) -> Option<ErrorCode> {
    tokens(text)
        .into_iter()
        .find_map(|(_, _, name)| name.parse().ok())
        .or_else(|| {
            if text.contains("not permitted for service") {
                Some(ErrorCode::PeerNotPermittedForService)
            } else if text.contains("node is not in fabric allow-list") {
                Some(ErrorCode::PeerNotInAllowList)
            } else {
                None
            }
        })
}

/// The text for people: `text` without the tokens that came with it from
/// another machine, so a report carries one token, at its start.
pub fn without_tokens(text: &str) -> String {
    let mut kept = String::with_capacity(text.len());
    let mut from = 0;
    for (start, end, _) in tokens(text) {
        kept.push_str(&text[from..start]);
        from = end;
    }
    kept.push_str(&text[from..]);
    kept
}

/// The code of a failure: its [`FabricError`], else a code its text carries
/// from another machine, else [`ErrorCode::Internal`].
pub fn code_of(error: &anyhow::Error) -> ErrorCode {
    find(error)
        .map(FabricError::code)
        .or_else(|| code_in_text(&format!("{error:#}")))
        .unwrap_or(ErrorCode::Internal)
}

/// What the daemon's control socket carries beside a failure's message, so
/// a command reports the code the daemon chose. Every field is absent in a
/// reply from a daemon older than codes.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorFields {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub peer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

impl ErrorFields {
    /// The fields of a failure about to be sent as a reply.
    pub fn of(error: &anyhow::Error) -> Self {
        let found = find(error);
        Self {
            code: Some(code_of(error).as_str().to_string()),
            peer: found.and_then(FabricError::peer_name).map(str::to_string),
            service: found
                .and_then(FabricError::service_name)
                .map(str::to_string),
        }
    }

    /// The failure a reply reported: its code when this build knows it, else
    /// one its message carries, else a plain error that reports as `internal`.
    pub fn into_error(self, message: String) -> anyhow::Error {
        let code = self
            .code
            .as_deref()
            .and_then(|name| name.parse().ok())
            .or_else(|| code_in_text(&message));
        let Some(code) = code else {
            return anyhow::Error::msg(message);
        };
        let mut error = FabricError::new(code, without_tokens(&message));
        error.peer = self.peer;
        error.service = self.service;
        error.into()
    }
}

/// What a failed command writes and the status it exits with.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    /// The human line, without a trailing newline.
    pub stderr: String,
    /// The `--json` object on one line, when JSON was asked for.
    pub stdout: Option<String>,
    pub status: i32,
}

/// Render a failure as docs/errors.md specifies.
pub fn render(error: &anyhow::Error, json: bool) -> Report {
    let code = code_of(error);
    let found = find(error);
    let message = without_tokens(&format!("{error:#}"));
    let stdout = json.then(|| {
        json!({
            "error": {
                "code": code.as_str(),
                "class": code.class().as_str(),
                "message": message,
                "peer": found.and_then(FabricError::peer_name),
                "service": found.and_then(FabricError::service_name),
            }
        })
        .to_string()
    });
    Report {
        stderr: format!("Error: {}", code.reason(&message)),
        stdout,
        status: code.exit_status(),
    }
}

/// Print a failure (the human line on stderr; with `json`, the error object on
/// stdout) and return the status the process should exit with.
pub fn report(error: &anyhow::Error, json: bool) -> i32 {
    let report = render(error, json);
    if let Some(stdout) = &report.stdout {
        println!("{stdout}");
    }
    eprintln!("{}", report.stderr);
    report.status
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use anyhow::Context;

    use super::*;

    /// The rows of the code table in docs/errors.md: code to (class, exit).
    fn documented_codes() -> BTreeMap<String, (String, i32)> {
        let doc = include_str!("../../../docs/errors.md");
        let table = doc
            .split("<!-- codes -->")
            .nth(1)
            .expect("docs/errors.md has no <!-- codes --> table");
        table
            .lines()
            .filter(|line| line.starts_with("| `"))
            .map(|line| {
                let cells: Vec<&str> = line.split('|').map(str::trim).collect();
                (
                    cells[1].trim_matches('`').to_string(),
                    (
                        cells[2].to_string(),
                        cells[3].parse().expect("exit status is a number"),
                    ),
                )
            })
            .collect()
    }

    #[test]
    fn the_documented_table_is_the_code_list() {
        let documented = documented_codes();
        let built: BTreeMap<String, (String, i32)> = ErrorCode::ALL
            .iter()
            .map(|code| {
                (
                    code.as_str().to_string(),
                    (code.class().as_str().to_string(), code.exit_status()),
                )
            })
            .collect();
        assert_eq!(documented, built);
    }

    #[test]
    fn every_code_is_snake_case_and_parses_back() {
        for code in ErrorCode::ALL {
            let name = code.as_str();
            assert!(name.bytes().next().unwrap().is_ascii_lowercase(), "{name}");
            assert!(
                name.bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{name}"
            );
            assert_eq!(name.parse::<ErrorCode>(), Ok(*code));
            assert_eq!(code_in_text(&code.reason("anything")), Some(*code));
        }
        assert!("peer_gone".parse::<ErrorCode>().is_err());
    }

    #[test]
    fn a_reason_keeps_the_text_older_readers_match() {
        let reason =
            ErrorCode::PeerNotPermittedForService.reason("peer not permitted for service \"web\"");
        assert_eq!(
            reason,
            "fabric:peer_not_permitted_for_service peer not permitted for service \"web\""
        );
        assert!(reason.contains("not permitted for service"));
        let reason = ErrorCode::PeerNotInAllowList.reason("node is not in fabric allow-list");
        assert!(reason.contains("node is not in fabric allow-list"));
    }

    #[test]
    fn a_token_is_found_inside_a_longer_text() {
        let text = "connection lost: closed by peer: fabric:peer_not_in_allow_list node is not in fabric allow-list (access revoked) (code 403)";
        assert_eq!(code_in_text(text), Some(ErrorCode::PeerNotInAllowList));
        // A Git URL is not a token, and an unknown code is not guessed.
        assert_eq!(code_in_text("cloning fabric://box/notes failed"), None);
        assert_eq!(code_in_text("fabric:peer_gone it left"), None);
        assert_eq!(code_in_text("fabric:Timeout no"), None);
        assert_eq!(
            code_in_text("fabric://box/notes: fabric:timeout no answer in 3 s"),
            Some(ErrorCode::Timeout)
        );
    }

    #[test]
    fn an_unknown_token_does_not_hide_a_known_one_or_the_old_text() {
        assert_eq!(
            code_in_text("fabric:peer_gone it left: fabric:timeout late"),
            Some(ErrorCode::Timeout)
        );
        assert_eq!(
            code_in_text("fabric:grant_expired peer not permitted for service \"web\""),
            Some(ErrorCode::PeerNotPermittedForService)
        );
    }

    #[test]
    fn a_report_carries_one_token_at_its_start() {
        assert_eq!(
            without_tokens("dial web: fabric:peer_offline peer is offline"),
            "dial web: peer is offline"
        );
        assert_eq!(
            without_tokens("clone fabric://box/notes: fabric:x_y z"),
            "clone fabric://box/notes: z"
        );
        let error = anyhow::anyhow!(
            "fabric:peer_not_permitted_for_service peer not permitted for service \"web\""
        )
        .context("dial web on box");
        assert_eq!(
            render(&error, false).stderr,
            "Error: fabric:peer_not_permitted_for_service dial web on box: peer not permitted for service \"web\""
        );
    }

    #[test]
    fn a_daemon_reply_keeps_the_code_and_names() {
        let sent = anyhow::Error::new(
            FabricError::new(ErrorCode::UnknownPeer, "unknown peer \"box\"").peer("box"),
        )
        .context("dial web");
        let fields = ErrorFields::of(&sent);
        let wire = serde_json::to_value(&fields).unwrap();
        assert_eq!(wire, json!({"code": "unknown_peer", "peer": "box"}));
        let fields: ErrorFields = serde_json::from_value(wire).unwrap();
        let received = fields.into_error(format!("{sent:#}"));
        let report = render(&received, true);
        assert_eq!(report.status, EXIT_FAULT);
        let object: serde_json::Value = serde_json::from_str(&report.stdout.unwrap()).unwrap();
        assert_eq!(
            object,
            json!({"error": {
                "code": "unknown_peer",
                "class": "fault",
                "message": "dial web: unknown peer \"box\"",
                "peer": "box",
                "service": null,
            }})
        );
    }

    #[test]
    fn a_reply_from_a_daemon_before_codes_still_classifies_refusals() {
        let old: ErrorFields = serde_json::from_value(json!({})).unwrap();
        let refused = old.clone().into_error(
            "connection lost: closed by peer: node is not in fabric allow-list (code 403)".into(),
        );
        assert_eq!(code_of(&refused), ErrorCode::PeerNotInAllowList);
        let other = old.into_error("disk full".into());
        assert_eq!(code_of(&other), ErrorCode::Internal);
        // A code a newer daemon knows and this build does not.
        let newer = ErrorFields {
            code: Some("grant_expired".into()),
            ..ErrorFields::default()
        }
        .into_error("expired".into());
        assert_eq!(code_of(&newer), ErrorCode::Internal);
    }

    #[test]
    fn a_code_given_as_context_is_found_under_more_context() {
        let error = anyhow::anyhow!("connection refused")
            .context(FabricError::new(ErrorCode::Unreachable, "peer is offline"))
            .context("dial web on box");
        assert_eq!(code_of(&error), ErrorCode::Unreachable);
        assert_eq!(
            render(&error, false).stderr,
            "Error: fabric:unreachable dial web on box: peer is offline: connection refused"
        );
    }

    #[test]
    fn the_texts_older_peers_send_still_classify() {
        assert_eq!(
            code_in_text("closed by peer: peer not permitted for service \"web\" (code 403)"),
            Some(ErrorCode::PeerNotPermittedForService)
        );
        assert_eq!(
            code_in_text("closed by peer: node is not in fabric allow-list (code 403)"),
            Some(ErrorCode::PeerNotInAllowList)
        );
        assert_eq!(code_in_text("connection timed out"), None);
    }

    #[test]
    fn a_coded_failure_reports_its_code_class_and_status() {
        let error = anyhow::Error::new(
            FabricError::new(ErrorCode::PeerOffline, "peer is offline")
                .peer("box")
                .service("web"),
        )
        .context("dial web on box");
        let report = render(&error, true);
        assert_eq!(report.status, EXIT_FAULT);
        assert_eq!(
            report.stderr,
            "Error: fabric:peer_offline dial web on box: peer is offline"
        );
        let object: serde_json::Value =
            serde_json::from_str(report.stdout.as_deref().unwrap()).unwrap();
        assert_eq!(
            object,
            json!({"error": {
                "code": "peer_offline",
                "class": "fault",
                "message": "dial web on box: peer is offline",
                "peer": "box",
                "service": "web",
            }})
        );
        assert!(!report.stdout.unwrap().contains('\n'));
    }

    #[test]
    fn without_json_nothing_goes_to_stdout() {
        let error: anyhow::Error =
            FabricError::new(ErrorCode::DaemonNotRunning, "not running").into();
        let report = render(&error, false);
        assert_eq!(report.stdout, None);
        assert_eq!(report.status, EXIT_FAULT);
    }

    #[test]
    fn a_refusal_from_the_far_side_is_a_refusal_here() {
        let error = Err::<(), _>(anyhow::anyhow!(
            "fabric:peer_not_permitted_for_service peer not permitted for service \"web\""
        ))
        .context("dial web on box")
        .unwrap_err();
        let report = render(&error, true);
        assert_eq!(report.status, EXIT_REFUSAL);
        let object: serde_json::Value =
            serde_json::from_str(report.stdout.as_deref().unwrap()).unwrap();
        assert_eq!(object["error"]["code"], "peer_not_permitted_for_service");
        assert_eq!(object["error"]["class"], "refusal");
        assert_eq!(object["error"]["peer"], serde_json::Value::Null);
    }

    #[test]
    fn an_uncoded_failure_keeps_todays_status() {
        let error = anyhow::anyhow!("disk full");
        let report = render(&error, true);
        assert_eq!(report.status, EXIT_INTERNAL);
        assert_eq!(report.stderr, "Error: fabric:internal disk full");
        let object: serde_json::Value =
            serde_json::from_str(report.stdout.as_deref().unwrap()).unwrap();
        assert_eq!(object["error"]["code"], "internal");
        assert_eq!(object["error"]["class"], "fault");
    }

    #[test]
    fn usage_shares_clap_status() {
        let error: anyhow::Error =
            FabricError::new(ErrorCode::InvalidArgument, "not a node id").into();
        assert_eq!(render(&error, false).status, EXIT_USAGE);
    }
}
