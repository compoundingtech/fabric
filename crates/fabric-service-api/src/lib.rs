//! What a fabric service needs from the base network, and nothing more.
//!
//! The base network proves who a peer is, checks that the peer may use a
//! service, and hands the service one stream: an authenticated stream from peer
//! X for service Y. A service is written against this crate alone. It never sees
//! the daemon, its endpoint, its configuration files or its connection
//! management, so the daemon can change any of those without touching a
//! service, and a service can change without touching the daemon.
//!
//! The daemon also runs a small local side for each service: a Unix socket the
//! local command connects to, bridged to a stream the daemon opened to the peer.
//! When the daemon has something to tell that local command which did not come
//! from the peer, such as a refusal or a reconnect, it asks the service to say
//! it in the service's own framing ([`Service::notice`]).
//!
//! A service that logs through `tracing` uses a target under `fabric::`, the
//! prefix the daemon's log filter admits.

use std::{future::Future, path::PathBuf, pin::Pin, sync::Arc};

use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::sync::CancellationToken;

pub type BoxFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;
pub type StreamRead = Box<dyn AsyncRead + Send + Unpin>;
pub type StreamWrite = Box<dyn AsyncWrite + Send + Unpin>;

/// The exit status a command-style service reports when the peer refused it.
///
/// The same 126 `sh` uses for "found but cannot execute", so a script can tell
/// a refusal from a command that ran and failed.
pub const REFUSED_EXIT_CODE: i32 = 126;

/// One authenticated stream from one peer for one service.
pub struct PeerStream {
    /// The peer's id as the handshake proved it: never a name the peer chose.
    pub peer: String,
    pub read: StreamRead,
    /// The service owns the write half and shuts it down when it is done, so
    /// the peer sees the end of the reply rather than a reset.
    pub write: StreamWrite,
    /// Cancelled when the base network ends the session under the service: a
    /// resumable session that was reaped or dropped. A one-shot stream is never
    /// cancelled; it ends when either side closes it.
    pub closed: CancellationToken,
    /// What this machine granted the peer, for a service that checks its own
    /// grants ([`Access::Grants`]). `None` for every other service, whose single
    /// grant the base network already checked.
    pub grants: Option<Arc<dyn Grants>>,
}

/// One wire protocol a service speaks.
#[derive(Debug, Clone, Copy)]
pub struct Protocol {
    /// The ALPN, which is also the protocol's name on the wire.
    pub alpn: &'static [u8],
    /// Served inside a resumable session that outlives any one connection,
    /// rather than on a single stream.
    pub resumable: bool,
    /// The event label of the log line that records an accepted connection.
    pub accept_event: &'static str,
}

impl Protocol {
    pub fn name(&self) -> &'static str {
        std::str::from_utf8(self.alpn).expect("a protocol name is UTF-8")
    }
}

/// Who decides whether a peer may use the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Access {
    /// The base network passes the stream on only when the peer's grants name
    /// the service.
    AllowList,
    /// The service reads its request first and checks a narrower grant itself,
    /// through [`PeerStream::grants`], because the grant names something only
    /// the request carries.
    Grants,
}

/// What this machine's owner granted the peer on the other end of a stream.
pub trait Grants: Send + Sync {
    /// The name this machine knows the peer by, if it has one.
    fn peer_name(&self) -> Option<String>;
    /// May the peer use `permission`?
    fn may(&self, permission: &str) -> Result<(), Denial>;
    /// The local path the owner shared with this service under `name`.
    fn shared(&self, name: &str) -> Option<PathBuf>;
}

/// Why a grant check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Denial {
    /// The peer has no grants on this machine at all, as opposed to not having
    /// this one. The two need different advice.
    pub no_grants: bool,
    pub reason: String,
}

/// How the daemon joins a local command's socket to the peer's stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Bridge {
    /// Stop both directions at the first error in either.
    FirstError,
    /// Keep delivering the peer's reply after the request direction fails: a
    /// refusal is sent and the stream closed without the request being read, and
    /// the refusal is the useful part.
    WholeReply,
}

/// Something the daemon needs to tell a service's local command.
///
/// `error` is already written out: in full, with its causes, for the notices
/// that say a wait is in progress, and as its outermost message for the others.
#[derive(Debug, Clone, Copy)]
pub enum Notice<'a> {
    /// The peer refused the service. Waiting will not change that.
    Refused { error: &'a str },
    /// The daemon could not open a stream to the peer for the service.
    Unavailable { error: &'a str },
    /// The peer does not speak the newest protocol; the older one is used.
    FallingBack,
    /// No resumable session could be started yet; trying again after `delay`.
    Probing {
        error: &'a str,
        delay: std::time::Duration,
    },
    /// The older protocol could not be reached either; trying again after
    /// `delay`.
    RetryingFallback {
        error: &'a str,
        delay: std::time::Duration,
    },
    /// A resumable session lost its transport and is reconnecting.
    Reconnecting {
        error: &'a str,
        attempt: u64,
        delay: std::time::Duration,
    },
    /// A resumable session reconnected and resumed where it was.
    Resumed,
    /// A resumable session could not be resumed.
    ResumeFailed { error: &'a str },
}

/// A fabric service: the protocols it speaks and how it serves one stream.
pub trait Service: Send + Sync + 'static {
    /// The word a person writes in a peer's allow array.
    fn name(&self) -> &'static str;

    /// Every protocol the service answers, newest first. A local command is
    /// connected with the newest the peer speaks.
    fn protocols(&self) -> &'static [Protocol];

    fn access(&self) -> Access {
        Access::AllowList
    }

    /// Serve one authenticated stream until it is done.
    fn serve(&self, stream: PeerStream) -> BoxFuture<'static, anyhow::Result<()>>;

    /// `notice` in the service's own framing, for its local command. `None`
    /// writes nothing and the command sees the stream end.
    fn notice(&self, _notice: &Notice<'_>) -> Option<Vec<u8>> {
        None
    }

    fn bridge(&self) -> Bridge {
        Bridge::FirstError
    }

    /// One local socket serves every command for this peer, rather than each
    /// command getting a fresh one.
    fn shares_local_socket(&self) -> bool {
        false
    }
}
