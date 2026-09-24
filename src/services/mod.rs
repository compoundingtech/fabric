//! The services a daemon serves, and the one place that names the built-in set.
//!
//! The daemon knows a service only through [`Services`]: which protocols it
//! answers, the word its grant uses, and how to hand it an authenticated stream.
//! What a service does with that stream lives in its module here, which reaches
//! the core only through `fabric-service-api`; a test below holds each module to
//! that.
//!
//! They are modules rather than crates of their own on purpose. As four crates
//! they grew the release binary by about 0.8 percent: a release build copies
//! each generic instantiation (`anyhow`, `alloc`, `tokio`, `serde_json`) into
//! every crate that uses it, and thin LTO made that larger, not smaller. As
//! modules behind the same interface the binary is slightly smaller than it was
//! before the interface existed.

pub mod exec;
pub mod git;
pub mod send_file;
pub mod shell;

use std::{
    fmt,
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::Result;
use fabric_service_api::{Denial, Grants};
use iroh::EndpointId;

pub use fabric_service_api::{Access, BoxFuture, Bridge, Notice, PeerStream, Protocol, Service};

use crate::{
    config::{Denied, FabricHome, PeerBook},
    daemon::DaemonState,
};

/// An ordered set of services, each answering its own protocols.
#[derive(Clone, Default)]
pub struct Services {
    entries: Arc<Vec<Arc<dyn Service>>>,
}

impl fmt::Debug for Services {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_list().entries(self.names()).finish()
    }
}

impl Services {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a service.
    ///
    /// Panics when its name or one of its protocols is already taken, including
    /// by the base network's own: two answers to one protocol would make which
    /// one a peer reaches depend on registration order.
    pub fn with(self, service: impl Service) -> Self {
        assert!(
            self.named(service.name()).is_none(),
            "service {:?} is registered twice",
            service.name()
        );
        for protocol in service.protocols() {
            assert!(
                self.find(protocol.alpn).is_none()
                    && !crate::daemon::is_base_network_alpn(protocol.alpn),
                "protocol {:?} already has a service",
                protocol.name()
            );
        }
        let mut entries = (*self.entries).clone();
        entries.push(Arc::new(service));
        Self {
            entries: Arc::new(entries),
        }
    }

    /// The service answering `alpn`, and which of its protocols that is.
    pub fn find(&self, alpn: &[u8]) -> Option<(&Arc<dyn Service>, &'static Protocol)> {
        self.entries.iter().find_map(|service| {
            service
                .protocols()
                .iter()
                .find(|protocol| protocol.alpn == alpn)
                .map(|protocol| (service, protocol))
        })
    }

    pub fn named(&self, name: &str) -> Option<&Arc<dyn Service>> {
        self.entries.iter().find(|service| service.name() == name)
    }

    pub fn names(&self) -> impl Iterator<Item = &'static str> + '_ {
        self.entries.iter().map(|service| service.name())
    }

    pub fn alpns(&self) -> impl Iterator<Item = &'static [u8]> + '_ {
        self.entries
            .iter()
            .flat_map(|service| service.protocols().iter().map(|protocol| protocol.alpn))
    }
}

/// The services every fabric daemon serves.
pub fn builtin(home: &FabricHome) -> Services {
    Services::new()
        .with(shell::Shell)
        .with(exec::Exec)
        .with(send_file::SendFile::new(home.root().join("inbox")))
        .with(git::Git::default())
}

/// A peer's grants as `peers.toml` held them when its stream arrived.
pub(crate) struct BookGrants {
    book: PeerBook,
    peer: EndpointId,
    service: &'static str,
}

impl BookGrants {
    pub(crate) fn new(book: PeerBook, peer: EndpointId, service: &'static str) -> Self {
        Self {
            book,
            peer,
            service,
        }
    }
}

impl Grants for BookGrants {
    fn peer_name(&self) -> Option<String> {
        self.book
            .peers()
            .iter()
            .find(|entry| entry.id == self.peer)
            .and_then(|entry| entry.name.clone())
    }

    fn may(&self, permission: &str) -> Result<(), Denial> {
        self.book
            .may(&self.peer, permission)
            .map_err(|denied| Denial {
                no_grants: matches!(denied, Denied::NoGrants { .. }),
                reason: denied.to_string(),
            })
    }

    /// `peers.toml` keeps one table of shared paths: the repositories shared
    /// with Git.
    fn shared(&self, name: &str) -> Option<PathBuf> {
        if self.service != git::SERVICE {
            return None;
        }
        self.book.git_remote(name).map(|remote| remote.path.clone())
    }
}

/// `fabric send-file`: hand one file to `peer`, streamed from disk.
pub(crate) async fn send_file(
    state: &DaemonState,
    peer: &str,
    name: &str,
    path: &Path,
) -> Result<()> {
    let stream = state
        .open_stream(peer, send_file::SEND_FILE_ALPN)
        .await?;
    send_file::send_file(stream, name, path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Named(&'static str, &'static [Protocol]);

    impl Service for Named {
        fn name(&self) -> &'static str {
            self.0
        }

        fn protocols(&self) -> &'static [Protocol] {
            self.1
        }

        fn serve(&self, _stream: PeerStream) -> BoxFuture<'static, Result<()>> {
            Box::pin(async { Ok(()) })
        }
    }

    const ONE: &[Protocol] = &[Protocol {
        alpn: b"test/one/0",
        resumable: false,
        accept_event: "one_accept",
    }];
    const ECHO: &[Protocol] = &[Protocol {
        alpn: b"fabric/echo/0",
        resumable: false,
        accept_event: "echo_accept",
    }];

    #[test]
    fn the_builtin_set_is_the_four_services_in_order() {
        let dir = tempfile::tempdir().unwrap();
        let services = builtin(&FabricHome::new(dir.path()));
        assert_eq!(
            services.names().collect::<Vec<_>>(),
            ["shell", "exec", "send-file", "git"]
        );
        let (service, protocol) = services.find(shell::SHELL_ALPN).unwrap();
        assert_eq!((service.name(), protocol.resumable), ("shell", false));
        let (service, protocol) = services.find(shell::RESUMABLE_SHELL_ALPN).unwrap();
        assert_eq!((service.name(), protocol.resumable), ("shell", true));
        assert!(services.find(b"fabric/echo/0").is_none());
    }

    /// The services live in this crate, but they reach the core only through
    /// `fabric-service-api`, as they would from a crate of their own. A path
    /// into the core from any of them fails here.
    #[test]
    fn the_services_reach_the_core_only_through_the_service_interface() {
        let sources = [
            ("exec.rs", include_str!("exec.rs")),
            ("git.rs", include_str!("git.rs")),
            ("send_file.rs", include_str!("send_file.rs")),
            ("shell/mod.rs", include_str!("shell/mod.rs")),
            ("shell/client.rs", include_str!("shell/client.rs")),
            ("shell/terminal.rs", include_str!("shell/terminal.rs")),
        ];
        for (file, source) in sources {
            for (index, line) in source.lines().enumerate() {
                assert!(
                    !line.contains("crate::") && !line.contains("super::super"),
                    "services/{file}:{} reaches into the core: {}",
                    index + 1,
                    line.trim()
                );
            }
        }
    }

    #[test]
    #[should_panic(expected = "already has a service")]
    fn one_protocol_has_one_service() {
        let _ = Services::new().with(Named("a", ONE)).with(Named("b", ONE));
    }

    #[test]
    #[should_panic(expected = "registered twice")]
    fn one_name_has_one_service() {
        let _ = Services::new().with(Named("a", ONE)).with(Named("a", &[]));
    }

    #[test]
    #[should_panic(expected = "already has a service")]
    fn a_service_cannot_take_a_base_network_protocol() {
        let _ = Services::new().with(Named("echo-again", ECHO));
    }
}
