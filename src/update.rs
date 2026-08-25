//! `fabric update` — replace this machine's fabric with a verified build.
//!
//! The recipe here is not new. It existed three times over, in `install.sh`, in
//! the README, and in a shell script that lived on one machine and was
//! base64-encoded across the wire to the others. Each copy knew a trap the
//! others did not. This is one copy, in the binary, with the traps as tests.
//!
//! WHAT THE CHECKSUM DOES AND DOES NOT DO. With `--url` and an explicit
//! `--sha256` it is a real check that the bytes are the ones the caller named.
//! On the release paths the sidecar is fetched from the SAME server as the
//! artifact, so it protects against corruption and truncation and NOT against a
//! compromised release. That is the ordinary trust model for a release install,
//! and it is written down here so nobody reads the word "verify" as more than it
//! is.

use anyhow::{Context, Result, bail};

/// `--check` answers three questions, not two. A sweep that cannot tell "the
/// release server is unreachable" from "an update is available" will act on the
/// wrong one.
pub const CHECK_EXIT_CURRENT: i32 = 0;
pub const CHECK_EXIT_AVAILABLE: i32 = 1;
pub const CHECK_EXIT_ERROR: i32 = 2;

const RELEASE_REPO: &str = "compoundingtech/fabric";

/// Where the artifact comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A published release. `None` means whatever is latest.
    Release { tag: Option<String> },
    /// An artifact the caller named, with the hash they expect it to have.
    Explicit { url: String, sha256: String },
}

/// The release target triple for the machine this binary was built for.
///
/// Deliberately built from `cfg!` rather than read from the environment: the
/// asset that gets installed must match the binary doing the installing, and a
/// runtime lookup could disagree with the compile that produced it.
pub fn target_triple() -> Result<&'static str> {
    Ok(match (cfg!(target_os = "macos"), cfg!(target_arch = "aarch64")) {
        (true, true) => "aarch64-apple-darwin",
        (false, true) => "aarch64-unknown-linux-gnu",
        (false, false) => "x86_64-unknown-linux-gnu",
        (true, false) => bail!("fabric publishes no release for this platform"),
    })
}

pub fn asset_name(target: &str) -> String {
    format!("fabric-{target}.tar.gz")
}

pub fn release_asset_url(tag: &str, asset: &str) -> String {
    format!("https://github.com/{RELEASE_REPO}/releases/download/{tag}/{asset}")
}

pub fn latest_release_api_url() -> String {
    format!("https://api.github.com/repos/{RELEASE_REPO}/releases/latest")
}

/// Decide where the artifact comes from, refusing the combination that would
/// run unverified bytes.
///
/// An explicit URL with no hash is remote code execution with good manners, so
/// it is rejected rather than defaulted. There is nothing sensible to default
/// to: the whole point of `--url` is that fabric does not know what is there.
pub fn resolve_source(
    tag: Option<String>,
    url: Option<String>,
    sha256: Option<String>,
) -> Result<Source> {
    match (tag, url, sha256) {
        (Some(_), Some(_), _) => {
            bail!("--tag and --url name two different artifacts; pass one of them")
        }
        (_, None, Some(_)) => {
            bail!("--sha256 only means something with --url; a release carries its own checksum")
        }
        (_, Some(url), None) => bail!(
            "--url requires --sha256.\n\
             \n\
             fabric will not install bytes it cannot check against a hash you \
             named, and it has nothing to compare {url} against on its own."
        ),
        (_, Some(url), Some(sha256)) => {
            let sha256 = normalise_sha256(&sha256)?;
            Ok(Source::Explicit { url, sha256 })
        }
        (tag, None, None) => Ok(Source::Release { tag }),
    }
}

/// Accept a hash in the shape a person actually pastes, and reject anything that
/// is not one. Length and alphabet are the whole contract.
fn normalise_sha256(raw: &str) -> Result<String> {
    let hash = raw.trim().to_ascii_lowercase();
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!(
            "--sha256 must be 64 hex characters, got {} characters: {raw}",
            hash.len()
        );
    }
    Ok(hash)
}

/// Pull the hash out of a published `.sha256` sidecar.
///
/// The sidecar reads `<hash>  dist/fabric-<target>.tar.gz`, carrying the path it
/// had on the builder. That path does not exist here, so handing the file to
/// `shasum -c` fails on the path rather than on the bytes. Take field one.
pub fn parse_sha256_sidecar(text: &str) -> Result<String> {
    let field = text
        .split_whitespace()
        .next()
        .context("the checksum sidecar was empty")?;
    normalise_sha256(field)
}

/// Compare the bytes we hold against the hash we expected.
pub fn verify_sha256(bytes: &[u8], expected: &str) -> Result<()> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let actual = hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    if actual != expected {
        bail!(
            "checksum mismatch, nothing was installed\n  expected  {expected}\n  actual    {actual}"
        );
    }
    Ok(())
}

/// Take the one `fabric` binary out of a release archive, refusing anything that
/// is not exactly that.
///
/// A release archive holds a single member named literally `fabric`. Not
/// `./fabric`, not a directory, not two files. Anything else is not a thing we
/// published, and unpacking it to find out would already have written it.
pub fn extract_fabric_binary(archive: &[u8]) -> Result<Vec<u8>> {
    use std::io::Read;
    let decoder = flate2::read::GzDecoder::new(archive);
    let mut tar = tar::Archive::new(decoder);
    let mut found: Option<Vec<u8>> = None;
    let mut names = Vec::new();
    for entry in tar.entries().context("the archive could not be read")? {
        let mut entry = entry.context("the archive holds an unreadable member")?;
        // Compare the RAW stored name. The parsed path agrees with it today —
        // the tar crate normalises `./fabric` on the way IN, not on the way out,
        // so both forms read back as `./fabric` and both would reject it. This
        // is belt and braces, not a fix: it cannot drift if the path parser ever
        // starts normalising, and the exact bytes are what we actually publish.
        //
        // I claimed the opposite here first, that the parsed form would accept
        // `./fabric`. Mutating the code back proved it would not. Left corrected
        // rather than left flattering.
        let raw = entry.path_bytes().into_owned();
        let name = String::from_utf8_lossy(&raw).into_owned();
        names.push(name.clone());
        if raw.as_slice() == b"fabric" {
            let mut bytes = Vec::new();
            entry
                .read_to_end(&mut bytes)
                .context("the fabric member could not be read")?;
            found = Some(bytes);
        }
    }
    if names.len() != 1 || found.is_none() {
        bail!(
            "the archive is not a fabric release: expected exactly one member named \
             `fabric`, found {names:?}"
        );
    }
    Ok(found.expect("checked above"))
}

/// The version a release tag promises. Tags are `v<version>`; the binary reports
/// `<version>`.
pub fn version_for_tag(tag: &str) -> &str {
    tag.strip_prefix('v').unwrap_or(tag)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a tarball the way the release workflow does, so the archive checks
    /// are tested against real bytes rather than a mock of them.
    fn make_archive(members: &[(&str, &[u8])]) -> Vec<u8> {
        let encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        let mut builder = tar::Builder::new(encoder);
        for (name, bytes) in members {
            let mut header = tar::Header::new_gnu();
            header.set_size(bytes.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            builder.append_data(&mut header, name, *bytes).unwrap();
        }
        builder.into_inner().unwrap().finish().unwrap()
    }

    const HASH: &str = "e6aac12fcf8be256aa713a017cfcd8d4e258f5f9f42e5bf8911ff189b73a1214";

    /// The one refusal that matters. `--url` with no hash means running bytes
    /// nobody checked, and there is nothing to default to: the whole point of
    /// `--url` is that fabric does not know what is there.
    #[test]
    fn an_explicit_url_without_a_hash_is_refused() {
        let error = resolve_source(None, Some("https://example.test/f.tar.gz".into()), None)
            .expect_err("an unverified url was accepted");
        let message = format!("{error}");
        assert!(
            message.contains("--sha256"),
            "the refusal must name what is missing: {message}"
        );
    }

    #[test]
    fn an_explicit_url_with_a_hash_is_accepted_and_the_hash_normalised() {
        let source = resolve_source(
            None,
            Some("file:///tmp/f.tar.gz".into()),
            // Pasted with stray case and whitespace, as a person would.
            Some(format!("  {}  ", HASH.to_ascii_uppercase())),
        )
        .expect("a hashed url was refused");
        assert_eq!(
            source,
            Source::Explicit {
                url: "file:///tmp/f.tar.gz".into(),
                sha256: HASH.into(),
            }
        );
    }

    #[test]
    fn a_hash_that_is_not_a_sha256_is_refused() {
        for bad in ["deadbeef", "", &"z".repeat(64)] {
            assert!(
                resolve_source(None, Some("file:///f".into()), Some(bad.into())).is_err(),
                "accepted {bad:?} as a sha256"
            );
        }
    }

    #[test]
    fn naming_two_sources_at_once_is_refused() {
        assert!(
            resolve_source(Some("v0.2.0".into()), Some("file:///f".into()), Some(HASH.into()))
                .is_err(),
            "--tag and --url name different artifacts and must not combine"
        );
    }

    #[test]
    fn a_hash_without_a_url_is_refused() {
        assert!(
            resolve_source(None, None, Some(HASH.into())).is_err(),
            "a release carries its own checksum, so --sha256 alone is a mistake worth naming"
        );
    }

    #[test]
    fn no_options_means_the_latest_release() {
        assert_eq!(
            resolve_source(None, None, None).unwrap(),
            Source::Release { tag: None }
        );
    }

    /// The sidecar carries the path it had on the builder, which does not exist
    /// here. Taking field one is the difference between checking the bytes and
    /// failing on a directory name.
    #[test]
    fn the_sidecar_builder_path_is_ignored() {
        let sidecar = format!("{HASH}  dist/fabric-aarch64-apple-darwin.tar.gz\n");
        assert_eq!(parse_sha256_sidecar(&sidecar).unwrap(), HASH);
    }

    #[test]
    fn a_checksum_mismatch_is_an_error_that_names_both_sides() {
        let error = verify_sha256(b"not the release", HASH).expect_err("a mismatch was accepted");
        let message = format!("{error}");
        assert!(message.contains(HASH), "the expected hash is missing: {message}");
        assert!(
            message.contains("nothing was installed"),
            "a mismatch must say that it changed nothing: {message}"
        );
    }

    #[test]
    fn a_matching_checksum_passes() {
        use sha2::{Digest, Sha256};
        let bytes = b"some release bytes";
        let hash = Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect::<String>();
        verify_sha256(bytes, &hash).expect("a matching checksum was rejected");
    }

    #[test]
    fn a_release_archive_yields_the_binary() {
        let archive = make_archive(&[("fabric", b"ELF-ish")]);
        assert_eq!(extract_fabric_binary(&archive).unwrap(), b"ELF-ish");
    }

    /// Write a tar header by hand so the stored name is EXACTLY what we say.
    ///
    /// `tar::Builder` normalises `./fabric` to `fabric` on the way in, so it
    /// cannot produce the archive shape this test is about. GNU tar does produce
    /// it — `tar -czf x.tgz .` stores the dot-slash — so the fixture has to be
    /// built by hand or the test would silently be checking `fabric`.
    fn make_archive_with_raw_name(name: &str, bytes: &[u8]) -> Vec<u8> {
        let mut header = [0u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100..107].copy_from_slice(b"0000755");
        header[108..115].copy_from_slice(b"0000000");
        header[116..123].copy_from_slice(b"0000000");
        let size = format!("{:011o}", bytes.len());
        header[124..135].copy_from_slice(size.as_bytes());
        header[136..147].copy_from_slice(b"00000000000");
        header[148..156].copy_from_slice(b"        ");
        header[156] = b'0';
        header[257..263].copy_from_slice(b"ustar\0");
        header[263..265].copy_from_slice(b"00");
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        let checksum = format!("{checksum:06o}\0 ");
        header[148..156].copy_from_slice(checksum.as_bytes());

        let mut tar = Vec::new();
        tar.extend_from_slice(&header);
        tar.extend_from_slice(bytes);
        tar.resize(tar.len().div_ceil(512) * 512, 0);
        tar.extend_from_slice(&[0u8; 1024]);

        use std::io::Write;
        let mut encoder =
            flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&tar).unwrap();
        encoder.finish().unwrap()
    }

    /// `./fabric` is what GNU tar stores for `tar -czf x.tgz .`, and it is not
    /// what the release workflow emits. Accepting it would mean accepting
    /// archives we did not build.
    #[test]
    fn an_archive_whose_member_is_dot_slash_fabric_is_refused() {
        // The fixture really does carry the dot-slash, or this proves nothing.
        let archive = make_archive_with_raw_name("./fabric", b"ELF-ish");
        let decoder = flate2::read::GzDecoder::new(&archive[..]);
        let mut probe = tar::Archive::new(decoder);
        let stored = probe
            .entries()
            .unwrap()
            .next()
            .unwrap()
            .unwrap()
            .path_bytes()
            .into_owned();
        assert_eq!(
            stored.as_slice(),
            b"./fabric",
            "the fixture was normalised, so the test below would prove nothing"
        );

        assert!(
            extract_fabric_binary(&archive).is_err(),
            "./fabric is not the member name a fabric release has"
        );
    }

    #[test]
    fn an_archive_with_an_extra_member_is_refused() {
        let archive = make_archive(&[("fabric", b"ELF-ish"), ("install.sh", b"rm -rf /")]);
        assert!(
            extract_fabric_binary(&archive).is_err(),
            "a release holds one member; a second one is somebody else's archive"
        );
    }

    #[test]
    fn an_archive_without_fabric_is_refused() {
        let archive = make_archive(&[("something-else", b"nope")]);
        assert!(extract_fabric_binary(&archive).is_err());
    }

    #[test]
    fn a_tag_names_the_version_the_binary_will_report() {
        assert_eq!(version_for_tag("v0.2.0+76376d4"), "0.2.0+76376d4");
        // Idempotent, because a caller may paste either form.
        assert_eq!(version_for_tag("0.2.0+76376d4"), "0.2.0+76376d4");
    }

    #[test]
    fn the_release_url_matches_what_the_workflow_publishes() {
        let asset = asset_name("aarch64-apple-darwin");
        assert_eq!(asset, "fabric-aarch64-apple-darwin.tar.gz");
        assert_eq!(
            release_asset_url("v0.2.0+76376d4", &asset),
            "https://github.com/compoundingtech/fabric/releases/download/v0.2.0+76376d4/fabric-aarch64-apple-darwin.tar.gz"
        );
    }
}
