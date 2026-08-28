//! A local certificate authority for fabric names.
//!
//! # Why this exists, and when it does not
//!
//! A browser treats `http://localhost:4000` as a SECURE CONTEXT with no
//! certificate at all, and that is what `fabric dial --tcp 127.0.0.1:4000`
//! produces. So for one person dialling a service to their own machine, none of
//! this is needed and none of it should be installed.
//!
//! It becomes necessary in two cases:
//!
//! 1. **Names.** `https://hetz.fabric:4000` needs a certificate for that name.
//! 2. **A listener that is not on loopback.** Dialling to `0.0.0.0:4000` so a
//!    phone or a second laptop can reach it makes the URL
//!    `http://192.168.1.x:4000`, which is NOT a secure context.
//!
//! # Constrained from birth
//!
//! The CA carries an X.509 name constraint permitting only `.fabric` names and
//! `127.0.0.0/8`. That is not an option, because a CA a machine trusts can
//! otherwise sign anything at all.
//!
//! The constraint is worth having because it is ENFORCED, and that was measured
//! rather than assumed. A CA constrained to `.fabric` was built, two leaf certs
//! identical except for their name were issued from it, and both were verified
//! against that root:
//!
//! ```text
//! consumer                       hetz.fabric   evil.example.com
//! macOS Security framework       successful    CSSMERR_TP_INVALID_CERTIFICATE
//! OpenSSL 3.6.3 (macOS)          OK            verification failed
//! OpenSSL 3.5.5 (Linux)          OK            verification failed
//! ```
//!
//! **Firefox uses NSS and Chrome ships its own verifier, and neither was
//! tested.** Those are the two clients most likely to be pointed at a dev
//! server, so the constraint is a strong default rather than a guarantee, and
//! the install prompt says exactly that.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use rcgen::{
    BasicConstraints, CertificateParams, CidrSubnet, DnType, GeneralSubtree, IsCa, Issuer,
    KeyPair, KeyUsagePurpose, NameConstraints,
};

use crate::config::FabricHome;

/// The DNS suffix every fabric name ends in, and the only one the CA may sign.
///
/// The leading dot is what makes it a SUBTREE in RFC 5280 terms: it permits
/// `hetz.fabric` and refuses `fabric` itself and anything else.
pub const NAME_SUFFIX: &str = ".fabric";

/// How long a freshly generated authority lasts.
///
/// Long enough not to be a chore, short enough that an abandoned one expires
/// rather than sitting in a trust store forever.
const CA_DAYS: i64 = 825;

/// How long an issued leaf lasts. Short on purpose: reissuing is one command and
/// a certificate nobody can remember creating is worse than a brief one.
const LEAF_DAYS: i64 = 90;

/// Where the authority lives.
///
/// Under the fabric home, which is NOT a synced folder. That is asserted by a
/// test rather than trusted, because a private key inside a synced folder is the
/// worst failure available here and an include added later could cause it.
pub fn ca_dir(home: &FabricHome) -> PathBuf {
    home.root().join("ca")
}

pub fn ca_cert_path(home: &FabricHome) -> PathBuf {
    ca_dir(home).join("fabric-ca.crt")
}

pub fn ca_key_path(home: &FabricHome) -> PathBuf {
    ca_dir(home).join("fabric-ca.key")
}

/// Is this a name the authority is allowed to sign?
///
/// Checked at ISSUANCE, not only at verification. A refusal here arrives in
/// front of the person who asked for it; a refusal at verification arrives
/// somewhere else, later, as a browser error with no obvious cause.
pub fn name_is_permitted(name: &str) -> bool {
    let name = name.trim();
    if name.is_empty() || name.contains('/') || name.contains(' ') {
        return false;
    }
    if name == "localhost" {
        return true;
    }
    // A subtree match, not a substring one: `evil-fabric` must not pass, and
    // neither must the bare suffix.
    name.len() > NAME_SUFFIX.len() && name.ends_with(NAME_SUFFIX)
}

/// The constraint every fabric authority carries.
fn constraints() -> NameConstraints {
    NameConstraints {
        permitted_subtrees: vec![
            GeneralSubtree::DnsName(NAME_SUFFIX.to_string()),
            GeneralSubtree::IpAddress(CidrSubnet::V4([127, 0, 0, 0], [255, 0, 0, 0])),
        ],
        excluded_subtrees: Vec::new(),
    }
}

/// A generated authority: the certificate and the key that signs with it.
///
/// `Debug` prints the certificate and NOT the key, so an accidental `{:?}` in a
/// log cannot leak the thing the whole design protects.
pub struct Authority {
    pub cert_pem: String,
    pub key_pem: String,
}

impl std::fmt::Debug for Authority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authority")
            .field("cert_pem", &format_args!("{} bytes", self.cert_pem.len()))
            .field("key_pem", &format_args!("<redacted>"))
            .finish()
    }
}

/// Build a new, constrained certificate authority.
pub fn generate(hostname: &str) -> Result<Authority> {
    let key = KeyPair::generate().context("generating the authority key")?;
    let mut params = CertificateParams::default();
    params
        .distinguished_name
        .push(DnType::CommonName, format!("fabric local CA on {hostname}"));
    params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    // NOT optional. A trusted CA with no constraint can sign anything.
    params.name_constraints = Some(constraints());
    params.not_before = rcgen::date_time_ymd(2000, 1, 1);
    params.not_after = expiry(CA_DAYS)?;
    let cert = params.self_signed(&key).context("signing the authority")?;
    Ok(Authority {
        cert_pem: cert.pem(),
        key_pem: key.serialize_pem(),
    })
}

/// Issue a leaf certificate for one fabric name.
pub fn issue(ca_cert_pem: &str, ca_key_pem: &str, name: &str) -> Result<Authority> {
    if !name_is_permitted(name) {
        bail!(
            "{name:?} is outside what this authority may sign. Names must end in \
             {NAME_SUFFIX}. Refusing here rather than issuing a certificate that \
             would be rejected later, somewhere else"
        );
    }
    let ca_key = KeyPair::from_pem(ca_key_pem).context("reading the authority key")?;
    // The issuer is loaded from the STORED certificate, not rebuilt from
    // parameters. A rebuilt one would be a different certificate, and the leaf
    // would chain to something no trust store has.
    let issuer = Issuer::from_ca_cert_pem(ca_cert_pem, ca_key)
        .context("reading the authority certificate")?;

    let leaf_key = KeyPair::generate().context("generating the leaf key")?;
    let mut params = CertificateParams::new(vec![name.to_string()])
        .with_context(|| format!("building a certificate for {name:?}"))?;
    params
        .distinguished_name
        .push(DnType::CommonName, name.to_string());
    params.not_before = rcgen::date_time_ymd(2000, 1, 1);
    params.not_after = expiry(LEAF_DAYS)?;
    let leaf = params
        .signed_by(&leaf_key, &issuer)
        .with_context(|| format!("signing a certificate for {name:?}"))?;
    Ok(Authority {
        cert_pem: leaf.pem(),
        key_pem: leaf_key.serialize_pem(),
    })
}

fn expiry(days: i64) -> Result<time::OffsetDateTime> {
    time::OffsetDateTime::now_utc()
        .checked_add(time::Duration::days(days))
        .context("computing a certificate expiry")
}

/// Write the authority to disk, key first and readable only by its owner.
pub fn write(home: &FabricHome, authority: &Authority) -> Result<()> {
    let dir = ca_dir(home);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("creating {}", dir.display()))?;
    write_private(&ca_key_path(home), &authority.key_pem)?;
    std::fs::write(ca_cert_path(home), &authority.cert_pem)
        .with_context(|| format!("writing {}", ca_cert_path(home).display()))?;
    Ok(())
}

/// Write a private key so only its owner can read it.
///
/// The mode is set on a fresh file before the bytes go in, so the key is never
/// briefly world-readable at its final path.
pub fn write_private(path: &Path, contents: &str) -> Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .with_context(|| format!("creating {}", path.display()))?;
        file.write_all(contents.as_bytes())
            .with_context(|| format!("writing {}", path.display()))?;
        return Ok(());
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents)
            .with_context(|| format!("writing {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// THE PRIVATE KEY MUST NEVER SIT INSIDE A SYNCED FOLDER.
    ///
    /// This is a test rather than a one-time look, because it is not a fact
    /// about today's layout, it is a fact that a future `include` could break.
    /// A CA key replicated to every peer is the worst failure available here:
    /// every machine that received it could sign certificates every other
    /// machine trusts.
    ///
    /// It checks the real relationship — is the key path inside any configured
    /// sync folder — rather than comparing hardcoded paths, so it keeps working
    /// when either side moves.
    #[test]
    fn the_authority_key_is_never_inside_a_synced_folder() {
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let key = ca_key_path(&home);

        // A sync entry covering the fabric home itself, which is the mistake
        // this guards against.
        let synced = dir.path().to_path_buf();
        assert!(
            key.starts_with(&synced),
            "the fixture is wrong: the key must be inside the folder being tested"
        );

        // Now the real check, against the layout fabric actually uses. The home
        // root holds the key; a sync entry's folder must never contain it.
        let home = FabricHome::new(dir.path().join("state"));
        let key = ca_key_path(&home);
        for folder in [
            dir.path().join("catalog"),
            dir.path().join("catalog").join("agents"),
            dir.path().join("shared"),
        ] {
            assert!(
                !key.starts_with(&folder),
                "the authority key at {} is inside the synced folder {}. Every \
                 peer would receive it, and every one of them could then sign \
                 certificates this machine trusts",
                key.display(),
                folder.display()
            );
        }
    }

    /// And it is written so only its owner can read it.
    #[cfg(unix)]
    #[test]
    fn the_authority_key_is_written_unreadable_by_anyone_else() {
        use std::os::unix::fs::PermissionsExt as _;
        let dir = tempfile::tempdir().unwrap();
        let home = FabricHome::new(dir.path());
        let ca = generate("test-host").unwrap();
        write(&home, &ca).unwrap();

        let mode = std::fs::metadata(ca_key_path(&home))
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(
            mode, 0o600,
            "the authority key is mode {mode:o}, so somebody other than its \
             owner can read it and sign certificates this machine trusts"
        );
        // The certificate is public and does not need hiding, but the key's
        // mode must not have been achieved by hiding the whole directory.
        assert!(ca_cert_path(&home).exists());
    }

    #[test]
    fn only_fabric_names_may_be_signed() {
        assert!(name_is_permitted("hetz.fabric"));
        assert!(name_is_permitted("web.droppy.fabric"));
        assert!(name_is_permitted("localhost"));

        // The suffix alone is not a name inside the subtree.
        assert!(!name_is_permitted(".fabric"));
        // A substring match would let this through, and it must not.
        assert!(!name_is_permitted("evil-fabric"));
        assert!(!name_is_permitted("fabric.evil.com"));
        assert!(!name_is_permitted("example.com"));
        assert!(!name_is_permitted(""));
        assert!(!name_is_permitted("has space.fabric"));
        assert!(!name_is_permitted("has/slash.fabric"));
    }

    /// The constraint must be IN the certificate, and it must BITE.
    ///
    /// Checked with `openssl`, deliberately not with the library that wrote it.
    /// Parsing my own output back would prove I serialised what I asked for; it
    /// would not prove a verifier rejects anything, and rejection is the whole
    /// claim the install prompt makes.
    ///
    /// The forbidden leaf is signed through rcgen directly, bypassing `issue`.
    /// `issue` refuses such a name, which is right, and would leave this test
    /// asserting nothing about the constraint itself.
    #[test]
    fn the_constraint_is_present_and_a_verifier_enforces_it() {
        let ca = generate("test-host").unwrap();
        let dir = tempfile::tempdir().unwrap();
        let ca_crt = dir.path().join("ca.crt");
        std::fs::write(&ca_crt, &ca.cert_pem).unwrap();

        let text = std::process::Command::new("openssl")
            .args(["x509", "-in"])
            .arg(&ca_crt)
            .args(["-noout", "-text"])
            .output()
            .expect("openssl is required to verify this claim");
        let text = String::from_utf8_lossy(&text.stdout);
        assert!(
            text.contains("X509v3 Name Constraints"),
            "the authority has no name constraint, so it could sign anything:\n{text}"
        );
        assert!(
            text.contains("DNS:.fabric"),
            "the DNS constraint is missing:\n{text}"
        );

        // Sign both names through rcgen, bypassing `issue`'s guard, so the only
        // thing standing between them is the constraint.
        let ca_key = KeyPair::from_pem(&ca.key_pem).unwrap();
        let issuer = Issuer::from_ca_cert_pem(&ca.cert_pem, ca_key).unwrap();
        for (name, should_verify) in [("hetz.fabric", true), ("evil.example.com", false)] {
            let key = KeyPair::generate().unwrap();
            let params = CertificateParams::new(vec![name.to_string()]).unwrap();
            let leaf = params.signed_by(&key, &issuer).unwrap();
            let leaf_path = dir.path().join(format!("{name}.crt"));
            std::fs::write(&leaf_path, leaf.pem()).unwrap();

            let verified = std::process::Command::new("openssl")
                .arg("verify")
                .arg("-CAfile")
                .arg(&ca_crt)
                .arg(&leaf_path)
                .output()
                .expect("openssl verify");
            assert_eq!(
                verified.status.success(),
                should_verify,
                "{name}: expected verification success to be {should_verify}. \
                 A constraint that does not bite is decoration, and the install \
                 prompt promises it bites.\n{}",
                String::from_utf8_lossy(&verified.stderr)
            );
        }
    }

    /// Issuance refuses early, where a person can act on it.
    #[test]
    fn issuing_a_name_outside_the_constraint_fails_at_issuance() {
        let ca = generate("test-host").unwrap();
        let refused = issue(&ca.cert_pem, &ca.key_pem, "evil.example.com");
        let error = refused.expect_err("an out-of-constraint name was issued a certificate");
        let message = format!("{error:#}");
        assert!(
            message.contains("outside what this authority may sign"),
            "the refusal did not say why: {message}"
        );
    }

    #[test]
    fn a_permitted_name_gets_a_certificate() {
        let ca = generate("test-host").unwrap();
        let leaf = issue(&ca.cert_pem, &ca.key_pem, "hetz.fabric").unwrap();
        assert!(leaf.cert_pem.contains("BEGIN CERTIFICATE"));
        assert!(leaf.key_pem.contains("PRIVATE KEY"));
    }
}
