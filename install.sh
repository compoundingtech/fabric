#!/usr/bin/env sh
set -eu

REPO="compoundingtech/fabric"
REPO_URL="https://github.com/$REPO"
INSTALL_DIR="${FABRIC_BIN_DIR:-${BIN_DIR:-${FABRIC_INSTALL_DIR:-$HOME/.local/bin}}}"
REQUESTED_VERSION="${FABRIC_VERSION:-latest}"
MODE="auto"
SOURCE_FALLBACK="${FABRIC_SOURCE_FALLBACK:-0}"
INSTALLED_TARGET=""

usage() {
  cat <<'EOF'
usage: install.sh [--from-source] [--source-fallback] [--version VERSION]

Options:
  --from-source       Build from source explicitly. From a checkout, builds that
                      checkout. From curl|sh, builds the requested release tag.
  --source-fallback   If prebuilt download/install fails, build the requested
                      release tag from source. This is off by default.
  --version VERSION   Install a specific release tag, e.g. v0.1.7 or 0.1.7.
                      Defaults to latest.

Environment:
  FABRIC_BIN_DIR      Install directory. Defaults to ~/.local/bin.
  FABRIC_VERSION      Same as --version.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --from-source)
      MODE="source"
      ;;
    --source-fallback)
      SOURCE_FALLBACK="1"
      ;;
    --version)
      if [ "$#" -lt 2 ]; then
        echo "error: --version requires a value" >&2
        exit 1
      fi
      REQUESTED_VERSION="$2"
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "error: unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
  shift
done

echo "fabric: experimental prototype installer"

warn() {
  echo "WARNING: $*" >&2
}

die() {
  echo "error: $*" >&2
  exit 1
}

fetch() {
  url="$1"
  dest="$2"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "$dest"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO "$dest" "$url"
  else
    return 1
  fi
}

fetch_stdout() {
  url="$1"
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url"
  elif command -v wget >/dev/null 2>&1; then
    wget -qO- "$url"
  else
    return 1
  fi
}

resolve_target_tag() {
  case "$REQUESTED_VERSION" in
    latest)
      raw=$(fetch_stdout "https://api.github.com/repos/$REPO/releases/latest") || return 1
      tag=$(printf '%s\n' "$raw" | sed -n 's/.*"tag_name"[[:space:]]*:[[:space:]]*"\([^"]*\)".*/\1/p' | head -n 1)
      [ -n "$tag" ] || return 1
      echo "$tag"
      ;;
    v*)
      echo "$REQUESTED_VERSION"
      ;;
    *)
      echo "v$REQUESTED_VERSION"
      ;;
  esac
}

require_cargo() {
  if ! command -v cargo >/dev/null 2>&1; then
    die "cargo is required for source installs"
  fi
}

install_binary() {
  src="$1"
  target="$INSTALL_DIR/fabric"
  mkdir -p "$INSTALL_DIR"

  if [ -e "$target" ] || [ -L "$target" ]; then
    if ! "$target" --help 2>/dev/null | grep -q "Local socket facade for iroh-backed cross-machine transports"; then
      die "refusing to overwrite non-fabric file at $target"
    fi
    rm -f "$target"
  fi

  cp "$src" "$target"
  chmod 755 "$target"
  INSTALLED_TARGET="$target"
  echo "installed: $target"
  echo "ensure $INSTALL_DIR is on PATH"

  if found=$(command -v fabric 2>/dev/null); then
    if [ "$found" = "$target" ]; then
      echo "fabric on PATH at $found"
    else
      echo "fabric is on PATH at $found (installed this copy at $target)"
    fi
  else
    echo "fabric is not currently on PATH; add $INSTALL_DIR to PATH"
  fi
}

installed_version() {
  [ -n "$INSTALLED_TARGET" ] || die "internal installer error: installed target is unknown"
  "$INSTALLED_TARGET" --version
}

binary_version() {
  "$1" --version
}

report_installed_version() {
  version=$(installed_version)
  echo "installed version: $version"
}

verify_binary_version() {
  bin="$1"
  tag="$2"
  expected="${tag#v}"
  version=$(binary_version "$bin")
  case "$version" in
    "$expected"|"$expected"+*)
      ;;
    *)
      die "candidate version $version does not match requested release $tag"
      ;;
  esac
}

verify_installed_version() {
  tag="$1"
  verify_binary_version "$INSTALLED_TARGET" "$tag"
  report_installed_version
}

verify_checksum() {
  archive="$1"
  checksum_file="$2"
  expected=$(sed -n '1s/[[:space:]].*//p' "$checksum_file")
  [ -n "$expected" ] || die "checksum file $checksum_file did not contain a hash"

  if command -v sha256sum >/dev/null 2>&1; then
    actual=$(sha256sum "$archive" | sed -n '1s/[[:space:]].*//p')
  elif command -v shasum >/dev/null 2>&1; then
    actual=$(shasum -a 256 "$archive" | sed -n '1s/[[:space:]].*//p')
  else
    warn "no sha256sum or shasum found; skipping checksum verification"
    return 0
  fi

  if [ "$actual" != "$expected" ]; then
    die "checksum mismatch for $archive"
  fi
}

build_from_source_dir() {
  src_dir="$1"
  expected_tag="${2:-}"
  require_cargo
  warn "building fabric from source: $src_dir"
  cargo build --release --manifest-path "$src_dir/Cargo.toml"
  if [ -n "$expected_tag" ]; then
    verify_binary_version "$src_dir/target/release/fabric" "$expected_tag"
  fi
  install_binary "$src_dir/target/release/fabric"
}

build_release_source() {
  tag="$1"
  if ! command -v git >/dev/null 2>&1; then
    die "git is required for source installs from release tags"
  fi

  tmp=$(mktemp -d)
  warn "building fabric from source for release $tag"
  git clone --depth 1 --branch "$tag" "$REPO_URL.git" "$tmp/fabric"
  build_from_source_dir "$tmp/fabric" "$tag"
  rm -rf "$tmp"
}

detect_target() {
  os=$(uname -s)
  arch=$(uname -m)
  case "$os:$arch" in
    Darwin:arm64|Darwin:aarch64) echo "aarch64-apple-darwin" ;;
    Linux:x86_64|Linux:amd64) echo "x86_64-unknown-linux-gnu" ;;
    Linux:arm64|Linux:aarch64) echo "aarch64-unknown-linux-gnu" ;;
    *) return 1 ;;
  esac
}

detect_script_source_dir() {
  case "$(basename "$0")" in
    install.sh) ;;
    *) return 1 ;;
  esac
  [ -f "$0" ] || return 1
  script_dir=$(CDPATH= cd "$(dirname "$0")" 2>/dev/null && pwd) || return 1
  [ -f "$script_dir/Cargo.toml" ] || return 1
  grep -q '^name = "fabric"' "$script_dir/Cargo.toml" || return 1
  echo "$script_dir"
}

install_prebuilt() {
  target="$1"
  tag="$2"
  tmp=$(mktemp -d)
  archive="$tmp/fabric-$target.tar.gz"
  checksum="$archive.sha256"
  url="$REPO_URL/releases/download/$tag/fabric-$target.tar.gz"

  echo "trying prebuilt release: $target ($tag)"
  if ! fetch "$url" "$archive"; then
    warn "failed to download $url"
    rm -rf "$tmp"
    return 1
  fi
  if ! fetch "$url.sha256" "$checksum"; then
    warn "failed to download $url.sha256"
    rm -rf "$tmp"
    return 1
  fi
  verify_checksum "$archive" "$checksum"
  if ! tar -xzf "$archive" -C "$tmp"; then
    warn "failed to extract $url"
    rm -rf "$tmp"
    return 1
  fi
  if [ ! -f "$tmp/fabric" ]; then
    warn "release archive $url did not contain ./fabric"
    rm -rf "$tmp"
    return 1
  fi

  verify_binary_version "$tmp/fabric" "$tag"
  install_binary "$tmp/fabric"
  rm -rf "$tmp"
  return 0
}

TARGET_TAG=$(resolve_target_tag) || die "could not resolve requested fabric release: $REQUESTED_VERSION"
echo "target release: $TARGET_TAG"

SOURCE_DIR=""
if source_dir=$(detect_script_source_dir 2>/dev/null); then
  SOURCE_DIR="$source_dir"
fi

if [ "$MODE" = "source" ]; then
  if [ -n "$SOURCE_DIR" ]; then
    warn "explicit source install from local checkout; installed version may differ from $TARGET_TAG"
    build_from_source_dir "$SOURCE_DIR"
    report_installed_version
  else
    build_release_source "$TARGET_TAG"
    verify_installed_version "$TARGET_TAG"
  fi
  exit 0
fi

if [ -n "$SOURCE_DIR" ]; then
  warn "install.sh was invoked from a fabric checkout; building local source instead of downloading a release"
  build_from_source_dir "$SOURCE_DIR"
  report_installed_version
  exit 0
fi

if target=$(detect_target); then
  if install_prebuilt "$target" "$TARGET_TAG"; then
    verify_installed_version "$TARGET_TAG"
    exit 0
  fi
  if [ "$SOURCE_FALLBACK" = "1" ]; then
    warn "prebuilt install failed; --source-fallback requested"
    build_release_source "$TARGET_TAG"
    verify_installed_version "$TARGET_TAG"
    exit 0
  fi
  die "prebuilt install failed for $target from $TARGET_TAG; not falling back to source automatically. Re-run with --source-fallback or --from-source to build from source."
fi

if [ "$SOURCE_FALLBACK" = "1" ]; then
  warn "no prebuilt target matched; --source-fallback requested"
  build_release_source "$TARGET_TAG"
  verify_installed_version "$TARGET_TAG"
  exit 0
fi

die "unsupported OS/arch for prebuilt release ($(uname -s)/$(uname -m)); re-run with --from-source to build from source."
