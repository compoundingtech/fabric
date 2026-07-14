#!/usr/bin/env sh
set -eu

INSTALL_DIR="${FABRIC_INSTALL_DIR:-$HOME/.local/bin}"
SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

echo "fabric: experimental prototype installer"

if [ ! -f "$SCRIPT_DIR/Cargo.toml" ]; then
  echo "error: install.sh must be run from a cloned fabric repository" >&2
  exit 1
fi

if ! command -v cargo >/dev/null 2>&1; then
  echo "error: cargo is required to install from a clone" >&2
  exit 1
fi

cargo build --release --manifest-path "$SCRIPT_DIR/Cargo.toml"

mkdir -p "$INSTALL_DIR"
cp "$SCRIPT_DIR/target/release/fabric" "$INSTALL_DIR/fabric"
chmod 755 "$INSTALL_DIR/fabric"

echo "installed: $INSTALL_DIR/fabric"
echo "ensure $INSTALL_DIR is on PATH"
