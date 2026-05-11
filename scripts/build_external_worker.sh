#!/usr/bin/env bash
set -euo pipefail

# Build the Signal benchmark worker binary for an external device target.
#
# Usage:
#   scripts/build_external_worker.sh <target-triple>
#
# Examples:
#   scripts/build_external_worker.sh armv7-unknown-linux-musleabihf   # LuckFox Pico Plus
#   scripts/build_external_worker.sh aarch64-unknown-linux-gnu        # Raspberry Pi 64-bit
#
# The resulting binary will be at:
#   target/<target-triple>/minsize/worker
#
# Requirements:
#   - Rust target installed:
#       rustup target add armv7-unknown-linux-musleabihf
#   - Cross-compilation toolchain (e.g., musl-cross, or appropriate linker)
#   - For ARMv7 musl: ensure arm-linux-musleabihf-ld is in PATH

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

TARGET="${1:-}"
if [ -z "$TARGET" ]; then
    echo "Usage: $0 <target-triple>"
    echo ""
    echo "Available targets for external devices:"
    echo "  armv7-unknown-linux-musleabihf   # LuckFox Pico Plus (ARM Cortex-A7)"
    echo "  aarch64-unknown-linux-gnu        # Raspberry Pi 64-bit"
    echo "  aarch64-unknown-linux-musl       # Raspberry Pi 64-bit (static)"
    exit 1
fi

echo "[build] Building worker for target: $TARGET"
echo "[build] Profile: minsize (optimized for size, stripped)"

# Ensure target is installed
rustup target add "$TARGET" 2>/dev/null || true

# Build with minsize profile
cargo build \
    --profile minsize \
    --target "$TARGET" \
    --bin worker

BINARY="target/$TARGET/minsize/worker"
if [ -f "$BINARY" ]; then
    echo "[build] Success: $BINARY"
    echo "[build] Size: $(ls -lh "$BINARY" | awk '{print $5}')"
    file "$BINARY"
else
    echo "[build] FAILED: binary not found at $BINARY"
    exit 1
fi
