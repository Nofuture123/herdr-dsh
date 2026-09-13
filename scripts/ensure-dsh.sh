#!/bin/sh
# Prereq bootstrap: Node + a resolvable dsh CLI. Idempotent, fails loudly.
set -u
BASE="$HOME/.local/share/dsh-bridge"
mkdir -p "$BASE"
fail() { echo "✗ [dsh] $1" >&2; exit 1; }
command -v node >/dev/null 2>&1 || fail "Node.js >= 20 required (brew install node)"
command -v python3 >/dev/null 2>&1 || fail "python3 required"
if [ -z "${DSH_BIN:-}" ] && ! command -v dsh >/dev/null 2>&1; then
  command -v npx >/dev/null 2>&1 || fail "dsh not found and npx unavailable (npm i -g @deepseek-ai/dsh)"
  npx --yes @deepseek-ai/dsh --version >/dev/null 2>&1 || fail "npx @deepseek-ai/dsh failed"
fi
mkdir -p "$BASE/logs" && chmod 700 "$BASE/logs" 2>/dev/null || true
NODE_BIN="$(command -v node)"; PY3="$(command -v python3)"
cat > "$BASE/env.sh" <<EOF
export PATH="$(dirname "$NODE_BIN"):/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:\$PATH"
export NODE_BIN="$NODE_BIN"
export PY3="$PY3"
EOF
echo "✓ [dsh] bootstrap ok"
