#!/bin/sh
# dsh TUI pane entrypoint.
set -eu
BASE="$HOME/.local/share/dsh-bridge"
[ -f "$BASE/env.sh" ] && . "$BASE/env.sh"
cd_context_cwd() { :; }
[ -n "${HERDR_PLUGIN_CONTEXT_JSON:-}" ] && python3 - <<'PY' 2>/dev/null || true
import json, os
try:
    d = json.loads(os.environ.get("HERDR_PLUGIN_CONTEXT_JSON", "{}"))
    cwd = d.get("workspace_cwd") or d.get("focused_pane_cwd")
    if cwd and os.path.isdir(cwd): os.chdir(cwd)
except Exception: pass
PY
if [ -n "${DSH_BIN:-}" ]; then exec sh -c "$DSH_BIN --profile tui"; fi
if command -v dsh >/dev/null 2>&1; then exec dsh --profile tui; fi
exec npx --yes @deepseek-ai/dsh --profile tui
