#!/bin/sh
# Split the focused pane (wide -> right, tall -> down) and open the dsh TUI there.
set -eu
HERDR="${HERDR_BIN_PATH:-herdr}"
HERE="$(cd "$(dirname "$0")" && pwd)"
direction=right
if size=$("$HERDR" pane edges --pane "${HERDR_PANE_ID:-}" 2>/dev/null | python3 -c '
import json, os, sys
try:
    d = json.load(sys.stdin)
    for p in d["layout"]["panes"]:
        if p.get("pane_id") == os.environ.get("HERDR_PANE_ID"):
            print(p["rect"]["width"], p["rect"]["height"]); break
except Exception: pass' 2>/dev/null) && [ -n "$size" ]; then
  w=${size%% *}; h=${size##* }
  [ "${w:-0}" -ge "${h:-0}" ] || direction=down
fi
out=$("$HERDR" pane split --pane "${HERDR_PANE_ID:-}" --direction "$direction" --no-focus)
pane_id=$(printf '%s' "$out" | python3 -c '
import json, sys
try: print(json.load(sys.stdin)["result"]["pane"]["pane_id"])
except Exception: pass' 2>/dev/null)
[ -n "$pane_id" ] || { echo "$out" >&2; exit 1; }
"$HERDR" pane rename "$pane_id" dsh >/dev/null 2>&1 || :
exec "$HERDR" pane run "$pane_id" "\"$PWD/bin/launch-dsh.sh\""
