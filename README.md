# herdr-dsh-plugin

DeepSeek Harness (dsh) in [Herdr](https://herdr.dev): TUI panes + a delegation
bridge. Any CLI agent in Herdr (pi / Codex / Claude / …) delegates tasks to a
`dsh --profile headless` executor through `dshcli`, using the same
receipt/evidence disk protocol as herdr-zcode-plugin.

## Install

```bash
herdr plugin install Nofuture123/herdr-dsh-plugin
```

Build-time prereqs: Node.js >= 20, python3, a resolvable `dsh`
(`npm i -g @deepseek-ai/dsh`, or npx on demand). No running dsh UI is
required at delegation time — the executor spawns one headless process per
task. DeepSeek login is required (credentials in `~/.dsh`).

## Use (agents: prefer flags — no JSON quoting traps)

```bash
dshcli send "add a test for X" --mode edit \
  --verify "python3 -m unittest discover -p test_*.py" \
  --scope test_tetris.py --key k1 --timeout 600

dshcli result --machine      # terminal result from disk evidence
dshcli list                  # task history from the evidence store
```

- Plain text = freeform task in the caller's cwd (`--workspace` to override).
- `--verify` is required for `--mode edit|yolo` (fail-closed) and runs after
  the task; success gates the task status.
- Avoid `$(...)` inside `--verify` — your own shell may expand it before the
  executor sees it. Use self-contained commands (`grep -qx 6 sum.txt`).
- JSON envelope is also accepted for full control:
  `{"goal":"...","workspace":"/abs","mode":"edit","verify":"...","scope":[...],
  "timeout":600,"idempotency_key":"k1","nonce":"..."}`

The executor pane streams dsh reasoning live (dim `·` lines) and shows the
final answer in an accent `┃ answer` block after the completion rule.

## Where records live

- Task evidence: `~/.local/share/dsh-bridge/results/<task_id>.json`
  (status, summary, verify, changed_files)
- Reasoning streams: `~/.local/share/dsh-bridge/logs/<task_id>/stream.log`
- Native dsh sessions: `~/.dsh/sessions/<workspace-bucket>/` (project-grouped);
  `dshcli open-session <task_id>` reopens the conversation in the dsh TUI.

## Uninstall

`herdr plugin uninstall dsh`, then `rm -rf ~/.local/share/dsh-bridge` if
desired.

## License

MIT
