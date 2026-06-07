## 2026-06-06 - Linked Install Force Flag

- Category: error
- Context: Installing the local Live Lens plugin with `openclaw plugins install --link --force` failed.
- Evidence: The CLI reported `--force is not supported with --link`.
- Lesson: For linked local OpenClaw plugin installs, run `openclaw plugins install --link <path>` without `--force`.
- Scope: Project-local.

## 2026-06-06 - Live Database Path

- Category: correction
- Context: Seeding the live dashboard initially assumed the plugin database was under `~/.openclaw/data`.
- Evidence: `DatabaseSync('/Users/clawdius/.openclaw/data/openclaw-live-lens.sqlite')` failed with `unable to open database file`; the live file was found at `/Users/clawdius/Projects/openclaw-live-lens/data/openclaw-live-lens.sqlite`.
- Lesson: For this linked local plugin, relative `databasePath` resolves under the linked plugin workspace, so seed or inspect `data/openclaw-live-lens.sqlite` in this repo.
- Scope: Project-local.

## 2026-06-06 - Browser Verification Variable Reuse

- Category: error
- Context: A browser verification script failed after reusing a top-level `const before` binding in the persistent browser automation session.
- Evidence: The runtime returned `Identifier 'before' has already been declared`.
- Lesson: Use fresh `var` names or unique identifiers for repeated browser verification snippets in this repo.
- Scope: Project-local.

## 2026-06-06 - Quote Query URLs In Zsh

- Category: error
- Context: A final `curl` check for a Live Lens filtered spans URL failed in zsh.
- Evidence: zsh reported `no matches found: http://127.0.0.1:18789/openclaw-lens/spans?runId=e2e-test-...`.
- Lesson: Quote local URLs containing `?` or `&` when running curl checks from zsh.
- Scope: Project-local.

## 2026-06-06 - Ripgrep Patterns Starting With Dashes

- Category: error
- Context: Searching dashboard CSS custom properties with `rg -n "--bg|--panel|..."` failed.
- Evidence: ripgrep reported `unrecognized flag --bg|--panel|...`.
- Lesson: Use `rg -n -- "pattern"` when a search pattern can begin with `-`.
- Scope: Project-local.
