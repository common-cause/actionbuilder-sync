# ActionBuilder Web-UI Automation (agent-managed)

Built 2026-08-07. The AB OSDI API cannot create sections/fields, saved queries,
or tasks, and cannot delete emails — that work only happens in the web UI at
`https://commoncause.actionbuilder.org`. This tooling lets agents do UI work
directly: a persistent authenticated browser session plus an interactive
browser MCP. It mirrors the `ptv-tools` pattern (PTV admin automation), which
proved out the design: one headed human login, storage state persisted outside
the repo, everything afterward runs headless.

Known UI-only work this exists for: campaign-local sections for VA/DC
(campaigns 24/25 — the silent tag-write drop), the tag taxonomy redesign
(creating/archiving sections and fields across 24 campaigns), and removing the
wrongly-attached staff email from the Kelly Dufour entities.

## Components

1. **Session layer** — `scripts/ab_ui_session.py`
   Owns login and the storage-state file. Requires `playwright` +
   `python-dotenv` in the project venv (installed 2026-08-07).

2. **`ab-ui` MCP server** — Playwright MCP (`@playwright/mcp@0.0.79`),
   registered project-locally in Rob's `~/.claude.json`. Gives agents
   interactive browser tools (`browser_navigate`, `browser_snapshot`,
   `browser_click`, `browser_type`, `browser_take_screenshot`, ...) against
   the authenticated UI. Available in sessions started after registration.

Both drive the **system Edge browser** (Playwright `msedge` channel — no
browser download) and share one auth artifact:
`~/.ab-ui/storage_state.json`.

## Session lifecycle

```bash
# Log in if needed — scripted + headless (ACTION_BUILDER_WEB_PW in .env):
.venv/Scripts/python.exe scripts/ab_ui_session.py login

# Agents: check liveness before UI work (headless; exit 0 live / 1 dead):
.venv/Scripts/python.exe scripts/ab_ui_session.py check

# Discard state and start over (add --manual for a headed human login):
.venv/Scripts/python.exe scripts/ab_ui_session.py force-relogin
```

With `ACTION_BUILDER_WEB_PW` set (it is, in `.env`), `login` fills the
`/login` form itself, verifies the session (poll stability + a fresh
navigation), and saves cookies + localStorage — ~15s, fully autonomous, no
2FA on this account (verified 2026-08-07). Without the password — or with
`--manual` — it opens a headed Edge window and waits up to 10 minutes for a
human to complete login.

Config (env or `.env`): `ACTION_BUILDER_WEB_PW` (enables scripted login),
`ACTION_BUILDER_WEB_USER` (default `rkerth@commoncause.org`),
`AB_UI_STORAGE_STATE` (default `~/.ab-ui/storage_state.json`),
`AB_UI_BROWSER_CHANNEL` (default `msedge`).

## Agent workflow

1. `check` the session (or just navigate with the MCP — landing on
   `/login` / a password field means the session is dead).
2. Drive the UI with `ab-ui` MCP tools. Prefer `browser_snapshot`
   (accessibility tree) over screenshots for reading pages.
3. Dead session → run `login` (scripted, headless, no human needed).

Useful URL patterns (internal numeric ids, not interact_ids):

- Entity profile: `/entity/view/{entity_id}/profile?campaignId={campaign_id}&clientQueryId=null`
- Wallchart: `/entity/list?campaignId={id}&clientQueryId=null`
- Filter editor: `/campaign/customize/filters/edit?campaignId={id}&queryId={id}`

## Scripted bulk operations

Once a flow is learned interactively, encode repeatable operations (e.g. "create
section X in campaign Y" × 24) as scripts sharing the same session:

```python
from playwright.sync_api import sync_playwright
from ab_ui_session import open_authenticated_context   # sibling import

with sync_playwright() as pw:
    browser, context = open_authenticated_context(pw, headless=True)
    page = context.new_page()
    page.goto("https://commoncause.actionbuilder.org/...")
    ...
```

## Safety and PII

- **UI actions are live production edits.** Same discipline as sync ops:
  read-only exploration is fine; bulk or destructive UI changes get Rob's
  sign-off first (take a cut → he grades → execute).
- Screenshots, traces, and MCP output land under `~/.ab-ui/` — **never inside
  the repo** (AB pages are row-level PII; the repo is org-visible). The MCP is
  registered with `--output-dir C:\Users\RobKerth\.ab-ui\mcp-output`.
- The storage-state file is a credential. It stays in `~/.ab-ui/`, outside the
  repo and OneDrive.

## Caveats

- **Session TTL is unknown.** The MCP runs `--isolated`: it loads the
  storage-state file at browser launch but never writes back rotated cookies,
  so if AB rotates sessions aggressively, re-logins will be needed more often.
  Low-stakes now that `login` is scripted and headless — agents self-recover.
- If the storage-state file doesn't exist yet, the MCP's first browser tool
  call errors — run `login` first.
- Login detection is signal-based (`_login_state`), learned the hard way on
  2026-08-07: positive markers (`/select-campaign`, `/entity`, `campaignId=`,
  a visible "Sign Out") are checked before negative heuristics, a single
  positive poll is never trusted (transient mid-navigation states look
  logged-in), and a page that looks logged in is never discarded on timeout.

## MCP registration (for re-creating / upgrading)

```
claude mcp add ab-ui --scope local -- cmd /c npx -y "@playwright/mcp@0.0.79" \
  --browser msedge --isolated \
  --storage-state "C:\Users\RobKerth\.ab-ui\storage_state.json" \
  --output-dir "C:\Users\RobKerth\.ab-ui\mcp-output"
```

Version is pinned; bump deliberately. `--scope local` = this project only,
Rob's machine only (paths are user-specific). On Windows, stdio MCP servers
need the `cmd /c npx` wrapper.
