# ActionBuilder Web-UI Automation (agent-managed)

Built 2026-08-07. The AB OSDI API cannot create sections/fields, saved queries,
or tasks, and cannot delete emails — that work only happens in the web UI at
`https://commoncause.actionbuilder.org`. This tooling lets agents do UI work
directly: a persistent authenticated browser session plus an interactive
browser MCP. It mirrors the `ptv-tools` pattern (PTV admin automation), which
proved out the design: one headed human login, storage state persisted outside
the repo, everything afterward runs headless.

Known UI-only work this exists for: the tag taxonomy redesign
(creating/archiving sections and fields across 24 campaigns). Completed with
this tooling 2026-08-07: field activation for VA/DC (the silent tag-write
drop), network-wide Soapboxx Stories activation, and the Kelly Dufour
wrong-email fix.

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

## GraphQL replay (learned 2026-08-07)

The AB web UI is an Angular app over `POST /api/graphql`. For bulk repeats of a
flow you've done once, replaying the captured mutation beats UI clicking:

1. Do the action once in the UI; pull the mutation from
   `browser_network_requests` / `browser_network_request` (request-body).
2. Replay it via in-page `fetch` (`browser_run_code_unsafe` →
   `page.evaluate`). **Cookies alone are NOT enough for mutations** — the app
   uses devise-token-auth; send these headers, read from localStorage:
   `access-token` (key `accessToken`), `client`, `uid`, `token-type`.
   Persist any rotated `access-token` response header back to localStorage
   before the next call. Queries parse without them; mutations 200 with a
   `"User is not logged in."` GraphQL error if they're missing.
3. Verify from the mutation's own response selection (e.g.
   `isInCampaign(campaignId)`), not the 200.

Known mutations: `AssociateTagToCampaign(input: {campaignId, tagId})` —
activates one tag value in one campaign (what the Customize → Info → Edit Info
checkboxes fire); `addTagCategoryToCampaign(input: {campaignId, tagCategoryId})`
(inverse `removeTagCategoryFromCampaign`) — enables a FIELD in a campaign,
which is what gates profile rendering + search-picker visibility, **including
for universal fields** (learned 2026-08-13: ⚠️ the field-level checkbox in
Customize → Info is INERT for universal fields — toggles visually, fires no
mutation, doesn't persist; use the mutation). GraphQL introspection is enabled
(`{ __schema { mutationType { fields { name } } } }`) — use it to find more.
`UpdateEmail(input: {entityId, campaignId, emailId,
changes: {email, emailType, status, subscribe}})` — edits a contact email
(there is NO delete-email affordance anywhere, UI or GraphQL menu — only
address rewrite and status flags).

**Campaign-scoped authorization:** entity mutations require the entity to be a
member of the `campaignId` you pass ("This entity is not accessible…").
For campaign-less entities (post-dedup residue), use the temporary loop:
`update_entity_with_tags(campaign_uuid, entity_id, [])` to connect → GraphQL
edit → `delete_person(campaign_uuid, entity_id)` to remove → log both ops to
sync_log (`connect_entity` / `remove_from_campaign`) so the removal-gap
overlay stays correct.

## Field/response activation model (learned 2026-08-07)

Sections (tag_groups), fields (tag_categories), and values (tags) are
network-level objects; campaigns **activate** them (junction tables
`campaigns_tag_groups` / `campaigns_tag_categories` / `campaigns_tags`).
Activation is per tag VALUE; checking a response in Customize → Info → Edit
Info fires one `AssociateTagToCampaign` per value and cascades the field/
section on. A signup-helper write to a value not activated in the target
campaign returns 200 and silently drops — the VA/DC and Soapboxx incidents.
The `phantom_tag_writes` dbt model detects this class network-wide.

Universal-field refinement (learned 2026-08-13 during taxonomy experiments):
universal WRITES always land (even with zero enablement anywhere), but each
campaign still needs the FIELD enabled (`campaigns_tag_categories` /
GraphQL `isInCampaign`) for the field to render on profiles and appear in the
search picker. Universal-field VALUES auto-enable wherever their field is
enabled (their response checkboxes render disabled-checked). A new universal
field starts enabled in ZERO campaigns; new campaigns start with zero
enablement rows even for pre-existing universal sections. Enable via
`addTagCategoryToCampaign` (the UI checkbox is inert for universal fields —
see Known mutations above). So universal fields fail VISIBLE-side
(data lands, UI hides it) where campaign-local fields fail WRITE-side
(200 + silent drop). Audit query: LEFT JOIN universal tag_categories against
campaigns_tag_categories and look for missing campaigns.

## Safety and PII

- **UI actions are live production edits.** Same discipline as sync ops:
  read-only exploration is fine; bulk or destructive UI changes get Rob's
  sign-off first (take a cut → he grades → execute).
- Screenshots, traces, and MCP output land under `~/.ab-ui/` — **never inside
  the repo** (AB pages are row-level PII; the repo is org-visible). The MCP is
  registered with `--output-dir C:\Users\RobKerth\.ab-ui\mcp-output`.
- ⚠️ **`--output-dir` does NOT apply to explicit `filename` arguments** on
  `browser_snapshot` / `browser_take_screenshot`: a relative filename resolves
  against the MCP server's cwd — the repo root. Always pass an absolute path
  under `~/.ab-ui/mcp-output`, or omit `filename` (auto-saved files do land in
  the output dir). Grep the auto-saved `page-*.yml` for element refs instead of
  re-snapshotting into context.
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
