"""
ActionBuilder web-UI browser session management.

The AB OSDI API cannot create sections/fields, saved queries, or tasks, and
cannot delete emails — that work only happens in the web UI. This module owns
getting and keeping an authenticated browser session so agents (and scripts)
can do that work. Same pattern as the ptv-tools package: one headed login by
a human, storage state persisted outside the repo, everything after that can
run headless.

Login flow:
    Scripted (default when ACTION_BUILDER_WEB_PW is set in .env): fill the
    /login form headlessly, poll until the session looks live, save cookies +
    localStorage to the storage-state path. Fully autonomous — agents can
    recover a dead session without a human.

    Manual (no stored password, or --manual): launch a headed browser at
    commoncause.actionbuilder.org, a human completes login in the window,
    then poll + save as above.

Consumers:
    - The `ab-ui` Playwright MCP server loads the same storage-state file
      (registered with --isolated --storage-state), giving agents interactive
      browser tools against the authenticated UI.
    - Bulk operation scripts import open_authenticated_context() from here.

Configuration (env vars, or .env in the project root):
    ACTION_BUILDER_WEB_PW    web-UI password — enables scripted login
    ACTION_BUILDER_WEB_USER  web-UI username (default rkerth@commoncause.org)
    AB_UI_STORAGE_STATE      storage-state JSON path
                             (default ~/.ab-ui/storage_state.json — machine-wide,
                             one login serves every consumer)
    AB_UI_BROWSER_CHANNEL    Playwright browser channel (default "msedge" —
                             uses system Edge, no browser download; set to
                             "chrome" or "" for bundled chromium)

CLI (run with the project venv python):
    python scripts/ab_ui_session.py login           # log in if needed (scripted+headless
                                                    # when ACTION_BUILDER_WEB_PW is set,
                                                    # else headed manual), saves state
    python scripts/ab_ui_session.py login --manual  # force headed manual login
    python scripts/ab_ui_session.py check           # headless: is the saved session live?
    python scripts/ab_ui_session.py force-relogin   # discard state, log in fresh

Exit codes: 0 = session live / login saved, 1 = no or dead session / failure.
Storage state and screenshots live under ~/.ab-ui/ — never inside the repo
(screenshots of AB pages are PII; the repo is org-visible).
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import (
    Browser,
    BrowserContext,
    Page,
    Playwright,
    TimeoutError as PlaywrightTimeoutError,
    sync_playwright,
)

AB_HOST = "commoncause.actionbuilder.org"
AB_HOME = f"https://{AB_HOST}/"
DEFAULT_WEB_USER = "rkerth@commoncause.org"

_DOTENV_LOADED = False


def _ensure_dotenv() -> None:
    global _DOTENV_LOADED
    if not _DOTENV_LOADED:
        # The project's .env; load_dotenv never overrides real env vars.
        load_dotenv(Path(__file__).resolve().parent.parent / ".env")
        _DOTENV_LOADED = True


def storage_state_path() -> Path:
    _ensure_dotenv()
    override = os.getenv("AB_UI_STORAGE_STATE")
    if override:
        return Path(override).expanduser().resolve()
    return Path.home() / ".ab-ui" / "storage_state.json"


def browser_channel() -> str | None:
    _ensure_dotenv()
    channel = os.getenv("AB_UI_BROWSER_CHANNEL", "msedge").strip()
    return channel or None


def screenshot_dir() -> Path:
    return storage_state_path().parent / "screenshots"


def web_credentials() -> tuple[str, str] | None:
    """(username, password) if the web password is configured, else None."""
    _ensure_dotenv()
    password = (os.getenv("ACTION_BUILDER_WEB_PW") or "").strip()
    if not password:
        return None
    user = (os.getenv("ACTION_BUILDER_WEB_USER") or DEFAULT_WEB_USER).strip()
    return user, password


def _launch(pw: Playwright, headless: bool) -> Browser:
    return pw.chromium.launch(headless=headless, channel=browser_channel())


def _screenshot(page: Page, label: str) -> None:
    d = screenshot_dir()
    d.mkdir(parents=True, exist_ok=True)
    ts = time.strftime("%Y%m%d-%H%M%S")
    path = d / f"{ts}-ab-{label}.png"
    try:
        page.screenshot(path=str(path), full_page=True)
        print(f"  [screenshot] {path}")
    except Exception as e:  # screenshot failure is never fatal
        print(f"  [screenshot failed] {e}")


def _login_state(page: Page) -> tuple[bool, str]:
    """(logged_in, reason). Positive signals (known logged-in-only routes,
    the sidebar's Sign Out link) are checked before the negative heuristics,
    so an unanticipated page can't false-negative a real session."""
    url = page.url
    if not url.startswith("http") or AB_HOST not in url:
        return False, f"off-host url={url}"
    lowered = url.lower()
    if any(s in lowered for s in ("select-campaign", "/entity", "campaignid=")):
        return True, f"logged-in route url={url}"
    try:
        if page.get_by_text("Sign Out", exact=True).first.is_visible():
            return True, f"'Sign Out' visible url={url}"
    except Exception:
        pass
    if any(s in lowered for s in ("login", "sign_in", "signin", "sessions/new")):
        return False, f"login route url={url}"
    try:
        if page.locator("input[type='password']").first.is_visible():
            return False, f"password field visible url={url}"
    except Exception:
        pass
    return True, f"no logged-out signals url={url}"


def _is_logged_in(page: Page) -> bool:
    return _login_state(page)[0]


def _fill_login_form(page: Page, user: str, password: str) -> None:
    """Fill and submit the /login form (plain email + password + Login)."""
    pw_input = page.locator("input[type='password']").first
    try:
        pw_input.wait_for(state="visible", timeout=15000)
    except PlaywrightTimeoutError:
        print("-> No login form appeared (already logged in?)", flush=True)
        return
    email_input = page.locator(
        "input[type='email'], input[name*='email' i], input[name*='user' i]"
    ).first
    email_input.fill(user)
    pw_input.fill(password)
    page.get_by_role("button", name="Login").first.click()
    print(f"-> Submitted login form as {user}", flush=True)


def login(
    timeout_seconds: int | None = None,
    headless: bool | None = None,
    manual: bool = False,
) -> None:
    """Log in and persist storage state. Scripted (headless-capable) when
    ACTION_BUILDER_WEB_PW is configured and manual isn't forced; otherwise
    headed, with a human completing auth in the opened window.

    Mid-login navigations can transiently look logged-in (URL without
    'login', password field not yet rendered), so a single positive poll
    is not trusted: the signal must hold for several consecutive polls,
    and then survive a fresh navigation to AB_HOME, before state is saved.
    (The first version trusted one poll and saved pre-auth cookies.)"""
    creds = None if manual else web_credentials()
    scripted = creds is not None
    if headless is None:
        headless = scripted
    if timeout_seconds is None:
        timeout_seconds = 90 if scripted else 600
    state_path = storage_state_path()
    state_path.parent.mkdir(parents=True, exist_ok=True)
    with sync_playwright() as pw:
        browser = _launch(pw, headless=headless)
        context = browser.new_context()
        page = context.new_page()
        try:
            print(f"-> Opening {AB_HOME}", flush=True)
            page.goto(AB_HOME, wait_until="domcontentloaded")
            if scripted:
                _fill_login_form(page, *creds)
                print(
                    f"-> Waiting up to {timeout_seconds}s for a live session...",
                    flush=True,
                )
            else:
                print(
                    "-> Complete the ActionBuilder login in the browser window.\n"
                    f"   Waiting up to {timeout_seconds}s for a live session...",
                    flush=True,
                )
            deadline = time.time() + timeout_seconds
            stable = 0
            last_reason = None
            while time.time() < deadline:
                ok, reason = _login_state(page)
                if reason != last_reason:
                    print(f"   [poll] logged_in={ok} — {reason}", flush=True)
                    last_reason = reason
                if ok:
                    stable += 1
                    if stable >= 3:
                        break
                else:
                    stable = 0
                time.sleep(2)
            else:
                # One last look before giving up — and never discard a page
                # that looks logged in (attempt #2 timed out holding a live
                # session on /select-campaign and threw it away).
                ok, reason = _login_state(page)
                if not ok:
                    _screenshot(page, "login-timeout")
                    hint = (
                        " Scripted login failed — bad password, a changed "
                        "form, or a 2FA step; try `login --manual`."
                        if scripted
                        else ""
                    )
                    raise RuntimeError(
                        f"Timed out waiting for login ({reason}).{hint}"
                    )
                print(f"   [poll] logged_in=True at deadline — {reason}", flush=True)
            # Save first, then re-verify with a fresh navigation; `check`
            # is the final arbiter if anything here goes sideways.
            context.storage_state(path=str(state_path))
            print(f"-> Login signal stable (url={page.url}); re-verifying...", flush=True)
            page.goto(AB_HOME, wait_until="domcontentloaded")
            time.sleep(2)
            ok, reason = _login_state(page)
            if not ok:
                _screenshot(page, "login-reverify-failed")
                state_path.unlink(missing_ok=True)
                raise RuntimeError(
                    "Login looked complete but a fresh navigation bounced "
                    f"back to login ({reason}). State NOT saved."
                )
            print(f"-> Login verified ({reason})")
            print(f"-> Saved storage state to {state_path}")
        finally:
            context.close()
            browser.close()


def verify_session(headless: bool = True) -> bool:
    state_path = storage_state_path()
    if not state_path.exists():
        print(f"-> No saved AB session at {state_path}")
        return False
    with sync_playwright() as pw:
        browser = _launch(pw, headless=headless)
        context = browser.new_context(storage_state=str(state_path))
        page = context.new_page()
        try:
            page.goto(AB_HOME, wait_until="domcontentloaded")
            time.sleep(2)
            ok, reason = _login_state(page)
            print(f"-> AB session: {'LIVE' if ok else 'DEAD'} ({reason})")
            return ok
        finally:
            context.close()
            browser.close()


def ensure_session(manual: bool = False) -> None:
    if verify_session():
        print("-> Existing AB session is valid; nothing to do.")
        return
    print("-> No valid saved AB session; logging in.")
    login(manual=manual)


def open_authenticated_context(
    pw: Playwright, headless: bool = True
) -> tuple[Browser, BrowserContext]:
    """For operation scripts: launch a browser with the saved auth state."""
    state_path = storage_state_path()
    if not state_path.exists():
        raise RuntimeError(
            f"No saved AB session at {state_path}. "
            "Run `python scripts/ab_ui_session.py login` first."
        )
    browser = _launch(pw, headless=headless)
    context = browser.new_context(storage_state=str(state_path))
    return browser, context


def main() -> int:
    ap = argparse.ArgumentParser(description="ActionBuilder UI session management")
    ap.add_argument(
        "command",
        choices=["login", "check", "force-relogin"],
        help="login: log in if needed (scripted+headless when "
        "ACTION_BUILDER_WEB_PW is set, else headed manual); "
        "check: headless liveness check; "
        "force-relogin: discard saved state and log in fresh",
    )
    ap.add_argument(
        "--manual",
        action="store_true",
        help="Force headed manual login even if a web password is configured.",
    )
    args = ap.parse_args()

    if args.command == "check":
        return 0 if verify_session() else 1

    if args.command == "force-relogin":
        state_path = storage_state_path()
        if state_path.exists():
            state_path.unlink()
            print(f"-> Removed {state_path}")
        login(manual=args.manual)
        return 0

    ensure_session(manual=args.manual)
    return 0


if __name__ == "__main__":
    sys.exit(main())
