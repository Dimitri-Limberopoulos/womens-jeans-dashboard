#!/usr/bin/env python3
"""
add_access_gate.py — Inject an email + password splash gate into
index.html. Restricts dashboard access to viewers who:
  1. Provide an email ending in @target.com or @alvarezandmarsal.com
  2. Enter the shared access password

On success the gate logs (email, timestamp, user agent) to a Google
Sheet via a Google Apps Script Web App, then hides itself. A 7-day
sticky cookie/localStorage stamp avoids re-prompting the same browser.

This is a SOFT BARRIER — easy to bypass via DevTools. Suitable as
"don't share the link casually" gating, not real auth. For real auth,
use Cloudflare Access in front of the public URL instead.

Idempotent — bracketed by marker comments. Re-running replaces the
prior block.

Configuration: open index.html and edit the values inside
window.ACCESS_GATE_CONFIG. The default password is "TargetOB2026"
(hash 9099754f…). To set a new password:
  1. Pick a password
  2. Open the dashboard, hit F12 to open the browser console, run:
       await ACCESS_GATE_HASH('your-new-password-here')
  3. Copy the hex string output, replace the passwordHash value in
     index.html, and re-deploy
"""

import os
import re
import shutil
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
INDEX_HTML = os.path.join(HERE, "index.html")

MARKER_BEGIN = "<!-- ACCESS_GATE_INJECT_BEGIN -->"
MARKER_END = "<!-- ACCESS_GATE_INJECT_END -->"

# Default password: "Jeans2026!" — change in index.html after first deploy
DEFAULT_PASSWORD_HASH = "29c08c5d9ae5b847d18681c92be3180fe1b2256b878baa64d8d56c71c060492b"

GATE_BLOCK = '''<style id="access-gate-css">
#access-gate {
  position: fixed; inset: 0; z-index: 999999;
  background: #fafafa;
  display: flex; align-items: center; justify-content: center;
  font-family: Montserrat, system-ui, sans-serif;
}
#access-gate.hidden { display: none; }
#access-gate-card {
  background: #fff; border: 1px solid #e5e5e5; border-radius: 12px;
  padding: 36px 40px; width: 380px; max-width: calc(100vw - 32px);
  box-shadow: 0 4px 24px rgba(0,0,0,0.08);
}
#access-gate-card label {
  display: block; font-size: .68rem; font-weight: 700;
  letter-spacing: .06em; text-transform: uppercase; color: #666;
  margin-bottom: 6px;
}
#access-gate-card input {
  width: 100%; box-sizing: border-box;
  padding: 10px 12px; font-size: .85rem; font-family: inherit;
  border: 1.5px solid #d1d5db; border-radius: 6px;
  margin-bottom: 14px; transition: border-color .15s;
}
#access-gate-card input:focus {
  outline: none; border-color: #CC0000;
}
#access-gate-btn {
  width: 100%; padding: 12px; font-size: .82rem; font-weight: 700;
  letter-spacing: .04em; text-transform: uppercase;
  background: #CC0000; color: #fff; border: none; border-radius: 6px;
  cursor: pointer; transition: background .15s;
  font-family: inherit;
}
#access-gate-btn:hover { background: #a30000; }
#access-gate-btn:disabled { background: #999; cursor: not-allowed; }
#access-gate-error {
  color: #CC0000; font-size: .72rem; font-weight: 600;
  min-height: 18px; margin-bottom: 8px;
}
</style>

<div id="access-gate" role="dialog" aria-modal="true">
  <div id="access-gate-card">
    <form id="access-gate-form" autocomplete="on">
      <label for="ag-email">Work email</label>
      <input type="email" id="ag-email" required autocomplete="email"
             placeholder="you@target.com or you@alvarezandmarsal.com">
      <label for="ag-password">Access password</label>
      <input type="password" id="ag-password" required autocomplete="current-password"
             placeholder="Shared password">
      <div id="access-gate-error" aria-live="polite"></div>
      <button type="submit" id="access-gate-btn">Enter dashboard</button>
    </form>
  </div>
</div>

<script id="access-gate-js">
(function () {
  // ── Configuration — edit these values to match your deployment ──────
  window.ACCESS_GATE_CONFIG = {
    // Allowed email domains (case-insensitive). One of these must match.
    allowedDomains: ['target.com', 'alvarezandmarsal.com'],

    // SHA-256 hex hash of the access password.
    // To set a new password, run in console:  await ACCESS_GATE_HASH('your-pw')
    // and replace this string with the output.
    passwordHash: ''' + repr(DEFAULT_PASSWORD_HASH) + ''',

    // Google Apps Script Web App URL that logs the email to a sheet.
    // Leave empty to disable logging. See GATE_SETUP.md for setup steps.
    logEndpoint: '',

    // How many days the gate stays "passed" in the same browser before
    // re-prompting. Set to 0 to require auth on every page load.
    stickyDays: 7,
  };

  function $(id) { return document.getElementById(id); }
  function showError(msg) { $('access-gate-error').textContent = msg || ''; }

  async function sha256(text) {
    var data = new TextEncoder().encode(text);
    var buf = await crypto.subtle.digest('SHA-256', data);
    return Array.from(new Uint8Array(buf))
      .map(function (b) { return b.toString(16).padStart(2, '0'); })
      .join('');
  }
  // Expose helper so users can compute new password hashes from the console
  window.ACCESS_GATE_HASH = sha256;

  function isDomainAllowed(email) {
    var m = /@([^@\\s]+)$/.exec((email || '').trim().toLowerCase());
    if (!m) return false;
    var domain = m[1];
    var allowed = (window.ACCESS_GATE_CONFIG.allowedDomains || []).map(function (d) {
      return d.toLowerCase();
    });
    return allowed.indexOf(domain) >= 0;
  }

  function isStickyValid() {
    try {
      var raw = localStorage.getItem('access-gate-passed');
      if (!raw) return false;
      var data = JSON.parse(raw);
      var stickyDays = window.ACCESS_GATE_CONFIG.stickyDays || 0;
      if (stickyDays <= 0) return false;
      var ageMs = Date.now() - (data.ts || 0);
      var maxAge = stickyDays * 24 * 60 * 60 * 1000;
      if (ageMs > maxAge) return false;
      // Validate that the sticky was issued for the current password
      // (so changing the password invalidates all sticky sessions).
      return data.h === window.ACCESS_GATE_CONFIG.passwordHash;
    } catch (e) { return false; }
  }

  function setSticky(email) {
    try {
      localStorage.setItem('access-gate-passed', JSON.stringify({
        ts: Date.now(),
        email: email,
        h: window.ACCESS_GATE_CONFIG.passwordHash,
      }));
    } catch (e) {}
  }

  function logAccess(email, success) {
    var url = window.ACCESS_GATE_CONFIG.logEndpoint;
    if (!url) return;
    try {
      // Use no-cors so the Apps Script POST works without CORS preflight.
      // We don't read the response anyway.
      fetch(url, {
        method: 'POST',
        mode: 'no-cors',
        body: JSON.stringify({
          email: email || '',
          success: !!success,
          ts: new Date().toISOString(),
          ua: navigator.userAgent || '',
          ref: document.referrer || '',
        }),
      }).catch(function () {});
    } catch (e) {}
  }

  function hideGate() {
    var g = $('access-gate');
    if (g) g.classList.add('hidden');
  }

  async function onSubmit(ev) {
    ev.preventDefault();
    showError('');
    var emailInput = $('ag-email');
    var pwInput = $('ag-password');
    var btn = $('access-gate-btn');
    var email = (emailInput.value || '').trim();
    var pw = pwInput.value || '';

    if (!email) { showError('Enter your work email.'); return; }
    if (!isDomainAllowed(email)) {
      showError('Email must end in @target.com or @alvarezandmarsal.com.');
      logAccess(email, false);
      return;
    }
    if (!pw) { showError('Enter the access password.'); return; }

    btn.disabled = true;
    btn.textContent = 'Verifying…';
    try {
      var hash = await sha256(pw);
      if (hash !== window.ACCESS_GATE_CONFIG.passwordHash) {
        showError('Incorrect password.');
        logAccess(email, false);
        btn.disabled = false; btn.textContent = 'Enter dashboard';
        return;
      }
      setSticky(email);
      logAccess(email, true);
      hideGate();
    } catch (e) {
      console.error('access gate:', e);
      showError('Something went wrong. Try again.');
      btn.disabled = false; btn.textContent = 'Enter dashboard';
    }
  }

  function init() {
    var form = $('access-gate-form');
    if (!form) return;
    form.addEventListener('submit', onSubmit);
    if (isStickyValid()) hideGate();
    else { try { $('ag-email').focus(); } catch (e) {} }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else { init(); }
})();
</script>'''


def remove_existing(html):
    pat = re.compile(re.escape(MARKER_BEGIN) + r".*?" + re.escape(MARKER_END), re.DOTALL)
    return pat.sub("", html)


def patch(html):
    html = remove_existing(html)
    body_open = re.search(r"<body[^>]*>", html)
    if not body_open:
        raise RuntimeError("No <body> tag found")
    insert_at = body_open.end()
    block = "\n" + MARKER_BEGIN + "\n" + GATE_BLOCK + "\n" + MARKER_END + "\n"
    return html[:insert_at] + block + html[insert_at:]


def main():
    dry = "--dry-run" in sys.argv
    with open(INDEX_HTML, "r", encoding="utf-8") as f:
        html = f.read()
    new_html = patch(html)
    delta = len(new_html) - len(html)
    print(f"index.html: {len(html):,} -> {len(new_html):,}  ({delta:+,} chars)")
    if dry:
        print("[dry-run] no write")
        return 0
    backup = INDEX_HTML + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
    shutil.copy2(INDEX_HTML, backup)
    print(f"Backup: {backup}")
    with open(INDEX_HTML, "w", encoding="utf-8") as f:
        f.write(new_html)
    print("Done. See GATE_SETUP.md for next steps (Google Sheet logging).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
