# Access Gate Setup

The dashboard now has a soft access gate that requires:

1. **An email address** ending in `@target.com` or `@alvarezandmarsal.com`
2. **A shared access password**

On successful entry, the gate logs the email + timestamp + user agent
to a Google Sheet you control. Successful entries are remembered in
the visitor's browser for 7 days, so they don't see the gate again
during that window.

> **Honest caveat:** This is a soft barrier — anyone who knows to open
> DevTools can bypass it. It works to keep casual link-sharing out
> and create a viewer log, but it is **not** a real authentication
> mechanism. For real auth, put Cloudflare Access in front of the
> public URL.

## What's deployed

- `index.html` already contains the gate (HTML + CSS + JS) injected
  between `<!-- ACCESS_GATE_INJECT_BEGIN -->` and `…END -->` markers
- Default password: **`TargetOB2026`** (change this — see step 4 below)
- Allowed domains: `@target.com`, `@alvarezandmarsal.com`
- Email logging endpoint: empty (you'll fill this in below)

## One-time setup steps

### 1. Make the repo private and enable GitHub Pages

In your GitHub repo settings:

- **Settings → General → Visibility**: switch to **Private**
- **Settings → Pages → Source**: deploy from `main` branch, root folder
- **Settings → Pages → Visibility**: keep as **Public** (the default).
  This means the rendered Pages URL is publicly accessible — your
  source code stays private, but the site is reachable so your gate
  can decide who actually gets to see the dashboard

After Pages finishes building, your dashboard URL is something like:

```
https://<your-org>.github.io/<repo-name>/
```

### 2. Create the Google Sheet for viewer logs

1. Go to [sheets.new](https://sheets.new)
2. Name it something like *"Jeans Dashboard Access Log"*
3. Add a header row: `Timestamp`, `Email`, `Success`, `User Agent`, `Referrer`
4. Optionally share with your team (View access)
5. Copy the **sheet ID** from the URL — it's the long string between
   `/d/` and `/edit`. For example:
   `https://docs.google.com/spreadsheets/d/`**`1AbCDeFgHiJkLmNoPQrStUvWxYz...`**`/edit#gid=0`

### 3. Create the Apps Script Web App

1. In the Sheet, **Extensions → Apps Script**
2. Replace the default code with this:

```javascript
const SHEET_ID = 'PASTE_YOUR_SHEET_ID_HERE';

function doPost(e) {
  try {
    const data = JSON.parse(e.postData.contents);
    const sheet = SpreadsheetApp.openById(SHEET_ID).getActiveSheet();
    sheet.appendRow([
      new Date(),
      data.email || '',
      data.success ? 'YES' : 'NO',
      data.ua || '',
      data.ref || '',
    ]);
    return ContentService.createTextOutput(JSON.stringify({ ok: true }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ ok: false, error: String(err) }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}
```

3. Replace `PASTE_YOUR_SHEET_ID_HERE` with the sheet ID from step 2
4. **Deploy → New deployment**
5. Click the gear icon next to "Select type" → choose **Web app**
6. Settings:
   - Description: `Jeans dashboard access logger`
   - Execute as: **Me**
   - Who has access: **Anyone**
7. Click **Deploy**, accept the OAuth prompt
8. Copy the **Web app URL** — it looks like:
   `https://script.google.com/macros/s/AKfycbz…/exec`

### 4. Wire the URL and password into the dashboard

Open `index.html` in a text editor. Find this block (Cmd/Ctrl-F for
`ACCESS_GATE_CONFIG`):

```javascript
window.ACCESS_GATE_CONFIG = {
  allowedDomains: ['target.com', 'alvarezandmarsal.com'],
  passwordHash: '9099754fcb2d0a63d139baf5404c162406c09017426b953f0f04a9681db0bd57',
  logEndpoint: '',
  stickyDays: 7,
};
```

**Change `logEndpoint`** to the Apps Script Web App URL from step 3:

```javascript
logEndpoint: 'https://script.google.com/macros/s/AKfycbz.../exec',
```

**Change the password.** The default is `TargetOB2026`. To change:

1. Open the deployed dashboard in a browser
2. Press F12 to open DevTools, click the **Console** tab
3. Run: `await ACCESS_GATE_HASH('your-new-password-here')`
4. Copy the hex string output
5. Paste it as the new `passwordHash` value in `index.html`
6. Commit and push — visitors will need the new password (and any
   browser with a sticky 7-day pass will be invalidated since the hash
   changed)

### 5. Test it

1. Push `index.html` changes to GitHub
2. Wait ~1 minute for Pages to rebuild
3. Open the dashboard URL in an **incognito** window
4. You should see the gate. Try:
   - A `gmail.com` email → "Email must end in @target.com…" rejection
   - A `target.com` email + wrong password → "Incorrect password" rejection
   - A `target.com` email + correct password → enters the dashboard
5. Open the Google Sheet — you should see four rows logged (one per attempt)

## How visitors experience it

- First visit → see gate, enter email + password, get in, sticky for 7 days
- Subsequent visits in the same browser within 7 days → no gate, dashboard loads directly
- After 7 days OR if you change the password → re-prompted

## Maintaining the viewer log

The Google Sheet captures every attempt. Successful logins have
`Success = YES`. Failed attempts (wrong domain, wrong password) are
also logged with `Success = NO` so you can spot probes.

If you want to disable logging temporarily, just blank out
`logEndpoint` in `index.html` and redeploy. The gate keeps working;
nothing gets sent to the Sheet.

## Re-running the injector

If you ever lose track of the gate code (e.g., regenerate `index.html`
from scratch), just run:

```bash
python3 add_access_gate.py
```

It re-injects the gate cleanly between marker comments. Idempotent —
running twice doesn't double-inject.

## Removing the gate

Open `index.html`, find `<!-- ACCESS_GATE_INJECT_BEGIN -->` and delete
everything through `<!-- ACCESS_GATE_INJECT_END -->`. Done.
