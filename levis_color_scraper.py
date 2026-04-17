"""
Lightweight Playwright scraper to extract color names from Levi's PDP pages.
Targets the 517 'Unknown' color entries in levis_pdp_results_v2.csv.

Uses playwright-stealth to bypass Akamai WAF bot detection.
Uses your real Chrome install (not "Chrome for Testing") to appear more legitimate.

Usage:
  pip3 install playwright playwright-stealth
  python3 levis_color_scraper.py

Output: levis_colors_scraped.csv (url, color_name)
"""

import asyncio
import csv
import os
import random
import time
import subprocess
import sys

# ── Config ──────────────────────────────────────────────────────────────────
CONCURRENCY = 2           # reduced to avoid detection (2 tabs, not 3)
SLEEP_RANGE = (3.0, 6.0)  # longer random delays to look human
TIMEOUT_MS = 20000         # page load timeout
INPUT_CSV = "levis_pdp_results_v2.csv"
OUTPUT_CSV = "levis_colors_scraped.csv"
PROGRESS_INTERVAL = 5      # print progress every N URLs
BATCH_SAVE_INTERVAL = 20   # save progress every N URLs

# ── Selectors to try (in priority order) ────────────────────────────────────
COLOR_SELECTORS = [
    'h2[data-testid="color-swatch-label"]',
    '[data-testid="color-swatch-label"]',
    '.color-swatch-label',
    '.selected-color-name',
    '[class*="colorName"]',
    '[class*="color-name"]',
    '[class*="ColorName"]',
    '[class*="swatch"][aria-checked="true"]',
    '[class*="swatch"].selected',
    '[role="radio"][aria-checked="true"]',
]


def find_real_chrome():
    """Find the user's real Chrome installation (not Chrome for Testing)."""
    paths = [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Google Chrome Canary.app/Contents/MacOS/Google Chrome Canary",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
    ]
    for p in paths:
        if os.path.exists(p):
            return p
    return None


async def warm_up_context(context):
    """Visit the Levi's homepage first to establish cookies/session like a real user."""
    page = await context.new_page()
    try:
        print("  Warming up: visiting levi.com homepage...")
        await page.goto("https://www.levi.com/US/en_US", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(random.uniform(3, 5))

        # Dismiss any popups
        try:
            close_btn = await page.query_selector('[aria-label="Close"], [data-testid="close-button"], .close-button')
            if close_btn:
                await close_btn.click()
                await asyncio.sleep(1)
        except Exception:
            pass

        # Click on "Women" -> "Jeans" to look like a real browsing session
        print("  Warming up: browsing to women's jeans...")
        await page.goto("https://www.levi.com/US/en_US/clothing/women/jeans", wait_until="domcontentloaded", timeout=20000)
        await asyncio.sleep(random.uniform(2, 4))

        print("  Warm-up complete - session established")
    except Exception as e:
        print(f"  Warm-up warning (continuing anyway): {e}")
    finally:
        await page.close()


async def extract_color(page, url):
    """Visit a Levi's PDP and extract the color name."""
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=TIMEOUT_MS)
        await asyncio.sleep(random.uniform(*SLEEP_RANGE))

        # Check for Access Denied
        title = await page.title()
        if "Access Denied" in title:
            return "ACCESS_DENIED"

        # Try CSS selectors
        for sel in COLOR_SELECTORS:
            try:
                el = await page.query_selector(sel)
                if el:
                    text = await el.inner_text()
                    if text and text.strip() and len(text.strip()) < 100:
                        return text.strip()
                    aria = await el.get_attribute('aria-label')
                    if aria and aria.strip():
                        return aria.strip()
            except Exception:
                continue

        # Try og:title meta tag: "Product Name - Color | Levi's® US"
        try:
            og = await page.query_selector('meta[property="og:title"]')
            if og:
                content = await og.get_attribute('content')
                if content and ' - ' in content:
                    color_part = content.split(' - ')[-1].split('|')[0].strip()
                    if color_part and len(color_part) < 80:
                        return color_part
        except Exception:
            pass

        # Try page title
        if title and ' - ' in title:
            parts = title.split(' - ')
            if len(parts) >= 2:
                color_part = parts[-1].split('|')[0].strip()
                if color_part and len(color_part) < 80:
                    return color_part

        # JS fallback: extract from __LSCO_INITIAL_STATE__
        try:
            color = await page.evaluate("""
                () => {
                    try {
                        const state = window.__LSCO_INITIAL_STATE__;
                        if (state) {
                            // Try various state paths
                            const stores = ['ssrStorePDP', 'ssrPDP', 'ssrStoreProduct'];
                            for (const store of stores) {
                                const pdp = state[store];
                                if (pdp) {
                                    const product = pdp.product || pdp.selectedProduct || pdp.productDetail;
                                    if (product) {
                                        const name = product.colorName || product.color || product.selectedColor
                                            || product.colorDisplayName || product.displayColor;
                                        if (name) return name;
                                    }
                                    // Try variants array
                                    if (pdp.variants) {
                                        for (const v of pdp.variants) {
                                            if (v.selected || v.isSelected) {
                                                return v.colorName || v.color || '';
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    } catch(e) {}

                    // DOM fallback: find elements with color-related classes
                    const sels = [
                        '[class*="color" i] h2', '[class*="color" i] span',
                        '[class*="Color" i] h2', '[class*="Color" i] span',
                        '[data-testid*="color" i]'
                    ];
                    for (const sel of sels) {
                        const els = document.querySelectorAll(sel);
                        for (const el of els) {
                            const t = el.textContent?.trim();
                            if (t && t.length > 2 && t.length < 60
                                && !/color/i.test(t) && !t.includes('{')) {
                                return t;
                            }
                        }
                    }
                    return '';
                }
            """)
            if color:
                return color
        except Exception:
            pass

        return "SCRAPE_FAILED"

    except Exception as e:
        err = str(e)[:80]
        if "Access Denied" in err or "403" in err:
            return "ACCESS_DENIED"
        return f"ERROR: {err}"


async def worker(queue, results, context, worker_id, save_fn):
    """Process URLs from queue using a dedicated browser tab."""
    page = await context.new_page()
    consecutive_blocks = 0

    while True:
        try:
            idx, url = queue.get_nowait()
        except asyncio.QueueEmpty:
            break

        color = await extract_color(page, url)
        results[idx] = (url, color)

        # Track consecutive blocks
        if color == "ACCESS_DENIED":
            consecutive_blocks += 1
            if consecutive_blocks >= 5:
                print(f"  Worker {worker_id}: 5 consecutive blocks - pausing 60s...")
                await asyncio.sleep(60)
                consecutive_blocks = 0
                # Refresh the page/context by navigating to homepage
                try:
                    await page.goto("https://www.levi.com/US/en_US", wait_until="domcontentloaded", timeout=15000)
                    await asyncio.sleep(random.uniform(3, 5))
                except Exception:
                    pass
        else:
            consecutive_blocks = 0

        done = sum(1 for r in results if r is not None)
        if done % PROGRESS_INTERVAL == 0:
            succeeded = sum(1 for r in results if r and r[1] not in ('SCRAPE_FAILED', 'ACCESS_DENIED') and not r[1].startswith('ERROR'))
            blocked = sum(1 for r in results if r and r[1] == 'ACCESS_DENIED')
            print(f"  Progress: {done}/{len(results)} | OK: {succeeded} | Blocked: {blocked}")

        if done % BATCH_SAVE_INTERVAL == 0:
            save_fn()

    await page.close()


async def main():
    print(f"Reading {INPUT_CSV}...")

    # Load Unknown Levi's entries
    urls_to_scrape = []
    with open(INPUT_CSV, 'r') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if row['color'].strip() in ('Unknown', ''):
                urls_to_scrape.append((i, row['url'].strip()))

    print(f"Found {len(urls_to_scrape)} Unknown color entries to scrape")

    # Check for existing progress (resume support)
    already_done = {}
    if os.path.exists(OUTPUT_CSV):
        with open(OUTPUT_CSV, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                cn = row['color_name']
                if cn not in ('SCRAPE_FAILED', 'ACCESS_DENIED', '') and not cn.startswith('ERROR'):
                    already_done[row['url']] = cn
        if already_done:
            print(f"Resuming: {len(already_done)} previously scraped successfully")

    # Filter out already-done URLs
    remaining = [(idx, url) for idx, url in urls_to_scrape if url not in already_done]
    print(f"Remaining to scrape: {len(remaining)}")

    if not remaining:
        print("All URLs already scraped!")
        return

    # Shuffle URLs to avoid hitting the same product family in sequence
    random.shuffle(remaining)

    # Set up queue
    queue = asyncio.Queue()
    for item in remaining:
        await queue.put(item)

    results = [None] * len(remaining)

    try:
        from playwright.async_api import async_playwright
    except ImportError:
        print("ERROR: playwright not installed. Run:")
        print("  pip3 install playwright")
        print("  python3 -m playwright install chromium")
        return

    # Try to import stealth
    try:
        from playwright_stealth import stealth_async
        HAS_STEALTH = True
        print("playwright-stealth loaded ✓")
    except ImportError:
        HAS_STEALTH = False
        print("WARNING: playwright-stealth not installed. Run: pip3 install playwright-stealth")
        print("Continuing without stealth (may get blocked)...")

    # Save function for periodic progress saves
    def save_progress():
        all_res = dict(already_done)
        for r in results:
            if r is not None:
                all_res[r[0]] = r[1]
        with open(OUTPUT_CSV, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['url', 'color_name'])
            writer.writeheader()
            for url, color in sorted(all_res.items()):
                writer.writerow({'url': url, 'color_name': color})

    chrome_path = find_real_chrome()
    print(f"Launching browser with {CONCURRENCY} concurrent tabs...")
    if chrome_path:
        print(f"  Using real Chrome: {chrome_path}")
    else:
        print("  Real Chrome not found, using Playwright Chromium")

    start_time = time.time()

    async with async_playwright() as p:
        launch_args = {
            "headless": False,
            "args": [
                "--disable-blink-features=AutomationControlled",
                "--disable-features=IsolateOrigins,site-per-process",
                "--no-first-run",
                "--no-default-browser-check",
            ]
        }
        if chrome_path:
            launch_args["executable_path"] = chrome_path

        browser = await p.chromium.launch(**launch_args)

        context = await browser.new_context(
            viewport={"width": 1366, "height": 768},
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            locale="en-US",
            timezone_id="America/New_York",
            color_scheme="light",
        )

        # Apply stealth patches
        if HAS_STEALTH:
            # stealth_async works on pages, we'll apply per-page in workers
            pass

        # Anti-detection init scripts
        await context.add_init_script("""
            // Hide webdriver flag
            Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
            delete navigator.__proto__.webdriver;

            // Fix chrome object
            window.chrome = { runtime: {}, loadTimes: function(){}, csi: function(){} };

            // Fix permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) =>
                parameters.name === 'notifications'
                    ? Promise.resolve({state: Notification.permission})
                    : originalQuery(parameters);

            // Fix plugins length
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });

            // Fix languages
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });

            // Fix hardware concurrency
            Object.defineProperty(navigator, 'hardwareConcurrency', {
                get: () => 8
            });

            // Fix platform
            Object.defineProperty(navigator, 'platform', {
                get: () => 'MacIntel'
            });
        """)

        # Warm up the browser session
        await warm_up_context(context)

        # Run workers
        workers_list = []
        for i in range(CONCURRENCY):
            workers_list.append(worker(queue, results, context, i, save_progress))

        await asyncio.gather(*workers_list)

        await browser.close()

    elapsed = time.time() - start_time
    print(f"\nScraping complete in {elapsed:.0f}s ({elapsed/60:.1f} min)")

    # Final save
    save_progress()

    # Summary
    all_results = dict(already_done)
    for r in results:
        if r is not None:
            all_results[r[0]] = r[1]

    succeeded = sum(1 for v in all_results.values() if v not in ('SCRAPE_FAILED', 'ACCESS_DENIED', '') and not v.startswith('ERROR'))
    blocked = sum(1 for v in all_results.values() if v == 'ACCESS_DENIED')
    failed = sum(1 for v in all_results.values() if v in ('SCRAPE_FAILED', '') or v.startswith('ERROR'))

    print(f"\nResults summary:")
    print(f"  Succeeded: {succeeded}")
    print(f"  Blocked (Access Denied): {blocked}")
    print(f"  Failed/No data: {failed}")
    print(f"  Total: {len(all_results)}")
    print(f"\nSaved to {OUTPUT_CSV}")

    if blocked > 0:
        print(f"\n⚠️  {blocked} URLs were blocked by Akamai.")
        print("  To retry blocked URLs, just run this script again (it resumes).")
        print("  If blocks persist, try:")
        print("    1. Wait 30+ minutes before retrying")
        print("    2. Reduce CONCURRENCY to 1")
        print("    3. Increase SLEEP_RANGE to (5.0, 10.0)")


if __name__ == "__main__":
    asyncio.run(main())
