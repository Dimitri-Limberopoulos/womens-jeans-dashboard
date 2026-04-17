#!/usr/bin/env python3
"""
Test Harness for Target PDP Scraper (Per-Color Output)
=======================================================
Runs 5 sample URLs, prints key fields per color row, saves test Excel.

Usage:
    pip3 install playwright openpyxl
    python3 -m playwright install chromium
    python3 test_target_pdp.py
"""

import asyncio, os, random
from playwright.async_api import async_playwright
from target_pdp_scraper import parse_target_pdp, setup_context, save_to_excel, extract_js_pricing

TEST_URLS = [
    "https://www.target.com/p/women-s-everyday-high-rise-wide-leg-jeans-universal-thread/-/A-94441288?preselect=94766131#lnk=sametab",
    "https://www.target.com/p/women-s-mid-rise-boyfriend-jeans-universal-thread/-/A-94776711?preselect=1003883672#lnk=sametab",
    "https://www.target.com/p/levi-s-women-s-501-jeans/-/A-92690344?preselect=94771243#lnk=sametab",
    "https://www.target.com/p/women-s-high-rise-sailor-wide-leg-ankle-jeans-universal-thread/-/A-94164750?preselect=89508472#lnk=sametab",
    "https://www.target.com/p/kbb-by-kahlana-women-s-mid-rise-barrel-leg-the-lynnox-jean-dark-wash/-/A-94680226?preselect=94893061#lnk=sametab",
]


async def main():
    sdir = os.path.dirname(os.path.abspath(__file__))
    print("🎯 Target PDP Test Harness (Per-Color Output)")
    print(f"   Testing {len(TEST_URLS)} URLs\n")

    pw = await async_playwright().start()
    browser = await pw.chromium.launch(headless=False, args=[
        "--disable-blink-features=AutomationControlled",
        "--disable-dev-shm-usage",
        "--no-sandbox",
    ])
    ctx = await setup_context(browser, 0)

    async def block_resources(route):
        await route.abort()

    page = await ctx.new_page()
    await page.route('**/*.{png,jpg,jpeg,gif,svg,woff,woff2,ttf,ico,webp}', block_resources)

    all_rows = []
    total_urls_ok = 0
    total_urls_err = 0

    for i, url in enumerate(TEST_URLS):
        print(f"\n{'='*70}")
        print(f"  URL {i+1}/{len(TEST_URLS)}: {url[:70]}...")
        print(f"{'='*70}")

        try:
            resp = await page.goto(url, wait_until='domcontentloaded', timeout=20000)
            status = resp.status if resp else 0
            print(f"  HTTP {status}")

            if status >= 400:
                print(f"  ❌ HTTP error {status}")
                all_rows.append({'url': url, 'error': f'HTTP {status}', 'retries': 0})
                total_urls_err += 1
                continue

            await asyncio.sleep(random.uniform(2, 4))
            html_content = await page.content()
            print(f"  Page size: {len(html_content):,} chars")

            # Extract per-TCIN pricing from JS (supplements HTML regex)
            js_pricing = await extract_js_pricing(page)
            if js_pricing:
                print(f"  JS pricing: {len(js_pricing)} TCINs found")

            color_rows = parse_target_pdp(html_content, url, js_pricing=js_pricing)

            if color_rows and color_rows[0].get('error'):
                print(f"  ❌ Parse error: {color_rows[0]['error']}")
                total_urls_err += 1
            else:
                total_urls_ok += 1
                print(f"  ✅ {len(color_rows)} color rows extracted")
                print(f"     Brand: {color_rows[0].get('brand', '?')} | "
                      f"Type: {color_rows[0].get('brand_type', '?')}")
                print(f"     Parent Price: {color_rows[0].get('current_price', '?')} ({color_rows[0].get('price_type', '?')})")
                if color_rows[0].get('original_price'):
                    print(f"     Original: {color_rows[0]['original_price']} | "
                          f"Save: {color_rows[0].get('save_percent', '?')}")
                print(f"     Rating: {color_rows[0].get('rating_avg', '?')} "
                      f"({color_rows[0].get('review_count', '?')} reviews)")

                for j, row in enumerate(color_rows):
                    print(f"\n     --- Color {j+1}: {row.get('color', '(none)')} ---")
                    print(f"         TCIN: {row.get('color_tcin', '?')}")
                    print(f"         Price: {row.get('current_price', '?')} ({row.get('price_type', '?')}) "
                          f"[min={row.get('price_min', '?')}, max={row.get('price_max', '?')}]")
                    if row.get('color_current_price'):
                        print(f"         Child Price: {row.get('color_current_price', '?')} "
                              f"(retail={row.get('color_current_retail', '?')}, reg={row.get('color_reg_retail', '?')})")
                    if row.get('original_price'):
                        print(f"         Original: {row['original_price']} | Save: {row.get('save_percent', '?')}")
                    print(f"         Sizes: {row.get('color_sizes', '?')[:60]}")
                    print(f"         Size Groups: {row.get('color_size_groups', '?')}")
                    print(f"         # SKUs: {row.get('color_num_skus', '?')}")
                    print(f"         Material: {row.get('material', '?')}")
                    print(f"         Rise: {row.get('rise', '?')} | Fit: {row.get('fit', '?')}")
                    print(f"         Inseam: {row.get('inseam_length', '?')} | Stretch: {row.get('stretch', '?')}")
                    print(f"         Non-Basic: {row.get('non_basic', '?')}")
                    if row.get('color_image_url'):
                        print(f"         Image: {row['color_image_url'][:60]}...")

            for row in color_rows:
                row['retries'] = 0
                if 'url' not in row or not row.get('url'):
                    row['url'] = url
            all_rows.extend(color_rows)

        except Exception as e:
            print(f"  ❌ Error: {str(e)[:200]}")
            all_rows.append({'url': url, 'error': str(e)[:200], 'retries': 0})
            total_urls_err += 1

        if i < len(TEST_URLS) - 1:
            delay = random.uniform(3, 6)
            print(f"\n  Waiting {delay:.1f}s...")
            await asyncio.sleep(delay)

    # Save results
    out = save_to_excel(all_rows, sdir)

    print(f"\n\n{'='*70}")
    print(f"  TEST SUMMARY")
    print(f"{'='*70}")
    print(f"  URLs tested:    {len(TEST_URLS)}")
    print(f"  URLs OK:        {total_urls_ok}")
    print(f"  URLs errored:   {total_urls_err}")
    print(f"  Total rows:     {len(all_rows)}")
    print(f"  Excel saved:    {out}")

    # Field coverage
    ok_rows = [r for r in all_rows if not r.get('error')]
    if ok_rows:
        print(f"\n  Field coverage ({len(ok_rows)} data rows):")
        key_fields = [
            'title', 'brand', 'brand_type', 'color', 'current_price',
            'color_current_price', 'price_min', 'price_max',
            'color_sizes', 'color_tcin', 'material', 'rise', 'fit',
            'inseam_length', 'stretch', 'rating_avg', 'review_count',
            'description', 'department', 'origin', 'color_image_url', 'url',
        ]
        for field in key_fields:
            has = sum(1 for r in ok_rows if r.get(field) not in (None, '', 0, False, '0%'))
            pct = has / len(ok_rows) * 100
            icon = '✅' if pct >= 80 else '⚠️' if pct >= 50 else '❌'
            print(f"    {icon} {field:25s} {has}/{len(ok_rows)} ({pct:.0f}%)")

    await page.close()
    await ctx.close()
    await browser.close()
    await pw.stop()


if __name__ == '__main__':
    asyncio.run(main())
