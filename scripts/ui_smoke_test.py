#!/usr/bin/env python3
"""Post-deploy UI smoke test using Playwright.

Opens each page in a headless browser and checks for:
- JavaScript errors (SyntaxError, TypeError, etc.)
- Console errors
- Page elements rendering correctly

Usage:
    uv run python scripts/ui_smoke_test.py <app_url> --token <token>

Requires:
    - playwright (uv add --dev playwright)
    - chromium (uv run python -m playwright install chromium)
"""

from __future__ import annotations

import argparse
import os
import sys

from playwright.sync_api import sync_playwright


def main():
    parser = argparse.ArgumentParser(description="UI smoke test with Playwright")
    parser.add_argument("app_url", help="Deployed app URL")
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""), help="OAuth token")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    base_url = args.app_url.rstrip("/")
    token = args.token

    pages_to_check = [
        ("/", "Home"),
        ("/history", "History"),
        ("/compare", "Compare"),
        ("/spark-perf", "Spark Perf"),
    ]

    passed = 0
    failed = 0
    errors: list[str] = []

    print(f"🌐 UI Smoke Test: {base_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            extra_http_headers={"Authorization": f"Bearer {token}"} if token else {},
        )

        for path, name in pages_to_check:
            url = f"{base_url}{path}"
            page = context.new_page()

            js_errors: list[str] = []
            console_errors: list[str] = []

            page.on("pageerror", lambda err: js_errors.append(str(err)))
            page.on("console", lambda msg: console_errors.append(msg.text) if msg.type == "error" else None)

            print(f"\n  🔍 {name} ({path})")

            try:
                response = page.goto(url, wait_until="networkidle", timeout=30000)

                # Check HTTP status
                status = response.status if response else 0
                if status == 200:
                    print(f"    ✅ HTTP {status}")
                    passed += 1
                else:
                    msg = f"{name}: HTTP {status}"
                    print(f"    ❌ {msg}")
                    errors.append(msg)
                    failed += 1

                # Wait a bit for async JS to execute
                page.wait_for_timeout(2000)

                # Check JS errors
                if js_errors:
                    for err in js_errors:
                        msg = f"{name}: JS Error — {err[:200]}"
                        print(f"    ❌ {msg}")
                        errors.append(msg)
                        failed += 1
                else:
                    print(f"    ✅ No JS errors")
                    passed += 1

                # Check console errors (warnings excluded)
                if console_errors:
                    for err in console_errors:
                        if args.verbose:
                            print(f"    ⚠ Console error: {err[:200]}")
                    # Console errors are warnings, not failures
                    print(f"    ⚠ {len(console_errors)} console error(s)")
                else:
                    print(f"    ✅ No console errors")
                    passed += 1

                # Check page has content (not blank)
                body_text = page.inner_text("body")
                if len(body_text.strip()) > 50:
                    print(f"    ✅ Page has content ({len(body_text)} chars)")
                    passed += 1
                else:
                    msg = f"{name}: Page appears blank ({len(body_text)} chars)"
                    print(f"    ❌ {msg}")
                    errors.append(msg)
                    failed += 1

            except Exception as e:
                msg = f"{name}: Page load failed — {e}"
                print(f"    ❌ {msg}")
                errors.append(msg)
                failed += 1
            finally:
                page.close()

        browser.close()

    # Summary
    total = passed + failed
    print(f"\n{'='*50}")
    print(f"Results: {passed}/{total} passed, {failed} failed")
    if errors:
        print("\nFailures:")
        for e in errors:
            print(f"  ❌ {e}")
    print(f"{'='*50}")

    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
