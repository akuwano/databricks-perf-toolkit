#!/usr/bin/env python3
"""Post-deploy smoke test for Databricks Performance Toolkit.

Runs against a deployed app to verify core functionality works.
Exit code 0 = all checks passed, 1 = failures detected.

Usage:
    python scripts/smoke_test.py <app_url>
    python scripts/smoke_test.py https://databricks-perf-kit-staging-xxx.aws.databricksapps.com

Requires:
    - requests (pip install requests)
    - A valid Databricks OAuth token (set DATABRICKS_TOKEN env var)
      or run from a context where the app accepts unauthenticated requests.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TIMEOUT = 30  # seconds per request
ANALYSIS_TIMEOUT = 300  # seconds to wait for analysis completion (LLM takes ~2-3 min)
SAMPLE_PROFILE = Path(__file__).parent.parent / "json" / "physical_table_good_perf_query_profile.json"


def _session(token: str = "") -> requests.Session:
    s = requests.Session()
    s.headers["Content-Type"] = "application/json"
    if token:
        s.headers["Authorization"] = f"Bearer {token}"
    return s


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------

class SmokeTest:
    def __init__(self, app_url: str, token: str = "", verbose: bool = False):
        self.base = app_url.rstrip("/")
        self.session = _session(token)
        self.verbose = verbose
        self.passed = 0
        self.failed = 0
        self.errors: list[str] = []

    def _get(self, path: str) -> requests.Response | None:
        url = f"{self.base}{path}"
        try:
            r = self.session.get(url, timeout=TIMEOUT)
            if self.verbose:
                print(f"  GET {path} → {r.status_code} ({len(r.content)} bytes)")
            return r
        except Exception as e:
            self._fail(f"GET {path} failed: {e}")
            return None

    def _post(self, path: str, data: dict | None = None, files: dict | None = None) -> requests.Response | None:
        url = f"{self.base}{path}"
        try:
            if files:
                # Remove Content-Type for multipart
                headers = {k: v for k, v in self.session.headers.items() if k != "Content-Type"}
                r = requests.post(url, files=files, headers=headers, timeout=TIMEOUT)
            else:
                r = self.session.post(url, json=data, timeout=TIMEOUT)
            if self.verbose:
                print(f"  POST {path} → {r.status_code}")
            return r
        except Exception as e:
            self._fail(f"POST {path} failed: {e}")
            return None

    def _check(self, name: str, condition: bool, detail: str = ""):
        if condition:
            self.passed += 1
            print(f"  ✅ {name}")
        else:
            self._fail(f"{name}: {detail}")

    def _fail(self, msg: str):
        self.failed += 1
        self.errors.append(msg)
        print(f"  ❌ {msg}")

    # --- Individual checks ---

    def check_app_health(self):
        """Check app is running and returns HTML."""
        print("\n🔍 App Health")
        r = self._get("/")
        if r is None:
            return
        self._check("App returns 200", r.status_code == 200)
        self._check("HTML response", "Performance Toolkit" in r.text, f"Got: {r.text[:100]}")

    def check_debug_config(self):
        """Check runtime-config is loaded."""
        print("\n🔍 Debug Config")
        r = self._get("/api/v1/debug/config")
        if r is None:
            return
        self._check("Config endpoint returns 200", r.status_code == 200)
        data = r.json()
        settings = data.get("settings", {})

        # Check catalog is not default
        catalog_source = settings.get("catalog", {}).get("source", "")
        self._check(
            "Catalog from runtime_config (not default)",
            catalog_source == "runtime_config",
            f"source={catalog_source}",
        )

        # Check http_path is set
        http_path = settings.get("http_path", {}).get("value", "")
        self._check("HTTP path is configured", bool(http_path), f"value={http_path}")

    def check_dbsql_settings(self):
        """Check DBSQL settings endpoint."""
        print("\n🔍 DBSQL Settings")
        r = self._get("/api/v1/settings")
        if r is None:
            return
        self._check("Settings returns 200", r.status_code == 200)
        data = r.json()
        self._check("Catalog is set", bool(data.get("catalog")), f"catalog={data.get('catalog')}")
        self._check("Schema is set", bool(data.get("schema")), f"schema={data.get('schema')}")

    def check_spark_perf_settings(self):
        """Check Spark Perf settings endpoint."""
        print("\n🔍 Spark Perf Settings")
        r = self._get("/api/v1/spark-perf/settings")
        if r is None:
            return
        self._check("Spark Perf settings returns 200", r.status_code == 200)
        data = r.json()
        self._check("Catalog is set", bool(data.get("catalog")), f"catalog={data.get('catalog')}")
        self._check("ETL Job ID is set", bool(data.get("etl_job_id")), f"etl_job_id={data.get('etl_job_id')}")

    def check_spark_perf_applications(self):
        """Check Spark Perf applications list (may be empty but should not error)."""
        print("\n🔍 Spark Perf Applications")
        r = self._get("/api/v1/spark-perf/applications?page=1&page_size=5")
        if r is None:
            return
        self._check("Applications returns 200", r.status_code == 200)
        data = r.json()
        self._check("Response has items key", "items" in data, f"keys={list(data.keys())}")
        self._check("Response has total key", "total" in data)

    def check_history(self):
        """Check analysis history endpoint."""
        print("\n🔍 Analysis History")
        r = self._get("/api/v1/history?limit=5")
        if r is None:
            return
        # May return empty list or error if tables don't exist yet — both are OK
        self._check("History returns 200", r.status_code == 200)

    def check_pages_load(self):
        """Check all main pages return 200 (no server errors)."""
        print("\n🔍 Page Load Check")
        pages = [
            ("/", "Home"),
            ("/history", "History"),
            ("/compare", "Compare"),
            ("/spark-perf", "Spark Perf"),
        ]
        for path, name in pages:
            r = self._get(path)
            if r:
                self._check(f"{name} page loads", r.status_code == 200, f"status={r.status_code}")

    def check_analysis_flow(self):
        """Run a full analysis and verify report content."""
        print("\n🔍 Analysis Flow")
        if not SAMPLE_PROFILE.exists():
            print(f"  ⏭ Skipped (sample profile not found: {SAMPLE_PROFILE})")
            return

        # --- A: Upload and start analysis ---
        with open(SAMPLE_PROFILE, "rb") as f:
            r = self._post(
                "/api/v1/analyze",
                files={"file": ("test_profile.json", f, "application/json")},
            )
        if r is None:
            return
        self._check("Analysis accepted", r.status_code == 200, f"status={r.status_code}")

        data = r.json()
        analysis_id = data.get("id", "") or data.get("analysis_id", "")
        self._check("Analysis ID returned", bool(analysis_id), f"keys={list(data.keys())}")
        if not analysis_id:
            return

        # Poll for completion
        start = time.time()
        status = "processing"
        while time.time() - start < ANALYSIS_TIMEOUT:
            r = self._get(f"/api/v1/analyze/{analysis_id}/status")
            if r and r.status_code == 200:
                status_data = r.json()
                status = status_data.get("status", "")
                if status in ("completed", "failed"):
                    break
            time.sleep(3)

        self._check("Analysis completed", status == "completed", f"status={status}")
        if status != "completed":
            return

        # --- B: Report page loads and has content ---
        r = self._get(f"/shared/{analysis_id}")
        if r is None:
            return
        self._check("Report page returns 200", r.status_code == 200, f"status={r.status_code}")
        self._check("Report page has content", len(r.text) > 1000, f"length={len(r.text)}")

        # --- C: Report contains expected sections ---
        report_text = r.text

        # Check for key sections (support both EN and JA)
        section_checks = [
            ("Executive Summary / サマリー",
             any(kw in report_text for kw in ["Executive Summary", "エグゼクティブサマリー", "サマリー"])),
            ("Bottleneck / ボトルネック",
             any(kw in report_text for kw in ["Bottleneck", "ボトルネック"])),
            ("Recommendations / 推奨",
             any(kw in report_text for kw in ["Recommend", "推奨", "Action Plan", "アクションプラン"])),
            ("Query metrics present",
             any(kw in report_text for kw in ["Total Time", "合計時間", "total_time", "Cache Hit", "キャッシュ"])),
        ]
        for name, found in section_checks:
            self._check(f"Report contains: {name}", found)

        # Check KPI cards are rendered (shared_result.html)
        kpi_checks = [
            ("KPI: Total Time", any(kw in report_text for kw in ["Total Time", "合計時間"])),
            ("KPI: Cache Hit", any(kw in report_text for kw in ["Cache Hit", "キャッシュヒット", "Cache"])),
            ("KPI: Photon", "Photon" in report_text),
        ]
        for name, found in kpi_checks:
            self._check(f"{name}", found)

    # --- Runner ---

    def run_all(self, skip_analysis: bool = False):
        print(f"🚀 Smoke Test: {self.base}")

        self.check_app_health()
        self.check_debug_config()
        self.check_dbsql_settings()
        self.check_spark_perf_settings()
        self.check_spark_perf_applications()
        self.check_history()
        self.check_pages_load()

        if not skip_analysis:
            self.check_analysis_flow()

        # Summary
        total = self.passed + self.failed
        print(f"\n{'='*50}")
        print(f"Results: {self.passed}/{total} passed, {self.failed} failed")
        if self.errors:
            print("\nFailures:")
            for e in self.errors:
                print(f"  ❌ {e}")
        print(f"{'='*50}")

        return self.failed == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Post-deploy smoke test")
    parser.add_argument("app_url", help="Deployed app URL")
    parser.add_argument("--token", default=os.environ.get("DATABRICKS_TOKEN", ""), help="OAuth token")
    parser.add_argument("--skip-analysis", action="store_true", help="Skip full analysis flow test")
    parser.add_argument("-v", "--verbose", action="store_true", help="Verbose output")
    args = parser.parse_args()

    test = SmokeTest(args.app_url, token=args.token, verbose=args.verbose)
    success = test.run_all(skip_analysis=args.skip_analysis)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
