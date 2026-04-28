"""Tests for V6 canonical schema normalizer (Week 2 Day 4).

Verifies the adapter from existing ProfileAnalysis -> canonical Report dict.
Uses jsonschema to assert each output is a valid V6 report.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator

from core.models import (
    ActionCard,
    Alert,
    BottleneckIndicators,
    ProfileAnalysis,
    QueryMetrics,
    Severity,
    StreamingContext,
)
from core.v6_schema import build_canonical_report

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_PATH = REPO_ROOT / "schemas" / "report_v6.schema.json"


@pytest.fixture(scope="module")
def validator():
    with open(SCHEMA_PATH, encoding="utf-8") as f:
        schema = json.load(f)
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema)


def _make_analysis(**kw):
    qm = QueryMetrics(
        query_id=kw.get("query_id", "q-test-1"),
        query_typename=kw.get("query_typename", "LakehouseSqlQuery"),
        total_time_ms=kw.get("total_time_ms", 12345),
        read_bytes=kw.get("read_bytes", 1024 * 1024 * 100),
        spill_to_disk_bytes=kw.get("spill_to_disk_bytes", 0),
        result_from_cache=kw.get("result_from_cache", False),
        is_federation_query=kw.get("is_federation_query", False),
    )
    bi = BottleneckIndicators(
        cache_hit_ratio=kw.get("cache_hit_ratio", 0.85),
        alerts=kw.get("alerts", []),
    )
    pa = ProfileAnalysis(
        query_metrics=qm,
        bottleneck_indicators=bi,
        action_cards=kw.get("action_cards", []),
        streaming_context=kw.get("streaming_context"),
    )
    if kw.get("warehouse_info") is not None:
        pa.warehouse_info = kw["warehouse_info"]
    return pa


def test_minimum_healthy_report_validates(validator):
    pa = _make_analysis()
    r = build_canonical_report(pa)
    validator.validate(r)
    assert r["schema_version"] == "v6.0"
    assert r["summary"]["verdict"] == "healthy"
    assert r["findings"] == []


def test_skipped_cached_verdict(validator):
    pa = _make_analysis(result_from_cache=True)
    r = build_canonical_report(pa)
    validator.validate(r)
    assert r["summary"]["verdict"] == "skipped_cached"


def test_card_only_finding_validates(validator):
    card = ActionCard(
        problem="Spill が支配的",
        evidence=["peak_memory=12GB", "spill_bytes=8GB"],
        likely_cause="warehouse メモリ不足",
        fix_sql="SET spark.databricks.photon.enabled=true;",
        fix="Photon を有効化",
        expected_impact="high",
        validation_metric="spill_bytes",
        risk="low",
        risk_reason="設定変更のみ",
        priority_rank=80,
        coverage_category="MEMORY",
        root_cause_group="spill_dominant",
    )
    pa = _make_analysis(action_cards=[card], spill_to_disk_bytes=8 * 1024**3)
    r = build_canonical_report(pa)
    validator.validate(r)
    assert len(r["findings"]) == 1
    f = r["findings"][0]
    assert f["issue_id"] in ("spill_dominant", "spill_dominant_2")
    assert f["category"] == "memory"
    assert f["actions"][0]["fix_type"] == "configuration"
    assert f["actions"][0]["target"].startswith("spark.databricks.photon")
    # evidence parsed
    metrics = {e["metric"] for e in f["evidence"]}
    assert "peak_memory" in metrics or "evidence_text" in metrics


def test_alert_pairs_with_card(validator):
    alert = Alert(
        severity=Severity.HIGH,
        category="cache",
        message="Cache hit ratio 低",
        metric_name="cache_hit_ratio",
        current_value="25%",
        threshold="<50%",
        recommendation="warm-up を検討",
    )
    card = ActionCard(
        problem="cache が低い",
        evidence=["cache_hit_ratio=25%"],
        fix="Warm-up を実施",
        coverage_category="DATA",
        priority_rank=60,
    )
    pa = _make_analysis(alerts=[alert], action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    f = r["findings"][0]
    # Alert + card paired into one finding
    assert "alert_links" in f
    assert f["alert_links"][0].startswith("cache:")


def test_federation_suppresses_clustering_finding(validator):
    alert = Alert(severity=Severity.MEDIUM, category="clustering", message="LC 検討", metric_name="files_pruned")
    card = ActionCard(problem="LC 検討", fix_sql="ALTER TABLE t CLUSTER BY (a);", priority_rank=50)
    pa = _make_analysis(alerts=[alert], action_cards=[card], is_federation_query=True)
    r = build_canonical_report(pa)
    validator.validate(r)
    # Should be in appendix, not main findings
    assert r["findings"] == []
    assert "appendix_excluded_findings" in r
    assert r["appendix_excluded_findings"][0]["suppression_reason"] == "federation_workload_irrelevant"


def test_streaming_suppresses_optimize_action(validator):
    sc = StreamingContext(is_streaming=True, target_table="my.tbl")
    card = ActionCard(problem="OPTIMIZE 実行", fix_sql="OPTIMIZE my.tbl;", priority_rank=40)
    pa = _make_analysis(action_cards=[card], streaming_context=sc)
    r = build_canonical_report(pa)
    validator.validate(r)
    assert r["context"]["is_streaming"] is True
    # OPTIMIZE on streaming -> suppressed
    assert any(
        f["suppression_reason"] == "streaming_inappropriate"
        for f in r.get("appendix_excluded_findings", [])
    )


def test_verification_steps_normalized(validator):
    card = ActionCard(
        problem="Photon 化",
        fix_sql="SET spark.databricks.photon.enabled=true;",
        verification_steps=[{"metric": "photon_ratio", "expected": ">90%"}],
        priority_rank=70,
    )
    pa = _make_analysis(action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    a = r["findings"][0]["actions"][0]
    assert a["verification"][0]["type"] == "metric"
    assert a["verification"][0]["expected"] == ">90%"


def test_no_evidence_card_gets_synthetic_evidence(validator):
    card = ActionCard(problem="何らかの問題", fix="調査", priority_rank=20)
    pa = _make_analysis(action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    f = r["findings"][0]
    assert len(f["evidence"]) >= 1
    # synthetic evidence has grounded=false
    assert any(e.get("grounded") is False for e in f["evidence"])


# ----- W2.5 #3: Evidence.grounded actual detection -----


def test_evidence_grounded_known_metric(validator):
    """metric_name は QueryMetrics に存在する → grounded=True."""
    alert = Alert(
        severity=Severity.HIGH,
        category="memory",
        message="Spill",
        metric_name="spill_to_disk_bytes",  # exists on QueryMetrics
        current_value="8 GB",
    )
    pa = _make_analysis(alerts=[alert])
    r = build_canonical_report(pa)
    validator.validate(r)
    ev = r["findings"][0]["evidence"][0]
    assert ev["grounded"] is True


def test_evidence_grounded_unknown_metric(validator):
    """alert.metric_name が profile に存在しない → grounded=False."""
    alert = Alert(
        severity=Severity.HIGH,
        category="other",
        message="Made-up",
        metric_name="this_metric_does_not_exist_anywhere_in_profile",
        current_value="42",
    )
    pa = _make_analysis(alerts=[alert])
    r = build_canonical_report(pa)
    validator.validate(r)
    ev = r["findings"][0]["evidence"][0]
    assert ev["grounded"] is False


def test_action_attaches_sql_skeleton_metadata(validator):
    """W5 Day 3: normalizer should attach skeleton metadata when fix_sql is set."""
    card = ActionCard(
        problem="Photon を有効化",
        fix_sql="SET spark.databricks.photon.enabled=true;",
        priority_rank=80,
    )
    pa = _make_analysis(action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    a = r["findings"][0]["actions"][0]
    assert a["fix_sql_skeleton"]  # non-empty
    assert a["fix_sql_skeleton_method"] in {"fullsql", "sqlglot", "bypass"}
    assert a["fix_sql_chars_original"] > 0
    # short SQL: skeleton == fullsql, chars match
    assert a["fix_sql_chars_in_prompt"] == a["fix_sql_chars_original"]


def test_action_includes_rollback_for_optimize(validator):
    card = ActionCard(
        problem="OPTIMIZE 推奨",
        fix_sql="OPTIMIZE my.tbl FULL;",
        fix="OPTIMIZE で file 数を削減",
        priority_rank=60,
    )
    pa = _make_analysis(action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    a = r["findings"][0]["actions"][0]
    assert a["rollback"]["type"] == "irreversible"
    assert a["impact_confidence"] in {"low", "medium", "needs_verification"}
    assert "OPTIMIZE" in a["preconditions"][0] or "OPTIMIZE" in (a.get("preconditions") or [""])[0]


def test_action_includes_rollback_for_configuration(validator):
    card = ActionCard(
        problem="Photon を有効化",
        fix_sql="SET spark.databricks.photon.enabled=true;",
        fix="Photon を有効化",
        priority_rank=80,
        expected_impact="30% 短縮",
    )
    pa = _make_analysis(action_cards=[card])
    r = build_canonical_report(pa)
    validator.validate(r)
    a = r["findings"][0]["actions"][0]
    assert a["rollback"]["type"] == "config"
    # priority_rank=80 + quant("30%") → high confidence
    assert a["impact_confidence"] == "high"


def test_evidence_grounded_card_evidence_kv(validator):
    """ActionCard.evidence の `metric=value` 形式で metric が known なら True."""
    card = ActionCard(
        problem="Spill",
        evidence=["spill_to_disk_bytes=8GB", "fictitious_metric=99"],
        priority_rank=80,
    )
    pa = _make_analysis(action_cards=[card], spill_to_disk_bytes=8 * 1024**3)
    r = build_canonical_report(pa)
    validator.validate(r)
    f = r["findings"][0]
    by_metric = {e["metric"]: e for e in f["evidence"]}
    assert by_metric["spill_to_disk_bytes"]["grounded"] is True
    assert by_metric["fictitious_metric"]["grounded"] is False
