"""Tests for Q3 evidence_grounding scorer (Week 3 Day 5)."""

from __future__ import annotations

import pytest

from eval.scorers.evidence_grounding import (
    EvidenceGroundingScore,
    aggregate_evidence_grounding,
    score_evidence_grounding,
)


def _base_report(findings):
    return {
        "schema_version": "v6.0",
        "report_id": "r",
        "generated_at": "2026-04-25T00:00:00Z",
        "query_id": "q",
        "context": {"is_serverless": False, "is_streaming": False, "is_federation": False},
        "summary": {"headline": "x", "verdict": "needs_attention", "key_metrics": []},
        "findings": findings,
    }


# ----- 1. metric grounding -----


def test_metric_grounding_all_grounded():
    r = _base_report([{
        "issue_id": "spill_dominant",
        "category": "memory",
        "severity": "high",
        "title": "Spill",
        "evidence": [
            {"metric": "peak_memory_bytes", "value_display": "12 GB",
             "source": "profile.queryMetrics", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.metric_grounding_ratio == 1.0


def test_metric_grounding_mixed():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "a", "value_display": "1", "source": "profile.x", "grounded": True},
            {"metric": "b", "value_display": "2", "source": "synthetic", "grounded": False},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.metric_grounding_ratio == 0.5


# ----- 2. ungrounded numeric -----


def test_ungrounded_numeric_in_action_what():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "high", "title": "Issue",
        "evidence": [
            {"metric": "m", "value_display": "abc",
             "source": "profile.x", "grounded": True},
        ],
        "actions": [{
            "action_id": "a", "target": "t", "fix_type": "configuration",
            "what": "30% 短縮 / 60秒 改善",  # neither in profile evidence
            "fix_sql": "",
        }],
    }])
    s = score_evidence_grounding(r)
    assert s.numeric_claim_total >= 2
    assert s.numeric_claim_unsupported >= 2
    assert s.ungrounded_numeric_ratio == 1.0


def test_actioncard_evidence_does_not_self_anchor_numerics():
    """V5 vs V6 smoke (2026-04-26) revealed an unfair scoring path:
    the V5 normalizer duplicates narrative text into
    ``actioncard.evidence`` value_displays, which the scorer was
    treating as a legitimate numeric anchor. That gave the V5 mode
    free credit for citing numerics it had just emitted itself.

    Anchor set must come from *profile-derived* sources only, not
    from evidence whose source is ``actioncard.evidence`` or
    ``synthetic``.
    """
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "high", "title": "Issue",
        "evidence": [
            # The narrative cited "12 GB" — this evidence entry quotes
            # the same string back, which is what V5 normalizer does.
            {"metric": "evidence_text", "value_display": "spill 12 GB observed",
             "source": "actioncard.evidence", "grounded": True},
        ],
        "actions": [{
            "action_id": "a", "target": "t", "fix_type": "configuration",
            "what": "12 GB の spill が問題",
            "fix_sql": "",
        }],
    }])
    s = score_evidence_grounding(r)
    # "12 GB" appears in both evidence value_display and narrative,
    # but the evidence source is ``actioncard.evidence`` — that must
    # NOT count as an anchor. So the narrative number stays
    # un-anchored and the ungrounded ratio is positive.
    assert s.ungrounded_numeric_ratio > 0
    assert s.numeric_claim_unsupported >= 1


def test_synthetic_evidence_does_not_self_anchor_numerics():
    """``synthetic`` source (LLM-generated derived/computed values)
    must also not self-anchor — the scorer would otherwise reward
    LLMs that fabricate plausible-looking evidence."""
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "high", "title": "Issue",
        "evidence": [
            {"metric": "ratio", "value_display": "5倍 改善見込み",
             "source": "synthetic", "grounded": True},
        ],
        "actions": [{
            "action_id": "a", "target": "t", "fix_type": "configuration",
            "what": "5倍 高速化",
            "fix_sql": "",
        }],
    }])
    s = score_evidence_grounding(r)
    assert s.ungrounded_numeric_ratio > 0


def test_whitespace_in_unit_normalized_for_anchor_match():
    """Smoke 5 (2026-04-26): LLM narrative writes "97ms" while
    structured evidence carries "97 ms" (with space). Without
    normalization the scorer treats them as different tokens and
    inflates ungrounded_numeric_ratio. Both must compare equal."""
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "high", "title": "x",
        "evidence": [
            # Evidence side: "97 ms" with a space.
            {"metric": "node_dur", "value_display": "97 ms",
             "source": "node[7557]", "grounded": True},
        ],
        "actions": [{
            "action_id": "a", "target": "t", "fix_type": "configuration",
            "what": "Whole Stage Codegen 97ms がある",  # narrative side: no space
            "fix_sql": "",
        }],
    }])
    s = score_evidence_grounding(r)
    # The single narrative numeric "97ms" should match the anchor
    # "97 ms" after whitespace normalization.
    assert s.numeric_claim_unsupported == 0


def test_grounded_numeric_anchored_in_evidence():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "high", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "12 GB",
             "source": "profile.x", "grounded": True},
        ],
        "actions": [{
            "action_id": "a", "target": "t", "fix_type": "configuration",
            "what": "12 GB を削減する",  # anchored in evidence
            "fix_sql": "",
        }],
    }])
    s = score_evidence_grounding(r)
    assert s.numeric_claim_unsupported == 0


# ----- 3. valid source taxonomy -----


def test_invalid_source_detected():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "1",
             "source": "made_up_provenance", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.valid_source_ratio == 0.0
    assert "made_up_provenance" in s.source_invalid


def test_valid_source_prefixes_accepted():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m1", "value_display": "1", "source": "profile.q.spill", "grounded": True},
            {"metric": "m2", "value_display": "2", "source": "node[12].peak", "grounded": True},
            {"metric": "m3", "value_display": "3", "source": "alert:memory", "grounded": True},
            {"metric": "m4", "value_display": "4", "source": "knowledge:spill", "grounded": True},
            {"metric": "m5", "value_display": "5", "source": "synthetic", "grounded": False},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.valid_source_ratio == 1.0


# ----- 4. valid knowledge_section_id -----


def test_unknown_knowledge_id_detected():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "1",
             "source": "knowledge:totally_not_a_real_section_xyz", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.knowledge_refs_total == 1
    assert s.valid_knowledge_section_ratio == 0.0
    assert "totally_not_a_real_section_xyz" in s.knowledge_refs_unknown


def test_known_knowledge_id_accepted():
    """`spill` is a real section_id present in dbsql_tuning.md."""
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "1",
             "source": "knowledge:spill", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.valid_knowledge_section_ratio == 1.0


# ----- 5. finding-level support -----


def test_finding_support_synthetic_only_does_not_count():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "evidence_text", "value_display": "...",
             "source": "synthetic", "grounded": False},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.findings_with_grounded_support == 0
    assert s.finding_support_ratio == 0.0


def test_finding_support_real_grounded_counts():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "1",
             "source": "profile.x", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.finding_support_ratio == 1.0


# ----- composite -----


def test_composite_score_clean_report():
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "peak_memory_bytes", "value_display": "12 GB",
             "source": "profile.queryMetrics", "grounded": True},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    assert s.composite_score == 1.0


# ----- W3.5 #2: weighted composite -----


def test_composite_weights_sum_to_one():
    """Sanity: weights add up to 1.0 (asserted at import time too)."""
    from eval.scorers.evidence_grounding import COMPOSITE_WEIGHTS
    assert abs(sum(COMPOSITE_WEIGHTS.values()) - 1.0) < 1e-9


def test_composite_demotes_taxonomy_when_core_low():
    """When core (finding_support + metric_grounded) is low and only
    taxonomy is 100%, composite should NOT cross 0.5.

    Construct a report whose finding has 1 grounded + 1 synthetic evidence
    (50% metric_grounded / 50% finding_support / 100% taxonomy).
    """
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "m", "value_display": "1",
             "source": "profile.x", "grounded": True},
            {"metric": "evidence_text", "value_display": "synthetic",
             "source": "synthetic", "grounded": False},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    # finding_support: 1 grounded non-synthetic exists → 1.0
    # metric_grounded_ratio: 1/2 = 0.5
    # ungrounded_numeric: 0 (no numeric)
    # valid_source: 100%, valid_knowledge_id: 100%
    # composite = 1.0*0.35 + 0.5*0.30 + 1.0*0.20 + 1.0*0.075 + 1.0*0.075
    #           = 0.35 + 0.15 + 0.20 + 0.075 + 0.075 = 0.85
    assert abs(s.composite_score - 0.85) < 1e-3


def test_composite_taxonomy_alone_does_not_inflate():
    """Even with taxonomy/id=100%, if both core signals are 0, composite
    must NOT cross 0.5 (the new weights guarantee taxonomy-only ≤ 0.30 +
    ungrounded_inverse contribution)."""
    # Build a case with all-synthetic findings: finding_support=0,
    # metric_grounded=0, but valid_source=100% (synthetic is allowed).
    r = _base_report([{
        "issue_id": "x", "category": "memory", "severity": "low", "title": "x",
        "evidence": [
            {"metric": "evidence_text", "value_display": "x",
             "source": "synthetic", "grounded": False},
        ],
        "actions": [],
    }])
    s = score_evidence_grounding(r)
    # finding_support: 0 (only synthetic)
    # metric_grounded: 0/1 = 0
    # ungrounded_numeric: 0 (no numerics) → inverse = 1.0
    # valid_source: 1.0, valid_knowledge_id: 1.0
    # composite = 0*0.35 + 0*0.30 + 1*0.20 + 1*0.075 + 1*0.075 = 0.35
    assert s.composite_score < 0.5
    assert abs(s.composite_score - 0.35) < 1e-3


def test_aggregate_avg():
    s1 = EvidenceGroundingScore(composite_score=0.8)
    s2 = EvidenceGroundingScore(composite_score=0.6)
    assert aggregate_evidence_grounding([s1, s2]) == 0.7


def test_aggregate_empty():
    assert aggregate_evidence_grounding([]) == 1.0
