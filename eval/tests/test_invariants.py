"""Tests for L2 profile-signature invariants scorer."""

from __future__ import annotations

from eval.profile_evidence import ProfileEvidence
from eval.scorers.invariants import (
    InvariantsScore,
    aggregate_invariants,
    score_invariants,
)


# ---- No-op behavior ----


def test_no_op_when_no_signature_fires():
    evidence = ProfileEvidence()  # all defaults False
    score = score_invariants(evidence, canonical={}, llm_narrative="anything")
    assert score.no_op is True
    assert score.score == 1.0
    assert score.fired_invariants == []


# ---- DECIMAL invariant ----


def test_decimal_invariant_satisfied_by_canonical_action():
    evidence = ProfileEvidence(
        decimal_arithmetic_in_heavy_agg=True,
        decimal_arithmetic_examples=[("55064", "SUM(qty * price)")],
    )
    canonical = {
        "findings": [
            {
                "actions": [
                    {"what": "Verify DECIMAL precision via DESCRIBE TABLE."}
                ]
            }
        ]
    }
    score = score_invariants(evidence, canonical, llm_narrative="")
    assert score.no_op is False
    assert "heavy_decimal_arithmetic" in score.satisfied_invariants
    assert score.violations == []
    assert score.score == 1.0


def test_decimal_invariant_violated_when_no_type_review_anywhere():
    """V6 retention failure: agg expression has arithmetic, but neither
    canonical Action nor narrative mentions DECIMAL/型/DESCRIBE TABLE."""
    evidence = ProfileEvidence(
        decimal_arithmetic_in_heavy_agg=True,
        decimal_arithmetic_examples=[("1", "SUM(qty * price)")],
    )
    canonical = {
        "findings": [
            {
                "actions": [
                    {
                        "what": "Add CLUSTER BY on shuffle key.",
                        # fix_sql populated so the new structural
                        # ``fix_sql_required_for_sql_actions`` invariant
                        # stays satisfied — this test is about the
                        # decimal invariant in isolation.
                        "fix_sql": "ALTER TABLE foo CLUSTER BY (shuffle_key);",
                    }
                ]
            }
        ]
    }
    score = score_invariants(evidence, canonical, llm_narrative="Shuffle is heavy.")
    assert "heavy_decimal_arithmetic" in score.fired_invariants
    assert "heavy_decimal_arithmetic" not in score.satisfied_invariants
    decimal_viols = [
        v for v in score.violations if v.invariant_id == "heavy_decimal_arithmetic"
    ]
    assert len(decimal_viols) == 1
    # 1 decimal violation × 0.25 penalty → 0.75
    assert abs(score.score - 0.75) < 0.001
    # Evidence quoted in violation
    assert any("SUM(qty * price)" in e for e in decimal_viols[0].evidence)


def test_decimal_invariant_satisfied_by_llm_narrative_alone():
    evidence = ProfileEvidence(decimal_arithmetic_in_heavy_agg=True)
    canonical = {}
    score = score_invariants(
        evidence,
        canonical,
        llm_narrative="The DECIMAL columns may need DESCRIBE TABLE review.",
    )
    assert "heavy_decimal_arithmetic" in score.satisfied_invariants


# ---- dominant_shuffle_outside_lc invariant ----


def test_clustering_invariant_satisfied_by_cluster_by_mention():
    evidence = ProfileEvidence(
        dominant_shuffle_keys_outside_lc=True,
        dominant_shuffle_outside_lc_columns=[("cat.sch.t", "ss_customer_sk")],
    )
    canonical = {
        "findings": [
            {"actions": [{"fix_sql": "ALTER TABLE t CLUSTER BY (a, b);"}]}
        ]
    }
    score = score_invariants(evidence, canonical, "")
    assert "dominant_shuffle_outside_lc" in score.satisfied_invariants


def test_clustering_invariant_satisfied_by_zorder_alternative():
    """ZORDER is in the clustering family — alternative remedy is OK."""
    evidence = ProfileEvidence(dominant_shuffle_keys_outside_lc=True)
    canonical = {
        "findings": [{"actions": [{"what": "Apply ZORDER on the join key."}]}]
    }
    score = score_invariants(evidence, canonical, "")
    assert "dominant_shuffle_outside_lc" in score.satisfied_invariants


def test_clustering_invariant_violated_when_only_decimal_mentioned():
    evidence = ProfileEvidence(dominant_shuffle_keys_outside_lc=True)
    canonical = {
        "findings": [{"actions": [{"what": "Migrate DECIMAL to BIGINT."}]}]
    }
    score = score_invariants(evidence, canonical, "")
    assert "dominant_shuffle_outside_lc" not in score.satisfied_invariants


# ---- CTE materialization invariant ----


def test_cte_invariant_satisfied_by_ctas_or_reused_exchange():
    evidence = ProfileEvidence(
        cte_multi_reference=True,
        cte_multi_reference_names=[("frequent_items", 3)],
    )
    score1 = score_invariants(evidence, {}, "Consider CTAS for the shared CTE.")
    assert "cte_recompute_materialization" in score1.satisfied_invariants

    score2 = score_invariants(
        evidence, {}, "Verify ReusedExchange in the EXPLAIN output."
    )
    assert "cte_recompute_materialization" in score2.satisfied_invariants


# ---- Spill invariant ----


def test_spill_invariant_satisfied_by_warehouse_or_filter_early():
    evidence = ProfileEvidence(
        spill_dominant=True, spill_total_bytes=200 * (1024**3)
    )
    # Warehouse family
    score1 = score_invariants(evidence, {}, "Scale up the warehouse.")
    assert "spill_heavy_warehouse_review" in score1.satisfied_invariants
    # Filter Early family
    score2 = score_invariants(evidence, {}, "Apply Filter Early on date_year.")
    assert "spill_heavy_warehouse_review" in score2.satisfied_invariants
    # Clustering family
    score3 = score_invariants(evidence, {}, "ALTER TABLE t CLUSTER BY ...")
    assert "spill_heavy_warehouse_review" in score3.satisfied_invariants


# ---- Combined ----


def test_multiple_violations_compound_penalty():
    evidence = ProfileEvidence(
        decimal_arithmetic_in_heavy_agg=True,
        dominant_shuffle_keys_outside_lc=True,
    )
    score = score_invariants(evidence, {}, llm_narrative="No useful content.")
    assert len(score.violations) == 2
    # 2 × 0.25 = 0.5 → score 0.5
    assert abs(score.score - 0.5) < 0.001


def test_evidence_text_quoted_in_violation():
    evidence = ProfileEvidence(
        dominant_shuffle_keys_outside_lc=True,
        dominant_shuffle_outside_lc_columns=[
            ("cat.s.t1", "col_a"),
            ("cat.s.t2", "col_b"),
        ],
    )
    score = score_invariants(evidence, {}, "nothing helpful")
    v = score.violations[0]
    assert "cat.s.t1.col_a" in v.evidence
    assert "cat.s.t2.col_b" in v.evidence


# ---- Aggregation ----


def test_aggregate_skips_no_op():
    scores = [
        InvariantsScore(no_op=True),
        InvariantsScore(score=0.5, fired_invariants=["x"]),
        InvariantsScore(score=1.0, fired_invariants=["y"]),
    ]
    assert aggregate_invariants(scores) == 0.75


def test_aggregate_returns_one_when_all_no_op():
    assert aggregate_invariants([InvariantsScore(no_op=True)]) == 1.0
    assert aggregate_invariants([]) == 1.0
