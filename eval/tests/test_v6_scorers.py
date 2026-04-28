"""Tests for V6 Week 1 mechanical scorers (Q4 / hallucination / recall)."""

from __future__ import annotations

from types import SimpleNamespace

from eval.scorers.actionability import (
    aggregate_actionability,
    score_actionability,
)
from eval.scorers.hallucination import (
    aggregate_hallucination,
    score_hallucination,
)
from eval.scorers.recall import score_recall


def _make_card(**kw):
    defaults = dict(
        card_index=0,
        problem="",
        fix_sql="",
        rationale="",
        expected_impact="",
        verification="",
        evidence="",
    )
    defaults.update(kw)
    return SimpleNamespace(**defaults)


# ----- actionability -----


def test_actionability_specific_card():
    card = _make_card(
        problem="fact_orders.customer_id 列で型不一致が発生",
        fix_sql="ALTER TABLE fact_orders ALTER COLUMN customer_id TYPE BIGINT;",
        rationale="join key の暗黙 CAST により Photon 化を阻害しているため",
        expected_impact="実行時間 30% 短縮見込み",
        verification="EXPLAIN で CAST が消えていることを確認",
    )
    s = score_actionability(card)
    assert s.has_target
    assert s.has_what
    assert s.has_why
    assert s.has_how
    assert s.has_expected_effect
    assert s.has_verification
    assert s.is_specific


def test_actionability_abstract_card():
    card = _make_card(problem="クラスタリングを検討してください")
    s = score_actionability(card)
    assert not s.is_specific


def test_aggregate_actionability_ratio():
    specific = score_actionability(_make_card(
        problem="fact.col1 列を変更",
        fix_sql="ALTER TABLE fact ALTER COLUMN col1 TYPE BIGINT",
        rationale="cast を避けるため",
        expected_impact="30% 改善",
        verification="EXPLAIN で確認",
    ))
    abstract = score_actionability(_make_card(problem="検討してください"))
    assert specific.is_specific
    assert not abstract.is_specific
    assert aggregate_actionability([specific, abstract]) == 0.5


# ----- hallucination -----


def test_hallucination_clean():
    card = _make_card(problem="spill が発生", fix_sql="SET spark.sql.shuffle.partitions=400;")
    s = score_hallucination(card, forbidden_claims=[
        {"id": "federation_recommendation", "description": "no federation"}
    ])
    assert s.score == 1.0
    assert s.forbidden_claim_hits == []


def test_hallucination_forbidden_hit():
    card = _make_card(problem="federation を使った最適化を推奨します")
    s = score_hallucination(card, forbidden_claims=[
        {"id": "federation_recommendation", "description": "no federation"}
    ])
    # "federation" keyword should match
    assert "federation_recommendation" in s.forbidden_claim_hits
    assert s.score < 1.0


def test_aggregate_hallucination_avg():
    s1 = score_hallucination(_make_card(), forbidden_claims=[])
    s2 = score_hallucination(
        _make_card(problem="federation 推奨"),
        forbidden_claims=[{"id": "federation_recommendation", "description": ""}],
    )
    avg = aggregate_hallucination([s1, s2])
    assert 0.5 < avg < 1.0


# ----- recall -----


def test_recall_full_coverage():
    must_cover = [
        {"id": "spill", "keywords": ["spill", "メモリ"]},
        {"id": "shuffle", "keywords": ["shuffle"]},
    ]
    text = "spill が観測され shuffle が支配的"
    s = score_recall(text, [], must_cover)
    assert s.recall_ratio == 1.0
    assert s.missed_issues == []


def test_recall_partial_coverage():
    must_cover = [
        {"id": "spill", "keywords": ["spill"]},
        {"id": "skew", "keywords": ["skew", "偏り"]},
    ]
    s = score_recall("spill のみ言及", [], must_cover)
    assert s.recall_ratio == 0.5
    assert "skew" in s.missed_issues


def test_recall_empty_must_cover_returns_one():
    s = score_recall("anything", [], [])
    assert s.recall_ratio == 1.0
    assert s.must_cover_count == 0


def test_recall_uses_card_text():
    must_cover = [{"id": "spill", "keywords": ["spill"]}]
    card = _make_card(problem="spill が観測")
    s = score_recall("", [card], must_cover)
    assert s.recall_ratio == 1.0


# ----- W2.5 #6: canonical recall -----


def test_recall_via_canonical_findings():
    """Finding.issue_id 直接ヒットで covered と判定."""
    canonical = {"findings": [{"issue_id": "spill_dominant"}]}
    must_cover = [{"id": "spill_dominant"}]  # no keywords given
    s = score_recall("", [], must_cover, canonical_report=canonical)
    assert s.recall_ratio == 1.0
    assert s.missed_issues == []


def test_recall_pulls_keywords_from_registry():
    """yaml に keywords がなくても registry から補完."""
    must_cover = [{"id": "spill_dominant"}]
    s = score_recall("ディスクスピル発生", [], must_cover, canonical_report=None)
    assert s.recall_ratio == 1.0


def test_strict_recall_misses_when_only_text():
    """strict 版は Finding.issue_id 一致のみカウント."""
    from eval.scorers.recall import score_canonical_recall

    canonical = {"findings": [{"issue_id": "shuffle_dominant"}]}
    must_cover = [{"id": "spill_dominant"}, {"id": "shuffle_dominant"}]
    s = score_canonical_recall(canonical, must_cover)
    assert s.recall_ratio == 0.5
    assert "spill_dominant" in s.missed_issues


# ----- canonical Action direct (W2.5 #2) -----


def test_canonical_action_specific():
    from eval.scorers.actionability import score_canonical_action

    a = {
        "action_id": "increase_warehouse",
        "target": "warehouse_size",
        "fix_type": "configuration",
        "what": "Upsize from L to XL",
        "why": "メモリ不足のため",
        "fix_sql": "SET spark.databricks.photon.enabled=true",
        "expected_effect": "30% 短縮",
        "verification": [{"type": "metric", "metric": "spill_bytes", "expected": "0"}],
    }
    s = score_canonical_action(a)
    assert s.has_target
    assert s.has_what
    assert s.has_why
    assert s.has_how
    assert s.has_expected_effect
    assert s.has_verification
    assert s.is_specific


def test_canonical_action_partial_not_specific():
    from eval.scorers.actionability import score_canonical_action

    a = {
        "action_id": "x",
        "target": "tbl",
        "fix_type": "investigation",
        "what": "確認する",
    }
    s = score_canonical_action(a)
    # 4/6 (target/what/how/no fix_sql but investigation+what counts as how)
    assert not s.is_specific


def test_canonical_report_hallucination_clean():
    from eval.scorers.hallucination import score_canonical_report_hallucination

    report = {
        "summary": {"headline": "Spill detected", "key_metrics": [{"value_display": "12 GB"}]},
        "findings": [{
            "title": "Spill",
            "description": "peak_memory が 12 GB",
            "evidence": [{"metric": "peak_memory_bytes", "value_display": "12 GB", "grounded": True}],
            "actions": [],
        }],
    }
    s = score_canonical_report_hallucination(report, forbidden_claims=[])
    assert s.score == 1.0
    assert s.forbidden_claim_hits == []


def test_canonical_report_hallucination_forbidden_hit():
    from eval.scorers.hallucination import score_canonical_report_hallucination

    report = {
        "summary": {"headline": "federation 推奨を提示", "key_metrics": []},
        "findings": [{
            "title": "x",
            "evidence": [{"metric": "m", "value_display": "1", "grounded": True}],
            "actions": [],
        }],
    }
    s = score_canonical_report_hallucination(
        report,
        forbidden_claims=[{"id": "federation_recommendation", "description": ""}],
    )
    assert "federation_recommendation" in s.forbidden_claim_hits
    assert s.score < 1.0


def test_canonical_report_hallucination_ungrounded_numeric():
    from eval.scorers.hallucination import score_canonical_report_hallucination

    report = {
        "summary": {"headline": "解決します", "key_metrics": []},  # no grounded numbers
        "findings": [{
            "title": "Issue",
            "evidence": [{"metric": "x", "value_display": "abc", "grounded": True}],
            "actions": [{
                "what": "30% 短縮",  # ungrounded numeric claim
                "expected_effect": "60秒",
                "fix_sql": "",
            }],
        }],
    }
    s = score_canonical_report_hallucination(report, forbidden_claims=[])
    # 30% and 60秒 are not in profile_numeric_displays
    assert len(s.unsupported_value_claims) >= 1
    assert s.score < 1.0


def test_canonical_report_hallucination_grounded_ratio_penalty():
    from eval.scorers.hallucination import score_canonical_report_hallucination

    report = {
        "summary": {"headline": "x", "key_metrics": []},
        "findings": [{
            "title": "Issue",
            "evidence": [
                {"metric": "a", "value_display": "1", "grounded": False},
                {"metric": "b", "value_display": "2", "grounded": False},
            ],
            "actions": [],
        }],
    }
    s = score_canonical_report_hallucination(report, forbidden_claims=[])
    # 0/2 grounded -> 0.3 penalty
    assert s.score <= 0.7 + 1e-6


def test_canonical_action_citation_lenient():
    """W5 Day 4: skeleton present + no profile identifiers → citation True."""
    from eval.scorers.actionability import score_canonical_action

    a = {
        "action_id": "a", "target": "t", "fix_type": "configuration", "what": "do",
        "fix_sql": "SET x=1",
        "fix_sql_skeleton": "SET x=1",
        "fix_sql_skeleton_method": "fullsql",
    }
    s = score_canonical_action(a)
    assert s.has_citation is True


def test_canonical_action_citation_strict_match():
    """W5 Day 4: skeleton must contain a known profile identifier."""
    from eval.scorers.actionability import score_canonical_action

    a = {
        "action_id": "a", "target": "t", "fix_type": "ddl",
        "what": "alter",
        "fix_sql_skeleton": "SELECT <3 cols>\nFROM fact_orders\nINNER JOIN dim_customer ON [eq]",
        "fix_sql_skeleton_method": "sqlglot",
    }
    s_known = score_canonical_action(
        a, profile_known_identifiers={"fact_orders", "dim_customer"}
    )
    assert s_known.has_citation is True

    s_unknown = score_canonical_action(
        a, profile_known_identifiers={"unrelated_table"}
    )
    assert s_unknown.has_citation is False


def test_canonical_action_citation_false_no_skeleton():
    from eval.scorers.actionability import score_canonical_action

    a = {"action_id": "a", "target": "t", "fix_type": "investigation", "what": "investigate"}
    s = score_canonical_action(a)
    assert s.has_citation is False


def test_canonical_action_threshold_now_6_of_7():
    """W5: is_specific requires 6 of 7 dimensions (was 5 of 6)."""
    from eval.scorers.actionability import score_canonical_action

    # 5 dims (target/what/why/how/expected_effect) — no verification, no citation
    a = {
        "action_id": "x", "target": "t", "fix_type": "configuration",
        "what": "change", "why": "because", "fix_sql": "SET x=1",
        "expected_effect": "30% better",
    }
    s = score_canonical_action(a)  # no profile ids, no skeleton fields → citation=False
    # 5/7 → not specific in W5 (was specific in W1 at 5/6)
    assert not s.is_specific


def test_canonical_report_actions_aggregation():
    from eval.scorers.actionability import (
        aggregate_actionability,
        score_canonical_report_actions,
    )

    report = {
        "findings": [
            {
                "actions": [
                    {
                        "action_id": "a1",
                        "target": "t",
                        "fix_type": "configuration",
                        "what": "what",
                        "why": "why",
                        "fix_sql": "SET x=1",
                        "expected_effect": "fast",
                        "verification": [{"type": "metric", "metric": "m", "expected": "ok"}],
                    },
                    {"action_id": "a2", "target": "t2", "fix_type": "investigation", "what": "check"},
                ]
            }
        ]
    }
    scores = score_canonical_report_actions(report)
    assert len(scores) == 2
    assert aggregate_actionability(scores) == 0.5
