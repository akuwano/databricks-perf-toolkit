"""Tests for L2 evidence grounding scorer."""

from core.models import ActionCard, BottleneckIndicators, ProfileAnalysis, QueryMetrics
from eval.scorers.l2_evidence import score_l2


def _make_analysis(**qm_kwargs) -> ProfileAnalysis:
    return ProfileAnalysis(
        query_metrics=QueryMetrics(**qm_kwargs),
        bottleneck_indicators=BottleneckIndicators(),
    )


def _make_profile(metrics: dict | None = None, nodes: list | None = None) -> dict:
    return {
        "query": {"metrics": metrics or {}},
        "graphs": [{"nodes": nodes or []}],
    }


class TestL2EmptyEvidence:
    def test_no_evidence_perfect_score(self):
        card = ActionCard(evidence=[])
        score = score_l2(card, _make_profile(), _make_analysis())
        assert score.evidence_count == 0
        assert score.grounding_ratio == 1.0


class TestL2GroundedEvidence:
    def test_metric_name_match(self):
        card = ActionCard(evidence=["spill_to_disk_bytes = 5.1 GB"])
        profile = _make_profile(metrics={"spillToDiskBytes": 5474836480})
        analysis = _make_analysis(spill_to_disk_bytes=5474836480)
        score = score_l2(card, profile, analysis)
        assert score.grounding_ratio == 1.0
        assert score.grounded_count == 1

    def test_node_name_match(self):
        card = ActionCard(evidence=["SortMergeJoin is slow"])
        profile = _make_profile(nodes=[{"name": "SortMergeJoin", "metrics": []}])
        analysis = _make_analysis()
        score = score_l2(card, profile, analysis)
        assert score.grounding_ratio == 1.0

    def test_camel_case_metric_match(self):
        card = ActionCard(evidence=["readBytes is high: 10 GB"])
        profile = _make_profile(metrics={"readBytes": 10737418240})
        analysis = _make_analysis(read_bytes=10737418240)
        score = score_l2(card, profile, analysis)
        assert score.grounding_ratio == 1.0

    def test_numeric_value_match(self):
        card = ActionCard(evidence=["Files read: 1500"])
        profile = _make_profile(metrics={})
        analysis = _make_analysis(read_files_count=1500)
        score = score_l2(card, profile, analysis)
        assert score.grounding_ratio == 1.0


class TestL2UngroundedEvidence:
    def test_fabricated_metric(self):
        card = ActionCard(evidence=["nonexistent_metric = 999"])
        score = score_l2(card, _make_profile(), _make_analysis())
        assert score.grounding_ratio == 0.0
        assert score.ungrounded_evidence == ["nonexistent_metric = 999"]


class TestL2MixedEvidence:
    def test_partial_grounding(self):
        card = ActionCard(evidence=[
            "spill_to_disk_bytes is high",
            "fabricated_metric = 42",
        ])
        analysis = _make_analysis(spill_to_disk_bytes=5000000000)
        score = score_l2(card, _make_profile(), analysis)
        assert score.evidence_count == 2
        assert score.grounded_count == 1
        assert score.grounding_ratio == 0.5
