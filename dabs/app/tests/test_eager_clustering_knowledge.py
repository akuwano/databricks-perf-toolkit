"""Eager Clustering disable workaround — knowledge + alert coverage.

When our LC ClusterOnWrite alert fires, the recommended mitigations
must include disabling eager clustering via
``delta.liquid.forceDisableEagerClustering=True`` as a less destructive
alternative to dropping CLUSTER BY entirely.
"""

from pathlib import Path

import pytest
from core.analyzers.explain_analysis import detect_lc_cluster_on_write_overhead
from core.models import BottleneckIndicators, ShuffleMetrics, TargetTableInfo

KNOWLEDGE_DIR = Path(__file__).resolve().parent.parent / "core" / "knowledge"


class TestKnowledgeContainsEagerClusteringDisable:
    """Knowledge files must document the force-disable eager clustering
    workaround so the LLM has canonical text to cite."""

    @pytest.mark.parametrize(
        "path",
        ["dbsql_tuning.md", "dbsql_tuning_en.md"],
    )
    def test_property_name_present(self, path):
        content = (KNOWLEDGE_DIR / path).read_text(encoding="utf-8")
        assert "forceDisableEagerClustering" in content, (
            f"{path} must document the delta.liquid.forceDisableEagerClustering property"
        )

    @pytest.mark.parametrize(
        "path",
        ["dbsql_tuning.md", "dbsql_tuning_en.md"],
    )
    def test_section_id_registered(self, path):
        content = (KNOWLEDGE_DIR / path).read_text(encoding="utf-8")
        assert "section_id: eager_clustering_disable" in content

    @pytest.mark.parametrize(
        "path",
        ["dbsql_tuning.md", "dbsql_tuning_en.md"],
    )
    def test_mentions_optimize_full_after(self, path):
        """Documentation must explain the follow-up OPTIMIZE FULL step."""
        content = (KNOWLEDGE_DIR / path).read_text(encoding="utf-8")
        assert "OPTIMIZE" in content
        # The property's section must appear in close proximity to OPTIMIZE FULL
        prop_idx = content.find("forceDisableEagerClustering")
        optimize_idx = content.find("OPTIMIZE", prop_idx)
        assert optimize_idx != -1, (
            f"OPTIMIZE FULL must be mentioned after the property intro in {path}"
        )


class TestAlertRecommendationIncludesEagerClusteringOption:
    """The LC ClusterOnWrite alert fires when Delta + LC + spill. Its
    recommendation text must include the eager-clustering-disable option."""

    def test_recommendation_mentions_force_disable(self):
        ind = BottleneckIndicators()
        sm = [
            ShuffleMetrics(
                node_id="1",
                partition_count=3239,
                sink_bytes_written=1_000_000_000_000,
                peak_memory_bytes=10_000_000_000_000,
                sink_num_spills=100,
            )
        ]
        target = TargetTableInfo(
            provider="delta",
            clustering_columns=[["COL1"]],
        )
        detect_lc_cluster_on_write_overhead(ind, sm, target)
        rec = next(
            a.recommendation for a in ind.alerts if a.metric_name == "lc_cluster_on_write_spill"
        )
        assert "forceDisableEagerClustering" in rec, (
            "Alert recommendation must cite the eager-clustering-disable workaround"
        )
