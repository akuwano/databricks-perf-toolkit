"""Tests for Evidence layer functionality.

TDD: Red -> Green -> Refactor
"""

import json

import pytest


class TestEvidenceDataModels:
    """Tests for Evidence data models."""

    def test_evidence_locator_creation(self):
        """Test EvidenceLocator can be created with path and anchors."""
        from core.evidence import EvidenceLocator

        locator = EvidenceLocator(
            path="graphs[0].nodes[12]",
            anchors={"nodeId": "12", "nodeName": "SortMergeJoin"},
        )

        assert locator.path == "graphs[0].nodes[12]"
        assert locator.path_kind == "jsonpath-ish"
        assert locator.anchors["nodeId"] == "12"

    def test_evidence_snippet_creation(self):
        """Test EvidenceSnippet can be created with content."""
        from core.evidence import EvidenceSnippet

        snippet = EvidenceSnippet(
            format="json",
            content='{"id": "12", "name": "SortMergeJoin"}',
        )

        assert snippet.format == "json"
        assert "SortMergeJoin" in snippet.content

    def test_evidence_item_creation(self):
        """Test EvidenceItem can be created with all fields."""
        from core.evidence import EvidenceItem, EvidenceLocator, EvidenceSnippet

        item = EvidenceItem(
            id="ev_spill_node_12",
            category="spill",
            title="Spill Operator: SortMergeJoin",
            locator=EvidenceLocator(path="graphs[0].nodes[12]"),
            snippet=EvidenceSnippet(content='{"spill": "8GB"}'),
            why_selected="spill_bytes=8.1GB (share 35%)",
            score=0.92,
        )

        assert item.id == "ev_spill_node_12"
        assert item.category == "spill"
        assert item.score == 0.92

    def test_evidence_bundle_creation(self):
        """Test EvidenceBundle can be created and indexed."""
        from core.evidence import (
            EvidenceBudget,
            EvidenceBundle,
            EvidenceItem,
            EvidenceSource,
        )

        bundle = EvidenceBundle(
            source=EvidenceSource(query_id="query-123"),
            items=[
                EvidenceItem(id="ev_1", category="spill", score=0.9),
                EvidenceItem(id="ev_2", category="hot_node", score=0.8),
                EvidenceItem(id="ev_3", category="spill", score=0.7),
            ],
            budgets=EvidenceBudget(max_items=20),
        )
        # Build index
        bundle.index = {"spill": [0, 2], "hot_node": [1]}

        assert len(bundle.items) == 3
        assert bundle.source.query_id == "query-123"
        assert bundle.get_by_category("spill") == [bundle.items[0], bundle.items[2]]

    def test_evidence_budget_defaults(self):
        """Test EvidenceBudget has sensible defaults."""
        from core.evidence import EvidenceBudget

        budget = EvidenceBudget()

        assert budget.max_items == 20
        assert budget.max_chars_total == 15000
        assert budget.max_chars_per_item == 1000


class TestBuildEvidence:
    """Tests for build_evidence function."""

    @pytest.fixture
    def sample_raw_profile(self):
        """Create a sample raw profile JSON."""
        return {
            "query": {
                "id": "query-123",
                "queryText": "SELECT * FROM table1",
                "metrics": {
                    "totalTimeMs": 10000,
                    "spillToDiskBytes": 8000000000,  # 8GB
                },
            },
            "graphs": [
                {
                    "nodes": [
                        {
                            "id": "node-1",
                            "name": "Scan table1",
                            "tag": "scan",
                            "keyMetrics": {
                                "durationMs": 5000,
                                "peakMemoryBytes": 1000000000,
                                "rowsNum": 1000000,
                            },
                            "metrics": [
                                {"label": "Files read", "value": "100"},
                                {"label": "Size of files read", "value": "5GB"},
                            ],
                            "metadata": [
                                {"key": "IS_PHOTON", "value": "true"},
                            ],
                        },
                        {
                            "id": "node-2",
                            "name": "SortMergeJoin",
                            "tag": "join",
                            "keyMetrics": {
                                "durationMs": 3000,
                                "peakMemoryBytes": 2000000000,
                                "rowsNum": 500000,
                            },
                            "metrics": [
                                {"label": "Spilled to disk size", "value": "8GB"},
                            ],
                            "metadata": [
                                {"key": "JOIN_ALGORITHM", "value": "SortMerge"},
                            ],
                        },
                    ]
                }
            ],
        }

    @pytest.fixture
    def sample_analysis(self, sample_raw_profile):
        """Create a sample ProfileAnalysis."""
        from core.constants import JoinType
        from core.models import (
            JoinInfo,
            NodeMetrics,
            ProfileAnalysis,
            QueryMetrics,
            ShuffleMetrics,
        )

        return ProfileAnalysis(
            query_metrics=QueryMetrics(
                query_id="query-123",
                total_time_ms=10000,
                spill_to_disk_bytes=8000000000,
            ),
            node_metrics=[
                NodeMetrics(
                    node_id="node-1",
                    node_name="Scan table1",
                    node_tag="scan",
                    duration_ms=5000,
                    peak_memory_bytes=1000000000,
                    rows_num=1000000,
                    files_read_size=5000000000,
                    files_read=100,
                ),
                NodeMetrics(
                    node_id="node-2",
                    node_name="SortMergeJoin",
                    node_tag="join",
                    duration_ms=3000,
                    peak_memory_bytes=2000000000,
                    rows_num=500000,
                    spill_bytes=8000000000,
                ),
            ],
            shuffle_metrics=[
                ShuffleMetrics(
                    node_id="node-2",
                    node_name="SortMergeJoin",
                    partition_count=2,  # Small to get high memory_per_partition_mb
                    peak_memory_bytes=2000000000,  # 2GB / 2 partitions = ~953MB/partition
                ),
            ],
            join_info=[
                JoinInfo(
                    node_name="SortMergeJoin",
                    join_type=JoinType.SORT_MERGE,
                    duration_ms=3000,
                ),
            ],
        )

    def test_build_evidence_returns_bundle(self, sample_analysis, sample_raw_profile):
        """Test build_evidence returns an EvidenceBundle."""
        from core.evidence import EvidenceBundle, build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        assert isinstance(bundle, EvidenceBundle)
        assert bundle.source.query_id == "query-123"

    def test_build_evidence_extracts_hot_nodes(self, sample_analysis, sample_raw_profile):
        """Test build_evidence extracts hot nodes by duration."""
        from core.evidence import build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        hot_nodes = bundle.get_by_category("hot_node")
        assert len(hot_nodes) > 0
        # First node should be the one with highest duration
        assert "node-1" in hot_nodes[0].id or "Scan" in hot_nodes[0].title

    def test_build_evidence_extracts_spill_nodes(self, sample_analysis, sample_raw_profile):
        """Test build_evidence extracts nodes with spill."""
        from core.evidence import build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        spill_items = bundle.get_by_category("spill")
        assert len(spill_items) > 0
        # Should include the SortMergeJoin with spill
        assert any("node-2" in item.id for item in spill_items)

    def test_build_evidence_extracts_join_info(self, sample_analysis, sample_raw_profile):
        """Test build_evidence extracts join information."""
        from core.evidence import build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        join_items = bundle.get_by_category("join")
        assert len(join_items) > 0
        assert any("SORT_MERGE" in item.title for item in join_items)

    def test_build_evidence_respects_budget(self, sample_analysis, sample_raw_profile):
        """Test build_evidence respects max_items budget."""
        from core.evidence import EvidenceBudget, build_evidence

        budget = EvidenceBudget(max_items=2)
        bundle = build_evidence(sample_analysis, sample_raw_profile, budget)

        assert len(bundle.items) <= 2

    def test_build_evidence_includes_locator(self, sample_analysis, sample_raw_profile):
        """Test each evidence item has a valid locator."""
        from core.evidence import build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        for item in bundle.items:
            assert item.locator.path != ""
            assert "graphs[" in item.locator.path

    def test_build_evidence_includes_snippet(self, sample_analysis, sample_raw_profile):
        """Test each evidence item has a snippet."""
        from core.evidence import build_evidence

        bundle = build_evidence(sample_analysis, sample_raw_profile)

        for item in bundle.items:
            assert item.snippet.content != ""
            # Should be valid JSON
            json.loads(item.snippet.content)


class TestFormatEvidenceForPrompt:
    """Tests for format_evidence_for_prompt function."""

    def test_format_evidence_empty_bundle(self):
        """Test formatting empty bundle returns empty string."""
        from core.evidence import EvidenceBundle, format_evidence_for_prompt

        bundle = EvidenceBundle()
        result = format_evidence_for_prompt(bundle)

        assert result == ""

    def test_format_evidence_includes_header(self):
        """Test formatting includes Evidence header."""
        from core.evidence import (
            EvidenceBundle,
            EvidenceItem,
            EvidenceLocator,
            EvidenceSnippet,
            format_evidence_for_prompt,
        )

        bundle = EvidenceBundle(
            items=[
                EvidenceItem(
                    id="ev_1",
                    category="spill",
                    title="Spill Node",
                    locator=EvidenceLocator(path="graphs[0].nodes[0]"),
                    snippet=EvidenceSnippet(content='{"test": true}'),
                    why_selected="test reason",
                    score=0.9,
                )
            ]
        )
        result = format_evidence_for_prompt(bundle)

        assert "## Evidence" in result
        assert "ev_1" in result
        assert "graphs[0].nodes[0]" in result

    def test_format_evidence_japanese(self):
        """Test formatting in Japanese."""
        from core.evidence import (
            EvidenceBundle,
            EvidenceItem,
            EvidenceLocator,
            EvidenceSnippet,
            format_evidence_for_prompt,
        )

        bundle = EvidenceBundle(
            items=[
                EvidenceItem(
                    id="ev_1",
                    category="spill",
                    title="Spill Node",
                    locator=EvidenceLocator(path="graphs[0].nodes[0]"),
                    snippet=EvidenceSnippet(content="{}"),
                    score=0.9,
                )
            ]
        )
        result = format_evidence_for_prompt(bundle, lang="ja")

        assert "プロファイルJSON" in result


class TestEvidenceIntegration:
    """Tests for Evidence integration with analyzers and LLM."""

    @pytest.fixture
    def sample_raw_profile(self):
        """Create a sample raw profile JSON."""
        return {
            "query": {
                "id": "query-123",
                "queryText": "SELECT * FROM table1",
                "metrics": {
                    "totalTimeMs": 10000,
                    "spillToDiskBytes": 8000000000,
                },
            },
            "graphs": [
                {
                    "nodes": [
                        {
                            "id": "node-1",
                            "name": "Scan table1",
                            "tag": "scan",
                            "keyMetrics": {
                                "durationMs": 5000,
                                "peakMemoryBytes": 1000000000,
                                "rowsNum": 1000000,
                            },
                            "metrics": [
                                {"label": "Files read", "value": "100"},
                                {"label": "Size of files read", "value": "5GB"},
                            ],
                            "metadata": [
                                {"key": "IS_PHOTON", "value": "true"},
                            ],
                        },
                    ]
                }
            ],
        }

    def test_profile_analysis_has_evidence_bundle_field(self):
        """Test ProfileAnalysis model has evidence_bundle field."""
        from core.evidence import EvidenceBundle
        from core.models import ProfileAnalysis

        analysis = ProfileAnalysis()
        assert hasattr(analysis, "evidence_bundle")
        assert analysis.evidence_bundle is None or isinstance(
            analysis.evidence_bundle, EvidenceBundle
        )

    def test_analyze_from_dict_builds_evidence(self, sample_raw_profile):
        """Test analyze_from_dict populates evidence_bundle."""
        from core.analyzers import analyze_from_dict
        from core.evidence import EvidenceBundle

        analysis = analyze_from_dict(sample_raw_profile)

        assert analysis.evidence_bundle is not None
        assert isinstance(analysis.evidence_bundle, EvidenceBundle)
        assert analysis.evidence_bundle.source.query_id == "query-123"

    def test_create_analysis_prompt_includes_evidence(self, sample_raw_profile):
        """Test create_analysis_prompt includes evidence section."""
        from core.analyzers import analyze_from_dict
        from core.llm import create_analysis_prompt

        analysis = analyze_from_dict(sample_raw_profile)
        prompt = create_analysis_prompt(analysis)

        # Prompt should include evidence section
        assert "## Evidence" in prompt or "Evidence" in prompt
