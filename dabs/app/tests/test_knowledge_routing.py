"""Tests for knowledge section parsing, routing, and review JSON parsing in llm.py."""

import json

import pytest
from core.constants import Severity
from core.llm import (
    ALWAYS_INCLUDE_SECTIONS,
    CATEGORY_TO_KNOWLEDGE_SECTIONS,
    filter_knowledge_by_alerts,
    format_review_for_refine,
    get_knowledge_section_refs,
    parse_knowledge_sections,
    parse_review_json,
)
from core.models import Alert

# --- Fixtures ---

SAMPLE_KNOWLEDGE = """\
# DBSQL Tuning Guide

## 概要
<!-- section_id: overview -->

This is the overview section.

## 1. I/Oの効率化
<!-- section_id: io -->

I/O optimization content here.
- Use file pruning
- Optimize reads

## 2. 実行プランの改善
<!-- section_id: execution_plan -->

Execution plan improvement content.
### 2.1 Join strategies
Use broadcast joins for small tables.

## 3. Shuffle最適化
<!-- section_id: shuffle -->

Shuffle optimization content.
Set spark.sql.shuffle.partitions = 400.

## 4. スピル（Disk Spill）の検出と対策
<!-- section_id: spill -->

Spill detection content.

## 5. Photon利用率の改善
<!-- section_id: photon -->

Photon utilization content.
Photon-compatible functions list.

## 6. キャッシュ効率
<!-- section_id: cache -->

Cache efficiency content.

## 7. クラウドストレージ制限の拡張
<!-- section_id: cloud_storage -->

Cloud storage limits content.

## 8. クラスタサイズの調整
<!-- section_id: cluster -->

Cluster sizing content.

## 9. ボトルネック指標サマリー
<!-- section_id: bottleneck_summary -->

Bottleneck indicators summary.

## 10. 推奨Sparkパラメータまとめ
<!-- section_id: spark_params -->

Recommended Spark parameters.

## 付録: クエリ最適化ヒント
<!-- section_id: appendix -->

Query optimization hints.

## 11. SQL書き換えパターン
<!-- section_id: sql_patterns -->

SQL rewrite patterns content.

## 12. Photon OOMトラブルシューティング
<!-- section_id: photon_oom -->

Photon OOM troubleshooting content.

## 13. Serverless固有のチューニング
<!-- section_id: serverless -->

Serverless tuning content.

## 14. Shuffleパーティション詳細チューニング
<!-- section_id: shuffle_advanced -->

Shuffle advanced tuning content.

## 15. Data Explosionの検出と対策
<!-- section_id: data_explosion -->

Data explosion content.

## 16. Data Skewの検出と対策
<!-- section_id: skew_advanced -->

Skew advanced content.

## 17. ブロードキャストJOIN詳細設定
<!-- section_id: broadcast_advanced -->

Broadcast advanced content.

## 18. Delta MERGEパフォーマンス最適化
<!-- section_id: merge_advanced -->

Merge advanced content.

## 2A. ハッシュテーブルリサイズの多発要因
<!-- section_id: hash_resize_causes -->

Hash resize causes content.

## 3A. 支配的シャッフルキーを Liquid Clustering キー候補として検討する
<!-- section_id: lc_shuffle_key_candidate -->

Shuffle key as LC candidate guidance.

## 7A. Compilation / file pruning overhead
<!-- section_id: compilation_overhead -->

Compilation overhead content.

## 7B. Driver overhead (queue + scheduling + compute wait)
<!-- section_id: driver_overhead -->

Driver overhead content.

## 19. Lakehouse Federation query tuning
<!-- section_id: federation -->

Federation content.

## 参考リンク
<!-- section_id: references -->

- https://example.com
"""


def _make_alert(category: str, severity: Severity = Severity.HIGH) -> Alert:
    """Create a minimal Alert for testing."""
    return Alert(
        category=category,
        severity=severity,
        message=f"Test alert for {category}",
        current_value="test",
        threshold="test",
        recommendation=f"Fix {category}",
    )


# --- parse_knowledge_sections ---


class TestParseKnowledgeSections:
    def test_parses_all_content_sections(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        # Should have 24 sections (1-18 + appendix + hash_resize_causes
        # + lc_shuffle_key_candidate + compilation_overhead
        # + driver_overhead + federation), excluding overview and references
        assert len(sections) == 24

    def test_excludes_overview(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        assert "overview" not in sections

    def test_excludes_references(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        assert "references" not in sections

    def test_section_headings_are_keys(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        assert "io" in sections
        assert "photon" in sections
        assert "appendix" in sections

    def test_section_content_preserved(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        assert "I/O optimization content" in sections["io"]
        assert "Use broadcast joins" in sections["execution_plan"]

    def test_subsections_included_in_parent(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        # ### 2.1 should be part of section 2
        assert "### 2.1 Join strategies" in sections["execution_plan"]

    def test_preserves_section_order(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        keys = list(sections.keys())
        assert keys.index("io") < keys.index("execution_plan")
        assert keys.index("shuffle") < keys.index("spill")

    def test_empty_input(self):
        assert parse_knowledge_sections("") == {}

    def test_none_like_empty(self):
        assert parse_knowledge_sections("") == {}

    def test_no_sections(self):
        assert parse_knowledge_sections("Just plain text, no headers.") == {}


# --- filter_knowledge_by_alerts ---


class TestFilterKnowledgeByAlerts:
    @pytest.fixture
    def sections(self):
        return parse_knowledge_sections(SAMPLE_KNOWLEDGE)

    def test_cache_alerts_select_correct_sections(self, sections):
        alerts = [_make_alert("cache")]
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "I/Oの効率化" in result
        assert "キャッシュ効率" in result

    def test_spill_alerts_select_correct_sections(self, sections):
        alerts = [_make_alert("spill")]
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "スピル（Disk Spill）の検出と対策" in result
        assert "クラスタサイズの調整" in result

    def test_photon_alerts_select_correct_sections(self, sections):
        alerts = [_make_alert("photon")]
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "Photon利用率の改善" in result

    def test_always_includes_reference_sections(self, sections, monkeypatch):
        """Kill-switch path: V6_ALWAYS_INCLUDE_MINIMUM disabled → all 3
        legacy reference sections (bottleneck_summary + spark_params +
        appendix) are auto-included. V6 standard reduces this to just
        bottleneck_summary."""
        from core import feature_flags
        monkeypatch.setenv(feature_flags.V6_ALWAYS_INCLUDE_MINIMUM, "0")
        feature_flags.reset_cache()

        alerts = [_make_alert("photon")]  # Only photon
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "ボトルネック指標サマリー" in result
        assert "推奨Sparkパラメータまとめ" in result
        assert "クエリ最適化ヒント" in result

    def test_excludes_unrelated_sections(self, sections):
        alerts = [_make_alert("photon")]  # Only photon
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "Shuffle最適化" not in result
        assert "スピル（Disk Spill）" not in result

    def test_multiple_categories_union_sections(self, sections):
        alerts = [_make_alert("cache"), _make_alert("shuffle")]
        result = filter_knowledge_by_alerts(sections, alerts)
        assert "I/Oの効率化" in result
        assert "キャッシュ効率" in result
        assert "Shuffle最適化" in result

    def test_no_duplicate_sections(self, sections):
        # cache and io both map to section_id "io"
        alerts = [_make_alert("cache"), _make_alert("io")]
        result = filter_knowledge_by_alerts(sections, alerts)
        # Count occurrences of the section header
        assert result.count("## 1. I/Oの効率化") == 1

    def test_empty_alerts_returns_all_sections(self, sections):
        result = filter_knowledge_by_alerts(sections, [])
        # Should contain all sections
        assert "I/Oの効率化" in result
        assert "Shuffle最適化" in result
        assert "Photon利用率の改善" in result

    def test_empty_sections_returns_empty(self):
        result = filter_knowledge_by_alerts({}, [_make_alert("cache")])
        assert result == ""

    def test_unknown_category_still_includes_always_sections(self, sections):
        alerts = [_make_alert("unknown_category")]
        result = filter_knowledge_by_alerts(sections, alerts)
        # Should still include ALWAYS_INCLUDE_SECTIONS
        assert "ボトルネック指標サマリー" in result


# --- CATEGORY_TO_KNOWLEDGE_SECTIONS completeness ---


class TestCategoryMapping:
    EXPECTED_CATEGORIES = [
        "cache",
        "io",
        "photon",
        "spill",
        "shuffle",
        "join",
        "statistics",
        "cloud_storage",
        "cluster",
        "memory",
        "agg",
        "skew",
        "explosion",
        "serverless",
        "merge",
    ]

    def test_all_categories_have_mappings(self):
        for cat in self.EXPECTED_CATEGORIES:
            assert cat in CATEGORY_TO_KNOWLEDGE_SECTIONS, f"Missing mapping for category: {cat}"
            assert len(CATEGORY_TO_KNOWLEDGE_SECTIONS[cat]) > 0

    def test_mapped_sections_exist_in_knowledge(self):
        sections = parse_knowledge_sections(SAMPLE_KNOWLEDGE)
        section_keys = set(sections.keys())
        for cat, section_ids in CATEGORY_TO_KNOWLEDGE_SECTIONS.items():
            for sid in section_ids:
                assert sid in section_keys, (
                    f"Category '{cat}' maps to section_id '{sid}' which is not a valid section"
                )


# --- get_knowledge_section_refs ---


class TestGetKnowledgeSectionRefs:
    def test_spill_refs(self):
        result = get_knowledge_section_refs("spill")
        assert result == "(→ Section 4, 8, 14, 15)"

    def test_cache_refs(self):
        result = get_knowledge_section_refs("cache")
        assert result == "(→ Section 1, 6)"

    def test_photon_refs(self):
        result = get_knowledge_section_refs("photon")
        assert result == "(→ Section 5, 12)"

    def test_unknown_category(self):
        result = get_knowledge_section_refs("nonexistent")
        assert result == ""

    def test_appendix_section_no_number(self):
        # "appendix" has no number in _SECTION_ID_TO_NUMBER, so it won't appear in refs
        result = get_knowledge_section_refs("photon")
        assert "appendix" not in result


# --- ALWAYS_INCLUDE_SECTIONS ---


class TestAlwaysIncludeSections:
    def test_contains_thresholds_summary(self):
        assert "bottleneck_summary" in ALWAYS_INCLUDE_SECTIONS

    def test_contains_parameters_summary(self):
        assert "spark_params" in ALWAYS_INCLUDE_SECTIONS

    def test_contains_hints(self):
        assert "appendix" in ALWAYS_INCLUDE_SECTIONS


# --- parse_review_json ---

VALID_REVIEW_JSON = json.dumps(
    {
        "overall": "良好",
        "issues": [
            {
                "type": "wrong_value",
                "location": "推奨事項 Priority 1",
                "claim": "spark.sql.shuffle.partitions = 200",
                "problem": "ナレッジには400が推奨値",
                "fix": "値を400に修正",
            }
        ],
        "additions": ["キャッシュヒット率が低いがI/O分析で言及されていない"],
        "pass": False,
    },
    ensure_ascii=False,
)

PASS_REVIEW_JSON = json.dumps(
    {
        "overall": "優秀",
        "issues": [],
        "additions": [],
        "pass": True,
    },
    ensure_ascii=False,
)


class TestParseReviewJson:
    def test_parse_valid_json(self):
        result = parse_review_json(VALID_REVIEW_JSON)
        assert result is not None
        assert result["overall"] == "良好"
        assert len(result["issues"]) == 1
        assert result["pass"] is False

    def test_parse_pass_json(self):
        result = parse_review_json(PASS_REVIEW_JSON)
        assert result is not None
        assert result["pass"] is True
        assert len(result["issues"]) == 0

    def test_parse_json_with_code_fence(self):
        wrapped = f"```json\n{VALID_REVIEW_JSON}\n```"
        result = parse_review_json(wrapped)
        assert result is not None
        assert result["overall"] == "良好"

    def test_parse_json_with_plain_fence(self):
        wrapped = f"```\n{VALID_REVIEW_JSON}\n```"
        result = parse_review_json(wrapped)
        assert result is not None

    def test_parse_plain_markdown_returns_none(self):
        markdown = "## レビュー総評\n\n良好です。"
        result = parse_review_json(markdown)
        assert result is None

    def test_parse_empty_returns_none(self):
        assert parse_review_json("") is None
        assert parse_review_json(None) is None

    def test_parse_missing_issues_key_returns_none(self):
        # JSON without "issues" key should return None
        invalid = json.dumps({"overall": "Good"})
        assert parse_review_json(invalid) is None


# --- format_review_for_refine ---


class TestFormatReviewForRefine:
    def test_formats_json_issues(self):
        result = format_review_for_refine(VALID_REVIEW_JSON)
        assert "Issue 1:" in result
        assert "wrong_value" in result
        assert "spark.sql.shuffle.partitions = 200" in result
        assert "値を400に修正" in result

    def test_formats_additions(self):
        result = format_review_for_refine(VALID_REVIEW_JSON)
        assert "Additions" in result
        assert "キャッシュヒット率" in result

    def test_pass_review_no_issues(self):
        result = format_review_for_refine(PASS_REVIEW_JSON)
        assert "Pass: True" in result
        assert "Issue" not in result

    def test_markdown_fallback(self):
        markdown = "## レビュー総評\n\n良好です。"
        result = format_review_for_refine(markdown)
        # Should return raw text unchanged
        assert result == markdown

    def test_empty_fallback(self):
        result = format_review_for_refine("")
        assert result == ""


# --- create_review_prompt fact pack ---


class TestReviewPromptFactPack:
    """Test that review prompt includes comprehensive metrics."""

    def test_review_prompt_contains_alerts(self):
        from core.llm_prompts import create_review_prompt
        from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics

        analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(query_id="test-001", total_time_ms=5000),
            bottleneck_indicators=BottleneckIndicators(
                cache_hit_ratio=0.25,
                alerts=[
                    Alert(
                        severity=Severity.HIGH,
                        category="cache",
                        message="Low cache hit ratio",
                        current_value="25%",
                        threshold=">80%",
                    )
                ],
            ),
        )
        result = create_review_prompt(analysis, "Some LLM analysis", "test-model")
        # Should contain alert info
        assert "cache" in result.lower()
        assert "25%" in result
        # Should contain more than just basic 7 metrics
        assert "Compilation Time" in result or "コンパイル時間" in result
        assert "verification" in result.lower() or "検証" in result


# --- create_refine_prompt change summary ---


class TestRefinePromptChangeSummary:
    """Test that refine prompt requests change summary."""

    def test_refine_prompt_requests_changes_comment(self):
        from core.llm_prompts import create_refine_prompt

        result = create_refine_prompt(
            "initial analysis",
            "review comments",
            "model-a",
            "model-b",
            lang="en",
        )
        assert "<!-- CHANGES:" in result

    def test_refine_prompt_with_analysis_includes_evidence(self):
        from core.llm_prompts import create_refine_prompt
        from core.models import BottleneckIndicators, ProfileAnalysis, QueryMetrics

        analysis = ProfileAnalysis(
            query_metrics=QueryMetrics(query_id="test-002", total_time_ms=3000),
            bottleneck_indicators=BottleneckIndicators(spill_bytes=1024**3),
        )
        result = create_refine_prompt(
            "initial analysis",
            "review comments",
            "model-a",
            "model-b",
            lang="en",
            analysis=analysis,
        )
        assert "reference for corrections" in result.lower()
        assert "test-002" in result
