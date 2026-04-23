"""Tests for Serverless SQL Warehouse config filtering and query rewrite recommendations."""

import re

from core.analyzers.recommendations import (
    _filter_fix_sql_for_serverless,
    generate_action_cards,
)
from core.constants import SERVERLESS_SUPPORTED_SPARK_CONFIGS
from core.llm_prompts.prompts import (
    _serverless_constraints_block,
    create_structured_system_prompt,
    create_system_prompt,
)
from core.models import BottleneckIndicators, JoinInfo, QueryMetrics
from core.reporters.details import generate_recommended_spark_params

# =============================================================================
# Constants tests
# =============================================================================


class TestServerlessConstants:
    def test_supported_configs_count(self):
        assert len(SERVERLESS_SUPPORTED_SPARK_CONFIGS) == 7

    def test_supported_configs(self):
        assert "spark.sql.ansi.enabled" in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.legacy.timeParserPolicy" in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.files.maxPartitionBytes" in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.session.timeZone" in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.databricks.execution.timeout" in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.databricks.io.cache.enabled" in SERVERLESS_SUPPORTED_SPARK_CONFIGS

    def test_non_serverless_configs_not_supported(self):
        assert "spark.sql.shuffle.partitions" not in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.autoBroadcastJoinThreshold" not in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.join.preferSortMergeJoin" not in SERVERLESS_SUPPORTED_SPARK_CONFIGS
        assert "spark.sql.adaptive.skewJoin.enabled" not in SERVERLESS_SUPPORTED_SPARK_CONFIGS


# =============================================================================
# _filter_fix_sql_for_serverless tests
# =============================================================================


class TestFilterFixSqlForServerless:
    def test_removes_non_supported_set(self):
        sql = "SET spark.sql.autoBroadcastJoinThreshold = 209715200;"
        result = _filter_fix_sql_for_serverless(sql)
        assert "autoBroadcastJoinThreshold" not in result

    def test_keeps_supported_set(self):
        sql = "SET spark.sql.files.maxPartitionBytes = 134217728;"
        result = _filter_fix_sql_for_serverless(sql)
        assert "maxPartitionBytes" in result

    def test_keeps_comments_and_sql(self):
        sql = (
            "-- Increase broadcast threshold\n"
            "SET spark.sql.autoBroadcastJoinThreshold = 209715200;\n"
            "\n"
            "-- Use BROADCAST hint\n"
            "SELECT /*+ BROADCAST(t) */ * FROM t1 JOIN t2 ON ...;"
        )
        result = _filter_fix_sql_for_serverless(sql)
        assert "autoBroadcastJoinThreshold" not in result
        assert "BROADCAST" in result
        assert "Use BROADCAST hint" in result

    def test_keeps_mixed_supported_and_non_supported(self):
        sql = (
            "SET spark.sql.files.maxPartitionBytes = 134217728;\n"
            "SET spark.sql.adaptive.skewJoin.enabled = true;\n"
            "SET spark.sql.autoBroadcastJoinThreshold = 209715200;"
        )
        result = _filter_fix_sql_for_serverless(sql)
        assert "maxPartitionBytes" in result
        assert "skewJoin" not in result
        assert "autoBroadcastJoinThreshold" not in result

    def test_empty_input(self):
        assert _filter_fix_sql_for_serverless("") == ""
        assert _filter_fix_sql_for_serverless(None) is None

    def test_no_set_statements(self):
        sql = "SELECT * FROM table WHERE id = 1;"
        result = _filter_fix_sql_for_serverless(sql)
        assert result == sql


# =============================================================================
# ActionCard serverless tests
# =============================================================================


def _make_indicators(**overrides) -> BottleneckIndicators:
    """Create BottleneckIndicators with defaults overridable for testing."""
    defaults = {
        "spill_bytes": 0,
        "has_data_skew": False,
        "skewed_partitions": 0,
        "photon_ratio": 0.9,
        "cache_hit_ratio": 0.8,
        "shuffle_impact_ratio": 0.0,
        "filter_rate": 0.5,
        "bytes_pruning_ratio": 0.5,
        "rescheduled_scan_ratio": 0.0,
        "hash_table_resize_count": 0,
        "avg_hash_probes_per_row": 0,
        # scan_impact default must exceed the scan_impact_mid gate (10%)
        # so IO-related cards surface in these tests.
        "scan_impact_ratio": 0.5,
    }
    defaults.update(overrides)
    return BottleneckIndicators(**defaults)


def _make_qm() -> QueryMetrics:
    # task_total_time_ms set above the photon_small_query_ms threshold
    # (5000ms) so Photon alerts/cards are not suppressed in these tests.
    return QueryMetrics(
        total_time_ms=1000,
        task_total_time_ms=60_000,
        read_bytes=100 * 1024**2,
        read_files_count=10,
        pruned_files_count=5,
    )


SET_PATTERN = re.compile(r"SET\s+(spark\.\S+)\s*=", re.IGNORECASE)


def _extract_set_configs(cards):
    """Extract all SET spark.* config keys from all cards' fix_sql."""
    configs = set()
    for card in cards:
        if card.fix_sql:
            for m in SET_PATTERN.finditer(card.fix_sql):
                configs.add(m.group(1).rstrip(";"))
    return configs


class TestActionCardsServerless:
    def test_skew_card_serverless_no_non_supported_configs(self):
        indicators = _make_indicators(has_data_skew=True, skewed_partitions=5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        configs = _extract_set_configs(cards)
        for c in configs:
            assert c in SERVERLESS_SUPPORTED_SPARK_CONFIGS, f"Non-supported config {c} found"

    def test_skew_card_serverless_has_cte_preagg(self):
        indicators = _make_indicators(has_data_skew=True, skewed_partitions=5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        skew_cards = [
            c for c in cards if "skew" in c.problem.lower() or "imbalance" in c.problem.lower()
        ]
        assert skew_cards, "Expected skew card"
        assert "CTE" in skew_cards[0].fix_sql.upper() or "WITH" in skew_cards[0].fix_sql.upper()

    def test_skew_card_classic_has_aqe_config(self):
        indicators = _make_indicators(has_data_skew=True, skewed_partitions=5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=False)
        configs = _extract_set_configs(cards)
        assert "spark.sql.adaptive.skewJoin.enabled" in configs

    def test_photon_card_serverless_no_non_supported_configs(self):
        indicators = _make_indicators(photon_ratio=0.4)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        configs = _extract_set_configs(cards)
        for c in configs:
            assert c in SERVERLESS_SUPPORTED_SPARK_CONFIGS, f"Non-supported config {c} found"

    def test_photon_card_serverless_has_hint(self):
        indicators = _make_indicators(photon_ratio=0.4)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        photon_cards = [c for c in cards if "photon" in c.problem.lower()]
        assert photon_cards, "Expected photon card"
        assert "SHUFFLE_HASH" in photon_cards[0].fix_sql or "BROADCAST" in photon_cards[0].fix_sql

    def test_shuffle_card_serverless_no_non_supported_configs(self):
        indicators = _make_indicators(shuffle_impact_ratio=0.5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        configs = _extract_set_configs(cards)
        for c in configs:
            assert c in SERVERLESS_SUPPORTED_SPARK_CONFIGS, f"Non-supported config {c} found"

    def test_shuffle_card_serverless_has_cte_rewrite(self):
        indicators = _make_indicators(shuffle_impact_ratio=0.5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=True)
        shuffle_cards = [c for c in cards if "shuffle" in c.problem.lower()]
        assert shuffle_cards, "Expected shuffle card"
        assert (
            "CTE" in shuffle_cards[0].fix_sql.upper() or "WITH" in shuffle_cards[0].fix_sql.upper()
        )

    def test_shuffle_card_classic_has_broadcast_threshold(self):
        indicators = _make_indicators(shuffle_impact_ratio=0.5)
        cards = generate_action_cards(indicators, [], _make_qm(), [], [], is_serverless=False)
        configs = _extract_set_configs(cards)
        assert "spark.sql.autoBroadcastJoinThreshold" in configs

    def test_hash_join_card_serverless_no_non_supported_configs(self):
        from core.models import QueryStructure, SQLAnalysis

        indicators = _make_indicators(hash_table_resize_count=200, avg_hash_probes_per_row=20)
        sql_analysis = SQLAnalysis(structure=QueryStructure(join_count=2))
        from core.constants import JoinType

        cards = generate_action_cards(
            indicators,
            [],
            _make_qm(),
            [],
            [JoinInfo(join_type=JoinType.SORT_MERGE)],
            sql_analysis=sql_analysis,
            is_serverless=True,
        )
        configs = _extract_set_configs(cards)
        for c in configs:
            assert c in SERVERLESS_SUPPORTED_SPARK_CONFIGS, f"Non-supported config {c} found"


# =============================================================================
# Reporter serverless tests
# =============================================================================


class TestReporterServerless:
    def test_serverless_no_non_supported_configs(self):
        bi = _make_indicators(
            photon_ratio=0.4, shuffle_impact_ratio=0.5, has_data_skew=True, spill_bytes=2 * 1024**3
        )
        result = generate_recommended_spark_params(bi, [], is_serverless=True)
        # Should not contain non-supported SET statements
        for m in SET_PATTERN.finditer(result):
            config = m.group(1).rstrip(";")
            assert config in SERVERLESS_SUPPORTED_SPARK_CONFIGS, (
                f"Non-supported config {config} in report"
            )

    def test_serverless_has_query_hints(self):
        bi = _make_indicators(photon_ratio=0.4, shuffle_impact_ratio=0.5)
        result = generate_recommended_spark_params(bi, [], is_serverless=True)
        assert "BROADCAST" in result
        assert "SHUFFLE_HASH" in result

    def test_serverless_has_cte_examples(self):
        bi = _make_indicators(shuffle_impact_ratio=0.5, spill_bytes=2 * 1024**3)
        result = generate_recommended_spark_params(bi, [], is_serverless=True)
        assert "WITH" in result or "CTE" in result

    def test_classic_has_full_configs(self):
        bi = _make_indicators(photon_ratio=0.4, shuffle_impact_ratio=0.5)
        result = generate_recommended_spark_params(bi, [], is_serverless=False)
        assert "autoBroadcastJoinThreshold" in result
        assert "preferSortMergeJoin" in result

    def test_serverless_cte_preagg_for_skew(self):
        bi = _make_indicators(shuffle_impact_ratio=0.5, has_data_skew=True, spill_bytes=2 * 1024**3)
        result = generate_recommended_spark_params(bi, [], is_serverless=True)
        assert "CTE" in result.upper() or "WITH" in result.upper()


# =============================================================================
# LLM prompt serverless tests
# =============================================================================


class TestLLMPromptServerless:
    def test_system_prompt_serverless_en(self):
        prompt = create_system_prompt("knowledge", lang="en", is_serverless=True)
        assert "Serverless SQL Warehouse" in prompt
        assert "spark.sql.ansi.enabled" in prompt
        assert "Do NOT recommend" in prompt
        # shuffle.partitions should be mentioned as NOT available
        assert "shuffle.partitions" in prompt

    def test_system_prompt_serverless_ja(self):
        prompt = create_system_prompt("knowledge", lang="ja", is_serverless=True)
        assert "Serverless SQL Warehouse" in prompt
        assert "spark.sql.ansi.enabled" in prompt

    def test_system_prompt_classic_no_serverless_block(self):
        prompt = create_system_prompt("knowledge", lang="en", is_serverless=False)
        assert "Serverless SQL Warehouse Constraints" not in prompt

    def test_structured_prompt_serverless_en(self):
        prompt = create_structured_system_prompt("knowledge", lang="en", is_serverless=True)
        assert "Serverless SQL Warehouse" in prompt

    def test_structured_prompt_classic_no_serverless_block(self):
        prompt = create_structured_system_prompt("knowledge", lang="en", is_serverless=False)
        assert "Serverless SQL Warehouse Constraints" not in prompt

    def test_serverless_constraints_mention_query_rewrite(self):
        block = _serverless_constraints_block("en")
        assert "query rewrite" in block.lower() or "QUERY REWRITE" in block
        assert "CTE" in block or "cte" in block.lower()
        assert "BROADCAST" in block or "broadcast" in block.lower()
