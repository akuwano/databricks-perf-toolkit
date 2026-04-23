"""Tests for L1 syntax validity scorer."""

import pytest

from core.models import ActionCard
from eval.scorers.l1_syntax import score_l1


class TestL1EmptySQL:
    def test_empty_fix_sql(self):
        card = ActionCard(fix_sql="")
        score = score_l1(card)
        assert score.has_fix_sql is False
        assert score.parses_ok is True
        assert score.serverless_compliant is True

    def test_none_like_fix_sql(self):
        card = ActionCard(fix_sql="   ")
        score = score_l1(card)
        assert score.has_fix_sql is False


class TestL1ValidSQL:
    def test_simple_select(self):
        card = ActionCard(fix_sql="SELECT * FROM my_table WHERE id = 1")
        score = score_l1(card)
        assert score.has_fix_sql is True
        assert score.parses_ok is True

    def test_alter_table(self):
        card = ActionCard(fix_sql="ALTER TABLE my_catalog.my_schema.my_table CLUSTER BY (col1, col2)")
        score = score_l1(card)
        assert score.parses_ok is True

    def test_select_with_hint(self):
        card = ActionCard(fix_sql="SELECT /*+ BROADCAST(t) */ * FROM my_table t")
        score = score_l1(card)
        assert score.parses_ok is True

    def test_multi_statement(self):
        card = ActionCard(fix_sql=(
            "SET spark.sql.files.maxPartitionBytes = 134217728;\n"
            "SELECT * FROM my_table"
        ))
        score = score_l1(card, is_serverless=True)
        assert score.parses_ok is True
        assert score.serverless_compliant is True


class TestL1InvalidSQL:
    def test_garbled_sql(self):
        card = ActionCard(fix_sql="))) INVALID (((")
        score = score_l1(card)
        assert score.parses_ok is False
        assert score.parse_error != ""


class TestL1ServerlessCompliance:
    def test_unsupported_config(self):
        card = ActionCard(fix_sql="SET spark.sql.shuffle.partitions = 200")
        score = score_l1(card, is_serverless=True)
        assert score.serverless_compliant is False
        assert "spark.sql.shuffle.partitions" in score.unsupported_configs

    def test_supported_config(self):
        card = ActionCard(fix_sql="SET spark.sql.files.maxPartitionBytes = 134217728")
        score = score_l1(card, is_serverless=True)
        assert score.serverless_compliant is True
        assert score.unsupported_configs == []

    def test_non_serverless_allows_all(self):
        card = ActionCard(fix_sql="SET spark.sql.shuffle.partitions = 200")
        score = score_l1(card, is_serverless=False)
        assert score.serverless_compliant is True

    def test_mixed_supported_unsupported(self):
        card = ActionCard(fix_sql=(
            "SET spark.sql.files.maxPartitionBytes = 134217728;\n"
            "SET spark.sql.autoBroadcastJoinThreshold = 10485760"
        ))
        score = score_l1(card, is_serverless=True)
        assert score.serverless_compliant is False
        assert "spark.sql.autoBroadcastJoinThreshold" in score.unsupported_configs


class TestL1Comments:
    def test_comment_only_lines_skipped(self):
        card = ActionCard(fix_sql=(
            "-- This is a comment\n"
            "SELECT * FROM my_table"
        ))
        score = score_l1(card)
        assert score.parses_ok is True
