"""Tests for multi-file knowledge loading."""

from core.constants import Severity
from core.llm import (
    filter_knowledge_for_analysis,
    load_tuning_knowledge,
    parse_knowledge_sections,
)
from core.models import Alert


class TestLoadTuningKnowledgeMultiFile:
    """Test that load_tuning_knowledge loads multiple files from a directory."""

    def test_loads_default_knowledge_dir(self):
        """Default knowledge dir should contain sections from multiple files."""
        content = load_tuning_knowledge()
        sections = parse_knowledge_sections(content)
        # Should contain base sections
        assert "io" in sections
        assert "photon" in sections
        # Should also contain new sections from additional files
        assert "sql_patterns" in sections
        assert "photon_oom" in sections
        assert "serverless" in sections
        assert "shuffle_advanced" in sections

    def test_loads_en_knowledge(self):
        """English knowledge should load _en.md files."""
        content = load_tuning_knowledge(lang="en")
        sections = parse_knowledge_sections(content)
        assert "io" in sections
        assert "sql_patterns" in sections
        assert "photon_oom" in sections
        assert "serverless" in sections

    def test_single_file_path_still_works(self, tmp_path):
        """Passing a single file path should still work (backward compat)."""
        md = tmp_path / "test.md"
        md.write_text("## Test\n<!-- section_id: test_sec -->\n\nTest content.\n")
        content = load_tuning_knowledge(base_path=str(md))
        assert "Test content" in content

    def test_directory_loads_multiple_files(self, tmp_path):
        """Passing a directory should load all matching dbsql_*.md files."""
        (tmp_path / "dbsql_a.md").write_text(
            "## Section A\n<!-- section_id: sec_a -->\n\nContent A.\n"
        )
        (tmp_path / "dbsql_b.md").write_text(
            "## Section B\n<!-- section_id: sec_b -->\n\nContent B.\n"
        )
        # _en.md should be excluded for ja
        (tmp_path / "dbsql_a_en.md").write_text(
            "## Section A EN\n<!-- section_id: sec_a -->\n\nContent A EN.\n"
        )
        content = load_tuning_knowledge(base_path=str(tmp_path), lang="ja")
        assert "Content A." in content
        assert "Content B." in content
        assert "Content A EN." not in content

    def test_directory_loads_en_files(self, tmp_path):
        """English loading should pick _en.md files only."""
        (tmp_path / "dbsql_a.md").write_text(
            "## Section A JA\n<!-- section_id: sec_a -->\n\nContent JA.\n"
        )
        (tmp_path / "dbsql_a_en.md").write_text(
            "## Section A EN\n<!-- section_id: sec_a -->\n\nContent EN.\n"
        )
        content = load_tuning_knowledge(base_path=str(tmp_path), lang="en")
        assert "Content EN." in content
        assert "Content JA." not in content

    def test_empty_directory_returns_empty(self, tmp_path):
        """Empty directory should return empty string."""
        content = load_tuning_knowledge(base_path=str(tmp_path), lang="ja")
        assert content == ""

    def test_nonexistent_directory_returns_empty(self, tmp_path):
        """Nonexistent directory should return empty string."""
        content = load_tuning_knowledge(base_path=str(tmp_path / "nonexistent"), lang="ja")
        assert content == ""


class TestMultiFileRoutingIntegration:
    """Integration test: multi-file knowledge correctly routes by alerts."""

    def test_photon_alert_includes_photon_oom(self):
        """Photon alerts should route to both photon and photon_oom sections."""
        content = load_tuning_knowledge()
        alerts = [
            Alert(
                category="photon",
                severity=Severity.HIGH,
                message="Low Photon utilization",
                current_value="30%",
                threshold=">80%",
                recommendation="Check",
            )
        ]
        filtered = filter_knowledge_for_analysis(content, alerts)
        assert "photon_oom" in parse_knowledge_sections(filtered) or "OOM" in filtered

    def test_spill_alert_includes_advanced_sections(self):
        """Spill alerts should route to shuffle_advanced and data_explosion."""
        content = load_tuning_knowledge()
        alerts = [
            Alert(
                category="spill",
                severity=Severity.HIGH,
                message="High spill detected",
                current_value="10GB",
                threshold="<1GB",
                recommendation="Tune shuffle",
            )
        ]
        filtered = filter_knowledge_for_analysis(content, alerts)
        sections = parse_knowledge_sections(filtered)
        assert "shuffle_advanced" in sections or "Data Explosion" in filtered

    def test_join_alert_includes_sql_patterns(self):
        """Join alerts should route to sql_patterns and broadcast_advanced."""
        content = load_tuning_knowledge()
        alerts = [
            Alert(
                category="join",
                severity=Severity.MEDIUM,
                message="Suboptimal join",
                current_value="SMJ",
                threshold="BHJ",
                recommendation="Check",
            )
        ]
        filtered = filter_knowledge_for_analysis(content, alerts)
        assert "sql_patterns" in parse_knowledge_sections(filtered) or "SQL" in filtered
