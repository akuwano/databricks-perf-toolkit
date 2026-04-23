"""Tests for core.sql_safe — SQL identifier validation (security-critical)."""

import pytest
from core.sql_safe import safe_fqn, validate_identifier

# ── validate_identifier: valid identifiers ──────────────────────────────────


class TestValidateIdentifierValid:
    """Identifiers that MUST be accepted."""

    @pytest.mark.parametrize(
        "name",
        [
            "my_catalog",
            "schema_01",
            "table_name",
            "a",
            "ABC",
            "abc123",
            "my-catalog",
            "with-hyphen-and_underscore",
            "ALL_UPPER",
            "MixedCase",
            "123numeric_start",
            "a_b_c_d_e",
            "_leading_underscore",
            "-leading-hyphen",
            "trailing_underscore_",
            "trailing-hyphen-",
        ],
    )
    def test_valid_identifiers_accepted(self, name: str) -> None:
        assert validate_identifier(name) == name

    def test_returns_same_string(self) -> None:
        result = validate_identifier("my_table", label="table")
        assert result == "my_table"

    def test_custom_label_not_in_success(self) -> None:
        """Label is only used in error messages; valid input just returns the name."""
        assert validate_identifier("ok", label="catalog") == "ok"


# ── validate_identifier: invalid identifiers ────────────────────────────────


class TestValidateIdentifierInvalid:
    """Identifiers that MUST be rejected to prevent SQL injection."""

    def test_empty_string_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty identifier"):
            validate_identifier("")

    def test_empty_with_custom_label(self) -> None:
        with pytest.raises(ValueError, match="Empty catalog"):
            validate_identifier("", label="catalog")

    @pytest.mark.parametrize(
        "name",
        [
            "'; DROP TABLE users --",
            "a; DROP TABLE",
            "-- comment",
            "name; SELECT 1",
        ],
    )
    def test_sql_injection_drop_table(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    @pytest.mark.parametrize(
        "name",
        [
            "`backtick`",
            "back`tick",
            '"double_quote"',
            "'single_quote'",
        ],
    )
    def test_quoting_characters_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    @pytest.mark.parametrize(
        "name",
        [
            "has space",
            "tab\there",
            "new\nline",
            "carriage\rreturn",
        ],
    )
    def test_whitespace_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    @pytest.mark.parametrize(
        "name",
        [
            "table.name",
            "catalog.schema.table",
            "a.b",
        ],
    )
    def test_dots_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    def test_semicolon_rejected(self) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier("name;")

    @pytest.mark.parametrize(
        "name",
        [
            "tbl/**/",
            "name()",
            "name=value",
            "a+b",
            "a%20b",
            "a&b",
            "a|b",
            "a$b",
            "a@b",
            "a!b",
            "a#b",
            "a^b",
            "a*b",
            "a~b",
            "a<b",
            "a>b",
            "a[0]",
            "a{b}",
            "a\\b",
            "a/b",
        ],
    )
    def test_special_characters_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    @pytest.mark.parametrize(
        "name",
        [
            "\u30c6\u30fc\u30d6\u30eb",  # Japanese katakana
            "t\u00e4ble",  # German umlaut
            "caf\u00e9",  # French accent
            "\u0442\u0430\u0431\u043b\u0438\u0446\u0430",  # Cyrillic
            "\ud83d\ude00table",  # Emoji prefix
        ],
    )
    def test_unicode_rejected(self, name: str) -> None:
        with pytest.raises(ValueError, match="Invalid"):
            validate_identifier(name)

    def test_error_message_includes_label(self) -> None:
        with pytest.raises(ValueError, match="Invalid schema"):
            validate_identifier("bad name", label="schema")

    def test_error_message_includes_value(self) -> None:
        with pytest.raises(ValueError, match="bad;name"):
            validate_identifier("bad;name")


# ── safe_fqn: correct FQN generation ────────────────────────────────────────


class TestSafeFqn:
    """Fully-qualified name generation with validation of all parts."""

    def test_basic_fqn(self) -> None:
        assert safe_fqn("my_catalog", "my_schema", "my_table") == "my_catalog.my_schema.my_table"

    def test_fqn_with_hyphens(self) -> None:
        assert safe_fqn("my-catalog", "my-schema", "my-table") == "my-catalog.my-schema.my-table"

    def test_fqn_with_underscores_and_numbers(self) -> None:
        assert safe_fqn("cat01", "schema_02", "tbl_003") == "cat01.schema_02.tbl_003"

    def test_invalid_catalog_raises(self) -> None:
        with pytest.raises(ValueError, match="catalog"):
            safe_fqn("bad catalog", "ok", "ok")

    def test_invalid_schema_raises(self) -> None:
        with pytest.raises(ValueError, match="schema"):
            safe_fqn("ok", "bad;schema", "ok")

    def test_invalid_table_raises(self) -> None:
        with pytest.raises(ValueError, match="table"):
            safe_fqn("ok", "ok", "bad'table")

    def test_empty_catalog_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty catalog"):
            safe_fqn("", "ok", "ok")

    def test_empty_schema_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty schema"):
            safe_fqn("ok", "", "ok")

    def test_empty_table_raises(self) -> None:
        with pytest.raises(ValueError, match="Empty table"):
            safe_fqn("ok", "ok", "")

    def test_injection_in_catalog(self) -> None:
        with pytest.raises(ValueError):
            safe_fqn("'; DROP TABLE users --", "schema", "table")

    def test_injection_in_schema(self) -> None:
        with pytest.raises(ValueError):
            safe_fqn("catalog", "schema; DROP TABLE x", "table")

    def test_injection_in_table(self) -> None:
        with pytest.raises(ValueError):
            safe_fqn("catalog", "schema", "table; DROP TABLE x")

    def test_dots_in_parts_rejected(self) -> None:
        """Dots would allow escaping the FQN structure."""
        with pytest.raises(ValueError):
            safe_fqn("a.b", "schema", "table")
        with pytest.raises(ValueError):
            safe_fqn("catalog", "a.b", "table")
        with pytest.raises(ValueError):
            safe_fqn("catalog", "schema", "a.b")
