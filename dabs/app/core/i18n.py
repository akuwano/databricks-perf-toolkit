"""
Internationalization (i18n) support for DBSQL Profiler Analyzer.

This module provides translation functions that work in both Flask and CLI contexts.
- In Flask context: Uses Flask-Babel for translations
- In CLI/background thread: Uses Python gettext with .mo files directly

Single source of truth: translations/ja/LC_MESSAGES/messages.po

Usage:
    from core.i18n import gettext as _, set_language, get_language

    # Set language for CLI
    set_language('ja')

    # Use translation
    message = _("Cache hit ratio is low ({ratio})")
"""

import gettext as _gettext_module
import os
import threading
from pathlib import Path

# Current language setting (for non-Flask contexts) — thread-local to prevent
# concurrent requests from clobbering each other's language.
_thread_local = threading.local()


def _get_current_language() -> str:
    return getattr(_thread_local, "language", "en")


def _set_current_language(lang: str) -> None:
    _thread_local.language = lang


# Cached gettext Translation objects (loaded from .mo files)
_translations_cache: dict[
    str, _gettext_module.GNUTranslations | _gettext_module.NullTranslations
] = {}


def _get_mo_translation(lang: str):
    """Load translation from .mo file, cached."""
    if lang in _translations_cache:
        return _translations_cache[lang]

    # Try multiple paths to find translations directory
    search_paths = [
        Path(__file__).parent.parent / "translations",  # dabs/app/translations
        Path.cwd() / "translations",
    ]

    t: _gettext_module.GNUTranslations | _gettext_module.NullTranslations
    for trans_dir in search_paths:
        if (trans_dir / lang / "LC_MESSAGES" / "messages.mo").exists():
            try:
                t = _gettext_module.translation(
                    "messages", localedir=str(trans_dir), languages=[lang]
                )
                _translations_cache[lang] = t
                return t
            except Exception:
                pass

    # Fallback: NullTranslations (returns original text)
    t = _gettext_module.NullTranslations()
    _translations_cache[lang] = t
    return t


def set_language(lang: str) -> None:
    """Set the current language for non-Flask contexts (thread-local).

    Args:
        lang: Language code ('en' or 'ja')
    """
    _set_current_language(lang if lang in ("en", "ja") else "en")


def get_language() -> str:
    """Get the current language setting (thread-local).

    Returns:
        Current language code ('en' or 'ja')
    """
    return _get_current_language()


def _try_flask_gettext(text: str) -> str | None:
    """Try to use Flask-Babel's gettext if available.

    Returns:
        Translated text if Flask context available, None otherwise
    """
    try:
        from flask import has_app_context

        if has_app_context():
            from flask_babel import gettext

            return str(gettext(text))
    except ImportError:
        pass
    except RuntimeError:
        pass
    return None


def _get_flask_locale() -> str | None:
    """Get the current locale from Flask-Babel if available.

    Returns:
        Locale code ('en', 'ja', etc.) or None if not in Flask context
    """
    try:
        from flask import has_app_context

        if has_app_context():
            from flask_babel import get_locale

            locale = get_locale()
            if locale:
                return str(locale)
    except ImportError:
        pass
    except RuntimeError:
        pass
    return None


def gettext(text: str) -> str:
    """Translate text to the current language.

    In Flask context, tries Flask-Babel first, then falls back to dictionary.
    In CLI context, uses the fallback dictionary.

    Args:
        text: English text to translate

    Returns:
        Translated text (or original if no translation available)
    """
    # Determine the effective language
    flask_locale = _get_flask_locale()
    effective_lang = flask_locale if flask_locale else _get_current_language()

    # If English, return as-is
    if effective_lang == "en":
        return text

    # Try Flask-Babel first (for .po/.mo translations in Flask context)
    flask_result = _try_flask_gettext(text)
    if flask_result is not None and flask_result != text:
        return flask_result

    # Fallback: use .mo file directly (works in background threads / CLI)
    if effective_lang == "ja" or effective_lang.startswith("ja"):
        t = _get_mo_translation("ja")
        return str(t.gettext(text))

    return text


def ngettext(singular: str, plural: str, n: int) -> str:
    """Translate text with plural forms.

    Args:
        singular: Singular form in English
        plural: Plural form in English
        n: Count for plural selection

    Returns:
        Translated text
    """
    # Determine the effective language
    flask_locale = _get_flask_locale()
    effective_lang = flask_locale if flask_locale else _get_current_language()

    text = singular if n == 1 else plural

    # If English, return as-is
    if effective_lang == "en":
        return text

    # Try Flask-Babel first
    try:
        from flask import has_app_context

        if has_app_context():
            from flask_babel import ngettext as flask_ngettext

            result = flask_ngettext(singular, plural, n)
            if result != text:
                return str(result)
    except ImportError:
        pass
    except RuntimeError:
        pass

    # Fallback: use .mo file directly
    if effective_lang == "ja" or effective_lang.startswith("ja"):
        t = _get_mo_translation("ja")
        return str(t.gettext(text))

    return text


def get_stage_messages() -> dict[str, str]:
    """Get translated stage messages for the loading overlay.

    Note: Not cached because translations depend on current locale.

    Returns:
        Dictionary mapping stage keys to translated messages
    """
    return {
        "queued": gettext("Queued..."),
        "metrics": gettext("Analyzing metrics..."),
        "explain": gettext("Analyzing EXPLAIN..."),
        "llm_initial": gettext("Stage 1/3: Initial LLM analysis..."),
        "llm_review": gettext("Stage 2/3: LLM review..."),
        "llm_refine": gettext("Stage 3/3: LLM refinement..."),
        "report": gettext("Generating report..."),
        "report_review": gettext("Reviewing report with LLM..."),
        "report_refine": gettext("Refining report with LLM..."),
        "done": gettext("Complete!"),
    }


def init_language_from_env() -> None:
    """Initialize language from environment variable DBSQL_LANG."""
    lang = os.environ.get("DBSQL_LANG", "en")
    set_language(lang)


# Section header translations used in reports (en ↔ ja)
_SECTION_HEADERS_EN_JA: list[tuple[str, str]] = [
    ("Query Performance Report", "クエリパフォーマンスレポート"),
    ("Executive Summary", "エグゼクティブサマリー"),
    ("Top Alerts", "トップアラート"),
    ("Recommended Actions", "推奨アクション"),
    ("Quick Summary", "クイックサマリー"),
    ("Performance Metrics", "パフォーマンスメトリクス"),
    ("Warehouse Sizing Recommendations", "ウェアハウスサイジング推奨"),
    ("Root Cause Analysis", "根本原因分析"),
    ("Hot Operators", "ホットオペレータ"),
    ("AQE Shuffle Health", "AQE Shuffle健全性"),
    ("Scan Locality", "スキャンローカリティ"),
    ("Data Flow", "データフロー"),
    ("Optimized SQL", "最適化済みSQL"),
    ("LLM Analysis Report", "LLM分析レポート"),
    ("Appendix", "付録"),
    ("Query Overview", "クエリ概要"),
    ("SQL / Query Structure", "SQL/クエリ構造"),
    ("Stage Execution", "ステージ実行"),
    ("Data Flow Details", "データフロー詳細"),
    ("Scan Locality Details", "スキャンローカリティ詳細"),
    ("Bottleneck Indicators", "ボトルネック指標"),
    ("Spill & Photon Analysis", "スピル＆Photon分析"),
    ("All Alerts", "全アラート"),
    ("Rationale", "根拠"),
    ("Cause Hypothesis", "原因仮説"),
    ("Improvement", "改善策"),
    ("Fix Risk", "修正リスク"),
    ("Verification Metric", "検証メトリクス"),
    ("Expected", "期待値"),
    ("Run", "実行"),
]


def relocalize_report(report: str, target_lang: str) -> str:
    """Re-localize a baked Markdown report to the target language.

    Swaps section headers between English and Japanese so the report
    matches the user's current UI language regardless of the language
    used at generation time.
    """
    if not report:
        return report

    for en, ja in _SECTION_HEADERS_EN_JA:
        if target_lang == "en":
            report = report.replace(ja, en)
        else:
            report = report.replace(en, ja)

    return report


# Alias for convenience
_ = gettext
