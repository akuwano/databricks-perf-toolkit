"""Tuning knowledge loading, parsing, and routing.

Routing uses stable section_id markers (<!-- section_id: xxx -->)
embedded in the markdown, decoupled from heading language.
"""

import logging
import re
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ..models import Alert

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)

# Section ID extracted from <!-- section_id: xxx --> markers in markdown
_SECTION_ID_RE = re.compile(r"<!--\s*section_id:\s*(\S+)\s*-->")

# =============================================================================
# Tuning knowledge loader
# =============================================================================

_KNOWLEDGE_FILES = {
    "ja": "dbsql_tuning.md",
    "en": "dbsql_tuning_en.md",
}

# Suffix patterns for locale-aware multi-file loading
_LANG_SUFFIX = {"ja": ".md", "en": "_en.md"}


def load_tuning_knowledge(base_path: str | None = None, lang: str = "ja") -> str:
    """Load tuning knowledge markdown for system prompt.

    Loads all ``dbsql_*.md`` files matching the requested language from the
    knowledge directory and concatenates them.  Files are sorted by name so
    that the base ``dbsql_tuning`` file comes first (alphabetical order).

    Args:
        base_path: Optional path to tuning file or directory containing it.
            - If None: uses default location (core/knowledge/)
            - If file path: reads that file directly
            - If directory path: reads all locale-matching files
        lang: Language code ("ja" or "en"). Falls back to "ja" if
            the requested language file does not exist.
    """
    if base_path is not None:
        path = Path(base_path)
        if path.is_file():
            return _read_file(path)
        return _load_knowledge_dir(path, lang)

    knowledge_dir = Path(__file__).parent.parent / "knowledge"
    return _load_knowledge_dir(knowledge_dir, lang)


def _load_knowledge_dir(directory: Path, lang: str, prefix: str = "dbsql") -> str:
    """Load and concatenate all locale-matching knowledge files from *directory*.

    File selection rules (using *prefix*):
    - For ``en``: files matching ``{prefix}_*_en.md``
    - For ``ja`` (default): files matching ``{prefix}_*.md`` **excluding** ``*_en.md``

    Falls back to the single legacy file when no multi-file matches are found.
    """
    if not directory.is_dir():
        logger.warning("Knowledge directory not found: %s", directory)
        return ""

    all_md = sorted(directory.glob(f"{prefix}_*.md"))

    if lang == "en":
        matched = [f for f in all_md if f.name.endswith("_en.md")]
    else:
        matched = [f for f in all_md if not f.name.endswith("_en.md")]

    if not matched:
        # Backward-compatible fallback: try legacy single file
        legacy_files = {
            "ja": f"{prefix}_tuning.md",
            "en": f"{prefix}_tuning_en.md",
        }
        legacy = directory / legacy_files.get(lang, legacy_files["ja"])
        if legacy.exists():
            return _read_file(legacy)
        legacy_ja = directory / legacy_files["ja"]
        return _read_file(legacy_ja) if legacy_ja.exists() else ""

    parts = []
    for f in matched:
        content = _read_file(f)
        if content:
            parts.append(content)

    combined = "\n\n".join(parts)
    logger.info(
        "Loaded %d knowledge files for prefix=%s, lang=%s, total %d chars",
        len(parts),
        prefix,
        lang,
        len(combined),
    )
    return combined


def _read_file(path: Path) -> str:
    """Read a file and return its content."""
    path = path.resolve()
    if path.exists():
        try:
            file_size = path.stat().st_size
            with open(path, encoding="utf-8") as f:
                content = f.read()
            logger.info(
                "Loaded tuning knowledge: path=%s, file_size=%d bytes, content_length=%d chars",
                path,
                file_size,
                len(content),
            )
            return content
        except Exception:
            logger.warning("Failed to read tuning knowledge file: %s", path, exc_info=True)
            return ""
    else:
        logger.warning("Tuning knowledge file not found: %s", path)
        return ""


def load_spark_tuning_knowledge(base_path: str | None = None, lang: str = "ja") -> str:
    """Load Spark Perf tuning knowledge markdown for system prompt.

    Loads all ``spark_*.md`` files matching the requested language.
    """
    if base_path is not None:
        path = Path(base_path)
        if path.is_file():
            return _read_file(path)
        return _load_knowledge_dir(path, lang, prefix="spark")

    knowledge_dir = Path(__file__).parent.parent / "knowledge"
    return _load_knowledge_dir(knowledge_dir, lang, prefix="spark")


# =============================================================================
# Knowledge section routing by alert category (section_id based)
# =============================================================================

# Maps DBSQL alert categories to section_ids (language-independent)
CATEGORY_TO_SECTION_IDS: dict[str, list[str]] = {
    "cache": ["io", "cache"],
    "io": ["io", "cloud_storage"],
    "photon": ["photon", "photon_oom"],
    # Spill is often a downstream symptom of a heavy shuffle, so route the
    # shuffle-key LC guidance here too.
    "spill": ["spill", "cluster", "shuffle_advanced", "data_explosion", "lc_shuffle_key_candidate"],
    # Shuffle-dominant queries benefit from considering the shuffle key as
    # an LC candidate. Route the ``lc_shuffle_key_candidate`` section so
    # the main analysis LLM sees that guidance on shuffle alerts.
    "shuffle": ["shuffle", "shuffle_advanced", "lc_shuffle_key_candidate"],
    "join": ["execution_plan", "sql_patterns", "broadcast_advanced", "hash_resize_causes"],
    "statistics": ["execution_plan", "hash_resize_causes"],
    "aggregation": ["execution_plan", "sql_patterns", "hash_resize_causes"],
    "join/aggregation": [
        "execution_plan",
        "sql_patterns",
        "broadcast_advanced",
        "hash_resize_causes",
    ],
    "cloud_storage": ["cloud_storage"],
    "cluster": ["cluster", "serverless"],
    # Memory pressure usually stems from heavy shuffle or spill — include
    # the shuffle-key LC guidance so the LLM has the full context.
    "memory": ["shuffle", "cluster", "photon_oom", "data_explosion", "lc_shuffle_key_candidate"],
    "agg": ["execution_plan", "sql_patterns"],
    # Data skew on a specific key often matches the same key that dominates
    # shuffle; route the shuffle-key LC guidance here too.
    "skew": ["skew_advanced", "shuffle_advanced", "lc_shuffle_key_candidate"],
    "explosion": ["data_explosion", "shuffle_advanced"],
    "serverless": ["serverless"],
    "merge": ["merge_advanced"],
    # v5.16.25: driver-side overhead routing (PR #93). Knowledge
    # sections 7A (compilation_overhead) and 7B (driver_overhead) were
    # added to dbsql_tuning.md alongside the new alert categories.
    "compilation": ["compilation_overhead", "cloud_storage"],
    "driver_overhead": ["driver_overhead", "serverless", "cluster"],
    # v5.18.0: Lakehouse Federation routing. When the query runs via
    # federation, point the LLM at the federation-specific guidance
    # (pushdown / fetchSize / source-side strategies) rather than
    # Delta-oriented advice.
    "federation": ["federation", "sql_patterns"],
}

# Backward-compatible alias
CATEGORY_TO_KNOWLEDGE_SECTIONS = CATEGORY_TO_SECTION_IDS

ALWAYS_INCLUDE_SECTION_IDS = ["bottleneck_summary", "spark_params", "appendix"]

# V6 minimal set — Codex指摘 #3 で spark_params / appendix を削除。
# `feature_flags.always_include_minimum() == True` のとき下を使う。
_ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM = ["bottleneck_summary"]


def get_always_include_section_ids() -> list[str]:
    """Return the active ALWAYS_INCLUDE list, honoring V6 feature flag.

    Modules should call this helper instead of referring to
    ALWAYS_INCLUDE_SECTION_IDS directly so the V6 minimal set takes
    effect everywhere it matters (Codex 指摘 #3 + W3 地雷 "ALWAYS_INCLUDE
    が隠れ注入になる").
    """
    try:
        from core import feature_flags  # noqa: WPS433
    except ImportError:
        return list(ALWAYS_INCLUDE_SECTION_IDS)
    if feature_flags.always_include_minimum():
        return list(_ALWAYS_INCLUDE_SECTION_IDS_V6_MINIMUM)
    return list(ALWAYS_INCLUDE_SECTION_IDS)

# Maps Spark Perf bottleneck types to spark_*.md section_ids
SPARK_CATEGORY_TO_SECTION_IDS: dict[str, list[str]] = {
    "DATA_SKEW": ["spark_data_skew", "spark_data_layout", "spark_shuffle_params"],
    "DISK_SPILL": ["spark_disk_spill", "spark_compute", "spark_shuffle_params"],
    "HEAVY_SHUFFLE": ["spark_heavy_shuffle", "spark_shuffle_params", "spark_code"],
    "SMALL_FILES": ["spark_small_files", "spark_data_layout"],
    "PHOTON_FALLBACK": ["spark_photon", "spark_compute", "spark_code"],
    "SPOT_LOSS": ["spark_spot_loss"],
    "SERIALIZATION": ["spark_serialization", "spark_code"],
    "HIGH_GC": ["spark_disk_spill", "spark_compute"],
    "STAGE_FAILURE": ["spark_spot_loss", "spark_disk_spill"],
    "MEMORY_SPILL": ["spark_disk_spill", "spark_shuffle_params"],
    "SKEW_SHUFFLE_PARALLELISM": ["spark_data_skew", "spark_shuffle_params"],
    # Streaming bottleneck types
    "STREAM_EXCEPTION": ["spark_streaming", "spark_spot_loss"],
    "STREAM_BACKLOG": ["spark_streaming", "spark_compute"],
    "STREAM_SLOW_BATCH": ["spark_streaming", "spark_disk_spill", "spark_data_skew"],
    "STREAM_STATE_GROWTH": ["spark_streaming", "spark_compute"],
    "STREAM_WATERMARK_DROP": ["spark_streaming"],
    "STREAM_PLANNING_OVERHEAD": ["spark_streaming", "spark_compute"],
    "STREAM_COMMIT_OVERHEAD": ["spark_streaming"],
    "STREAM_LOW_THROUGHPUT": ["spark_streaming", "spark_compute"],
}

SPARK_ALWAYS_INCLUDE_SECTION_IDS = ["spark_overview", "spark_diagnostics"]

# Backward-compatible alias
ALWAYS_INCLUDE_SECTIONS = ALWAYS_INCLUDE_SECTION_IDS

_EXCLUDED_SECTION_IDS = {"overview", "references"}

# Section number mapping for human-readable references
_SECTION_ID_TO_NUMBER: dict[str, str] = {
    "io": "1",
    "execution_plan": "2",
    "shuffle": "3",
    "spill": "4",
    "photon": "5",
    "cache": "6",
    "cloud_storage": "7",
    "cluster": "8",
    "bottleneck_summary": "9",
    "spark_params": "10",
    "sql_patterns": "11",
    "photon_oom": "12",
    "serverless": "13",
    "shuffle_advanced": "14",
    "data_explosion": "15",
    "skew_advanced": "16",
    "broadcast_advanced": "17",
    "merge_advanced": "18",
    # v5.16.25: driver-side overhead sections (PR #93).
    "compilation_overhead": "7A",
    "driver_overhead": "7B",
    # v5.18.0: Lakehouse Federation section (section 19 in dbsql_tuning.md).
    "federation": "19",
}


def parse_knowledge_sections(knowledge_text: str) -> dict[str, str]:
    """Parse markdown knowledge into {section_id: content} dict.

    Extracts section_id from <!-- section_id: xxx --> markers.
    Falls back to heading-based parsing if no markers are found.
    """
    if not knowledge_text:
        return {}

    sections: dict[str, str] = {}
    parts = re.split(r"(?m)^## ", knowledge_text)

    for part in parts[1:]:
        lines = part.split("\n", 1)
        heading = lines[0].strip().rstrip("#").strip()
        content = lines[1] if len(lines) > 1 else ""

        # Extract section_id from marker
        sid_match = _SECTION_ID_RE.search(content[:200])  # Check first 200 chars
        if sid_match:
            section_id = sid_match.group(1)
            if section_id in _EXCLUDED_SECTION_IDS:
                continue
            # Remove the marker line from content
            content = _SECTION_ID_RE.sub("", content, count=1).strip()
            sections[section_id] = f"## {heading}\n\n{content}"
        else:
            # Fallback: use heading as key (backward compat)
            if heading and heading not in {"概要", "参考リンク", "Overview", "References"}:
                sections[heading] = content.strip()

    return sections


def _severity_rank(severity) -> int:
    """Map severity to numeric rank (higher = more critical)."""
    name = severity.value if hasattr(severity, "value") else str(severity)
    return {"critical": 5, "high": 4, "medium": 3, "low": 2, "info": 1, "ok": 0}.get(
        name.lower(), 0
    )


def _score_sections_by_alerts(
    sections: dict[str, str],
    alerts: list[Alert],
) -> dict[str, int]:
    """Score each knowledge section by the max severity of alerts that reference it.

    Returns:
        {section_id: priority_score} — higher is more important.
        ALWAYS_INCLUDE sections get score 100.
    """
    scores: dict[str, int] = {}

    # Always-include sections get highest priority
    for sid in get_always_include_section_ids():
        if sid in sections:
            scores[sid] = 100

    # Score by alert severity
    for alert in alerts:
        sids = CATEGORY_TO_SECTION_IDS.get(alert.category, [])
        rank = _severity_rank(alert.severity)
        for sid in sids:
            if sid in sections:
                scores[sid] = max(scores.get(sid, 0), rank)

    return scores


def filter_knowledge_by_alerts(
    sections: dict[str, str],
    alerts: list[Alert],
) -> str:
    """Select relevant knowledge sections based on alert categories.

    Sections are prioritized by the highest severity of alerts that reference
    them. CRITICAL/HIGH alerts' sections are always included; MEDIUM/LOW
    sections are included if budget allows.
    """
    if not sections:
        return ""

    if not alerts:
        return _join_sections(sections, set(sections.keys()))

    scores = _score_sections_by_alerts(sections, alerts)

    # Include all scored sections (filtered by alert relevance)
    relevant_ids = set(scores.keys())

    # Also add ALWAYS_INCLUDE (V6 honors feature_flags.always_include_minimum)
    for sid in get_always_include_section_ids():
        relevant_ids.add(sid)

    return _join_sections(sections, relevant_ids)


def _join_sections(sections: dict[str, str], wanted_ids: set[str]) -> str:
    """Join selected sections preserving original order."""
    parts = []
    for section_id, content in sections.items():
        if section_id in wanted_ids:
            # Content already includes ## heading if parsed with section_id
            if content.startswith("## "):
                parts.append(content)
            else:
                parts.append(f"## {section_id}\n\n{content}")
    return "\n\n---\n\n".join(parts)


def get_knowledge_section_refs(category: str) -> str:
    """Get human-readable section references for an alert category.

    Returns:
        Section reference string like "(→ Section 4, 8)" or empty string.
    """
    section_ids = CATEGORY_TO_SECTION_IDS.get(category, [])
    if not section_ids:
        return ""
    nums = []
    for sid in section_ids:
        num = _SECTION_ID_TO_NUMBER.get(sid)
        if num:
            nums.append(num)
    if nums:
        return f"(→ Section {', '.join(nums)})"
    return ""


def filter_knowledge_for_analysis(
    tuning_knowledge: str,
    alerts: list[Alert],
    max_chars: int = 0,
    llm_client: "Any | None" = None,
    llm_model: str = "",
) -> str:
    """Filter tuning knowledge to relevant sections based on alert severity.

    Strategy:
    1. Parse knowledge into sections
    2. Score each section by the max severity of alerts referencing it
    3. Include all relevant sections sorted by priority
    4. If budget exceeded: summarize low-priority sections with LLM,
       then drop if still over budget

    Args:
        tuning_knowledge: Full knowledge markdown text
        alerts: List of alerts for routing
        max_chars: Character budget (0 = no limit)
        llm_client: Optional OpenAI client for summarization
        llm_model: Model to use for summarization
    """
    sections = parse_knowledge_sections(tuning_knowledge)
    if not sections:
        return tuning_knowledge

    scores = _score_sections_by_alerts(sections, alerts)
    if not scores and not alerts:
        scores = {sid: 1 for sid in sections}

    # Include all scored sections
    relevant_ids = set(scores.keys())
    for sid in get_always_include_section_ids():
        relevant_ids.add(sid)

    result = _join_sections(sections, relevant_ids)

    logger.info(
        "Knowledge routing: %d alerts -> %d/%d sections selected (%d chars)",
        len(alerts),
        len(relevant_ids),
        len(sections),
        len(result),
    )

    # Enforce character budget with priority-aware trimming
    if max_chars > 0 and len(result) > max_chars:
        logger.warning(
            "Knowledge %d chars exceeds budget %d, applying priority-based trimming",
            len(result),
            max_chars,
        )
        result = _trim_by_priority(
            sections,
            scores,
            max_chars,
            llm_client=llm_client,
            llm_model=llm_model,
        )

    return result


def _trim_by_priority(
    sections: dict[str, str],
    scores: dict[str, int],
    max_chars: int,
    llm_client: "Any | None" = None,
    llm_model: str = "",
) -> str:
    """Trim knowledge to fit budget, dropping lowest-priority sections first.

    For sections that must be dropped, attempt LLM summarization first
    to preserve key information in condensed form.
    """
    # Sort sections by priority score (ascending = lowest priority first to drop)
    _always = set(get_always_include_section_ids())
    scored_items = [
        (sid, sections[sid], scores.get(sid, 0))
        for sid in sections
        if sid in scores or sid in _always
    ]
    scored_items.sort(key=lambda x: x[2], reverse=True)

    # Phase 1: Include sections in priority order until budget is reached
    included: list[tuple[str, str]] = []
    to_summarize: list[tuple[str, str]] = []
    running_total = 0
    divider_len = len("\n\n---\n\n")

    for sid, content, _score in scored_items:
        entry_len = len(content) + (divider_len if included else 0)
        if running_total + entry_len <= max_chars:
            included.append((sid, content))
            running_total += entry_len
        else:
            to_summarize.append((sid, content))

    # Phase 2: Try to summarize dropped sections and fit them in
    if to_summarize:
        dropped_names = [sid for sid, _ in to_summarize]
        logger.info(
            "Knowledge trimming: %d sections included, %d sections over budget: %s",
            len(included),
            len(to_summarize),
            dropped_names,
        )

        # Attempt LLM summarization of dropped sections.
        # V6: skip the secondary summarization pass entirely when
        # `feature_flags.skip_condensed_knowledge()` is on (Codex指摘 #4).
        try:
            from core import feature_flags as _ff  # noqa: WPS433
        except ImportError:
            _ff = None
        if _ff is not None and _ff.skip_condensed_knowledge():
            logger.info(
                "V6: skipping condensed knowledge regen (skip_condensed_knowledge=on); "
                "%d sections dropped without summary",
                len(to_summarize),
            )
        elif llm_client and llm_model:
            summary = _summarize_sections_with_llm(to_summarize, llm_client, llm_model)
            if summary:
                summary_entry_len = len(summary) + divider_len
                if running_total + summary_entry_len <= max_chars:
                    included.append(("_condensed_knowledge", summary))
                    logger.info(
                        "Added LLM-condensed knowledge (%d chars) for %d dropped sections",
                        len(summary),
                        len(to_summarize),
                    )
                else:
                    # Truncate summary to fit remaining budget
                    remaining = max_chars - running_total - divider_len - 50
                    if remaining > 200:
                        included.append(
                            ("_condensed_knowledge", summary[:remaining] + "\n\n<!-- condensed -->")
                        )
                        logger.info("Added truncated condensed knowledge (%d chars)", remaining)

    # Reassemble
    parts = []
    for sid, content in included:
        if content.startswith("## "):
            parts.append(content)
        else:
            parts.append(f"## {sid}\n\n{content}")
    return "\n\n---\n\n".join(parts)


def _summarize_sections_with_llm(
    sections: list[tuple[str, str]],
    llm_client: "Any",
    model: str,
) -> str:
    """Summarize multiple knowledge sections into a condensed form using LLM.

    Returns condensed markdown or empty string on failure.
    """
    combined = "\n\n".join(f"### {sid}\n{content[:2000]}" for sid, content in sections)
    if len(combined) > 8000:
        combined = combined[:8000] + "\n\n<!-- truncated for summarization -->"

    prompt = (
        "以下のDBSQL チューニングナレッジセクションを、重要なポイントのみ箇条書きで要約してください。"
        "SQLパラメータ名や閾値などの具体的な値は省略せず残してください。"
        "500文字以内で出力してください。\n\n"
        f"{combined}"
    )

    try:
        response = llm_client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=512,
            temperature=0.1,
            timeout=30,
        )
        content = response.choices[0].message.content or ""
        if content:
            logger.info("Knowledge summarization successful: %d chars", len(content))
            return f"## 参考ナレッジ（要約版）\n\n{content}"
    except Exception as e:
        logger.warning("Knowledge summarization failed: %s", e)

    return ""


# =============================================================================
# Spark Perf knowledge filtering (by bottleneck type)
# =============================================================================


def filter_spark_knowledge(
    tuning_knowledge: str,
    bottleneck_types: list[str],
    max_chars: int = 0,
    llm_client: Any = None,
    llm_model: str = "",
) -> str:
    """Filter Spark tuning knowledge by detected bottleneck types.

    Uses SPARK_CATEGORY_TO_SECTION_IDS to select relevant sections.
    Falls back to full knowledge if parsing yields nothing.
    """
    sections = parse_knowledge_sections(tuning_knowledge)
    if not sections:
        return tuning_knowledge

    # Score sections by bottleneck type
    scores: dict[str, int] = {}
    for sid in SPARK_ALWAYS_INCLUDE_SECTION_IDS:
        if sid in sections:
            scores[sid] = 100

    for bt in bottleneck_types:
        bt_upper = bt.upper() if bt else ""
        sids = SPARK_CATEGORY_TO_SECTION_IDS.get(bt_upper, [])
        for sid in sids:
            if sid in sections:
                scores[sid] = max(scores.get(sid, 0), 5)

    if not scores:
        # No mapping found — include all sections
        return tuning_knowledge

    relevant_ids = set(scores.keys())
    result = _join_sections(sections, relevant_ids)

    logger.info(
        "Spark knowledge routing: %d bottleneck types -> %d/%d sections selected (%d chars)",
        len(bottleneck_types),
        len(relevant_ids),
        len(sections),
        len(result),
    )

    # Enforce character budget with priority-aware trimming
    if max_chars > 0 and len(result) > max_chars:
        logger.warning(
            "Spark knowledge %d chars exceeds budget %d, applying priority-based trimming",
            len(result),
            max_chars,
        )
        result = _trim_by_priority(
            sections,
            scores,
            max_chars,
            llm_client=llm_client,
            llm_model=llm_model,
        )

    return result
