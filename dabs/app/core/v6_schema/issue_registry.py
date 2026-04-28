"""V6 issue_id single-source registry (W2.5 #5).

Centralizes the controlled vocabulary that was previously duplicated across:
- docs/v6/output_contract.md (text)
- core/v6_schema/normalizer.py (heuristics)
- eval/goldens/cases/*.yaml (must_cover_issues[].id)

Anything that wants to refer to an issue (Finding.issue_id, golden case
must_cover_issues[].id, scorer recall keys) MUST use one of these constants.

Lookup helpers:
- ALL_ISSUE_IDS: set of valid ids
- ISSUE_BY_ID: id -> IssueDef
- ISSUE_BY_CATEGORY: category -> list[IssueDef]
- get_keywords(issue_id): canonical search keywords
- is_known(issue_id): bool
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class IssueDef:
    """Definition of a canonical issue.

    Fields:
      id: snake_case stable identifier (matches Finding.issue_id and
          golden case must_cover_issues[].id).
      category: must be one of schemas/report_v6.schema.json category enum.
      severity_default: typical severity emitted (advisory).
      keywords: case-insensitive substrings used by recall.score_recall
                to detect coverage in report text.
      description: short human description for docs/scorer reasoning.
    """

    id: str
    category: str
    severity_default: str
    keywords: tuple[str, ...] = field(default_factory=tuple)
    description: str = ""


# Canonical registry. Order is documentation-friendly, not load-bearing.
ISSUES: tuple[IssueDef, ...] = (
    # memory / spill
    IssueDef("spill_dominant", "memory", "high",
             ("spill", "peak_memory", "ディスクスピル"),
             "シャッフル/集計でディスクスピルが支配的"),
    # shuffle
    IssueDef("shuffle_volume", "shuffle", "medium",
             ("shuffle", "シャッフル"),
             "シャッフルバイト量が大きい"),
    IssueDef("shuffle_dominant", "shuffle", "high",
             ("shuffle", "シャッフル", "redistribution"),
             "シャッフル時間が支配的"),
    IssueDef("hash_resize_dominant", "shuffle", "medium",
             ("hash resize", "ハッシュテーブル", "リサイズ"),
             "Hash table resize がホットスポット"),
    # skew
    IssueDef("data_skew", "skew", "medium",
             ("skew", "偏り", "partition"),
             "パーティション偏り"),
    IssueDef("aqe_handled_skew", "skew", "low",
             ("aqe", "skew", "handled", "解消"),
             "AQE が skew を解消済み (negative)"),
    # photon
    IssueDef("photon_partial_fallback", "photon", "high",
             ("photon", "fallback", "非photon"),
             "Photon 部分 fallback"),
    IssueDef("photon_blocker_via_cast", "photon", "high",
             ("cast", "photon", "blocker"),
             "暗黙 CAST で Photon 阻害"),
    # scan / clustering
    IssueDef("low_file_pruning", "scan", "high",
             ("pruning", "files_pruned", "データスキッピング"),
             "ファイル/パーティションプルーニング不足"),
    IssueDef("large_scan_volume", "scan", "medium",
             ("scan", "bytes_read", "読み込み"),
             "読み込みバイトが大きい"),
    IssueDef("full_scan_large_table", "scan", "high",
             ("フルスキャン", "スキャン"),
             "大規模テーブルでフルスキャン"),
    IssueDef("missing_clustering", "clustering", "high",
             ("clustering", "クラスタリング", "パーティション"),
             "クラスタリング/パーティションなし"),
    # cache
    IssueDef("low_cache_hit", "cache", "medium",
             ("cache", "キャッシュ", "hit_ratio"),
             "Delta cache hit ratio が低い"),
    IssueDef("cold_node_possibility", "cache", "medium",
             ("cold", "scale-out", "non-local", "ノードローカリティ"),
             "Cold node 由来の可能性"),
    IssueDef("result_from_cache_detected", "cache", "low",
             ("result cache", "結果キャッシュ", "cached"),
             "結果キャッシュヒット"),
    # federation
    IssueDef("federation_detected", "federation", "low",
             ("federation", "リモート", "remote_query"),
             "Federation クエリ認識"),
    # streaming
    IssueDef("streaming_detected", "streaming", "low",
             ("streaming", "ストリーミング", "micro-batch", "マイクロバッチ"),
             "ストリーミングクエリ認識"),
    IssueDef("micro_batch_throughput", "streaming", "medium",
             ("throughput", "バッチ間隔", "batch", "スループット"),
             "マイクロバッチ throughput"),
    # driver / compilation
    IssueDef("driver_overhead", "driver", "medium",
             ("driver", "ドライバー", "overhead", "オーバーヘッド"),
             "Driver overhead 支配"),
    IssueDef("compilation_overhead", "compilation", "medium",
             ("compilation", "コンパイル", "plan"),
             "Compilation 時間支配"),
    IssueDef("driver_overhead_or_compilation", "other", "medium",
             ("driver", "compilation", "overhead"),
             "Federation での driver / compilation 支配 (総称)"),
    # serverless / compute
    IssueDef("serverless_detected", "other", "low",
             ("serverless", "サーバーレス"),
             "Serverless warehouse 認識"),
    IssueDef("cluster_underutilization", "compute", "medium",
             ("parallelism", "並列度", "utilization"),
             "並列度活用不足"),
    # SQL pattern
    IssueDef("implicit_cast_on_join_key", "sql_pattern", "high",
             ("cast", "暗黙", "join key", "型不一致"),
             "Join key 暗黙 CAST"),
    IssueDef("cte_recompute", "sql_pattern", "medium",
             ("cte", "再計算", "reusedexchange"),
             "CTE 再計算 (ReusedExchange なし)"),
    IssueDef("row_count_explosion", "sql_pattern", "critical",
             ("行数", "output rows", "cross join", "デカルト"),
             "Join 後行数爆発"),
    IssueDef("missing_join_predicate", "sql_pattern", "high",
             ("predicate", "on条件", "結合条件"),
             "Join 条件不足"),
    # cardinality / stats
    IssueDef("cardinality_estimate_off", "cardinality", "medium",
             ("推定", "estimate", "cardinality"),
             "Cardinality 推定が外れている"),
    # data type review (V6 alert coverage expansion, 2026-04-26)
    IssueDef("decimal_heavy_aggregate", "sql_pattern", "medium",
             ("DECIMAL", "decimal", "DESCRIBE TABLE", "型最適化"),
             "Heavy aggregate with arithmetic on wide DECIMAL columns"),
    # join / write
    IssueDef("merge_join_efficiency", "join", "medium",
             ("merge", "マージ", "join"),
             "MERGE source/target join 効率"),
    IssueDef("write_side_optimization", "other", "medium",
             ("書き込み", "write", "optimizewrite", "autocompact"),
             "書き込み側最適化"),
)


ISSUE_BY_ID: dict[str, IssueDef] = {i.id: i for i in ISSUES}
ALL_ISSUE_IDS: frozenset[str] = frozenset(ISSUE_BY_ID.keys())

ISSUE_BY_CATEGORY: dict[str, list[IssueDef]] = {}
for _i in ISSUES:
    ISSUE_BY_CATEGORY.setdefault(_i.category, []).append(_i)


def is_known(issue_id: str) -> bool:
    """True if `issue_id` is in the canonical registry."""
    return issue_id in ALL_ISSUE_IDS


def get_keywords(issue_id: str) -> tuple[str, ...]:
    """Return canonical keywords for an issue_id, or () if unknown."""
    defn = ISSUE_BY_ID.get(issue_id)
    return defn.keywords if defn else ()


def get_definition(issue_id: str) -> IssueDef | None:
    return ISSUE_BY_ID.get(issue_id)
