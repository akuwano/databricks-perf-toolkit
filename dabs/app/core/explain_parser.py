"""EXPLAIN EXTENDED text parser.

Databricks SQL の `EXPLAIN EXTENDED` 出力（テキスト）をセクション分割し、
Logical / Physical plan の主要ノードと属性を抽出する。

主な機能:
- セクション検出（== ... ==）
- Logical/Physical plan の行をノードとして分類
- DataFilters / PartitionFilters / ReadSchema / keys / functions 等の抽出
- Photon Explanation の非対応関数/操作の抽出
- Optimizer Statistics の統計状態（missing/partial/full）の抽出
- Exchange のパーティション種別・数の抽出
- Relation からのテーブル名/カラム/フォーマットの抽出

注意:
- Spark/Databricks の plan 形式はバージョンや設定で揺れるため、正規表現は
  「壊れにくいこと」を優先し、厳密なAST化はしない。
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import StrEnum

SECTION_HEADER_RE = re.compile(r"^==\s*(?P<section>[^=]+?)\s*==\s*$")


class PlanKind(StrEnum):
    LOGICAL = "logical"
    PHYSICAL = "physical"
    OTHER = "other"


class NodeFamily(StrEnum):
    # Physical-plan oriented
    ADAPTIVE = "adaptive"
    STAGE = "stage"
    SHUFFLE = "shuffle"
    EXCHANGE = "exchange"
    JOIN = "join"
    SCAN = "scan"
    AGG = "agg"
    WINDOW = "window"
    SORT = "sort"
    FILTER = "filter"
    PROJECT = "project"
    LIMIT = "limit"
    CAST = "cast"
    UNKNOWN = "unknown"


# -----------------------------
# 1) セクション検出（正規表現）
# -----------------------------

# 例: "== Physical Plan =="
# Spark/Databricks の explain は前後空白が入り得るため緩めに。
RE_SECTION_DETECT = SECTION_HEADER_RE


# ----------------------------------
# 2) Physical Plan ノード分類（正規表現）
# ----------------------------------

# 行頭の tree 記号/インデントを吸収: "+-", ":-", ":", "|", "*" など。
# 複数回繰り返す場合があるため、繰り返しパターンで吸収する。
RE_PLAN_PREFIX = re.compile(r"^(?P<prefix>(?:\s*(?:\+-|:-|:\s*|\|\s*|\*\(|\*\s))*\s*)")

# Physical の主要ノード（提示いただいた実データベース）
RE_PHYS_ADAPTIVE = re.compile(r"^AdaptiveSparkPlan\b")
RE_PHYS_COLUMNAR_TO_ROW = re.compile(r"^ColumnarToRow\b")
RE_PHYS_PHOTON_RESULT_STAGE = re.compile(r"^PhotonResultStage\b")
RE_PHYS_PHOTON_SORT = re.compile(r"^PhotonSort\b")
RE_PHYS_PHOTON_SHUFFLE_EXCHANGE_SOURCE = re.compile(r"^PhotonShuffleExchangeSource\b")
RE_PHYS_PHOTON_SHUFFLE_MAP_STAGE = re.compile(r"^PhotonShuffleMapStage\b")
RE_PHYS_PHOTON_SHUFFLE_EXCHANGE_SINK = re.compile(r"^PhotonShuffleExchangeSink\b")
RE_PHYS_PHOTON_FILTER = re.compile(r"^PhotonFilter\b")
RE_PHYS_PHOTON_WINDOW = re.compile(r"^PhotonWindow\b")
RE_PHYS_PHOTON_TOPK = re.compile(r"^PhotonTopK\b")
RE_PHYS_PHOTON_GROUPING_AGG = re.compile(r"^PhotonGroupingAgg\b")
RE_PHYS_PHOTON_PROJECT = re.compile(r"^PhotonProject\b")
RE_PHYS_PHOTON_BROADCAST_HASH_JOIN = re.compile(r"^PhotonBroadcastHashJoin\b")
RE_PHYS_PHOTON_SCAN = re.compile(r"^PhotonScan\b")


# ---------------------------------
# 3) Logical Plan ノード分類（正規表現）
# ---------------------------------

RE_LOGICAL_WITH_CTE = re.compile(r"^WithCTE\b")
RE_LOGICAL_CTE = re.compile(r"^CTE\b")
RE_LOGICAL_CTE_DEF = re.compile(r"^CTERelationDef\b")
RE_LOGICAL_CTE_REF = re.compile(r"^CTERelationRef\b")
RE_LOGICAL_SUBQUERY_ALIAS = re.compile(r"^SubqueryAlias\b")
RE_LOGICAL_AGGREGATE = re.compile(r"^Aggregate\b")
RE_LOGICAL_FILTER = re.compile(r"^Filter\b")
RE_LOGICAL_JOIN = re.compile(r"^Join\b")
RE_LOGICAL_UNRESOLVED_RELATION = re.compile(r"^UnresolvedRelation\b")
RE_LOGICAL_RELATION = re.compile(r"^Relation\b")
RE_LOGICAL_WINDOW = re.compile(r"^Window\b")
RE_LOGICAL_PROJECT = re.compile(r"^Project\b")
RE_LOGICAL_SORT = re.compile(r"^Sort\b")


# -------------------------------------------
# 4) 属性抽出の正規表現（DataFilters等）
# -------------------------------------------

# 例:
# - DataFilters: [isnotnull(a#1), (b#2 > 10)]
# - PartitionFilters: []
# - DictionaryFilters: [...]
# - ReadSchema: struct<...>
# - Location: ...
RE_ATTR_LIST = re.compile(
    r"\b(?P<key>DataFilters|PartitionFilters|DictionaryFilters)\s*:\s*\[(?P<value>[^\]]*)\]"
)
RE_ATTR_READ_SCHEMA = re.compile(r"\bReadSchema\s*:\s*(?P<value>.+?)\s*$")
RE_ATTR_LOCATION = re.compile(r"\bLocation\s*:\s*(?P<value>.+?)\s*$")
RE_ATTR_FORMAT = re.compile(r"\bFormat\s*:\s*(?P<value>\w+)\b")

# PhotonGroupingAgg / Project / Window / Sort などの括弧属性
# 例: keys=[...], functions=[...], output=[...]
RE_KV_BRACKET = re.compile(
    r"\b(?P<key>keys|functions|output|limit|partitioning|ordering)\s*=\s*(?P<value>\[[^\]]*\]|[^,\)]+)"
)

# DFPPlaceholder — Dynamic File Pruning estimated selectivity
# 例: DFPPlaceholder(joinId=33, estimatedSelectivity=0.31, pruningKey=..., buildKey=...)
RE_DFP_SELECTIVITY = re.compile(r"DFPPlaceholder\(.*?estimatedSelectivity=(?P<sel>[\d.]+|None)")
# Runtime Filter — hashedrelationcontains in OptionalDataFilters
RE_RUNTIME_FILTER = re.compile(r"hashedrelationcontains\((?P<key>[^)]+)\)")

# ---------------------------------
# 5) Exchange 属性抽出（パーティション種別・数）
# ---------------------------------
# 例: Exchange hashpartitioning(L_SHIPINSTRUCT#3139, 200), ENSURE_REQUIREMENTS, [plan_id=1632]
# 例: Exchange rangepartitioning(L_SHIPINSTRUCT#3139 ASC NULLS FIRST, 200)
RE_EXCHANGE_PARTITIONING = re.compile(
    r"(?P<type>hash|range)partitioning\s*\((?P<expr>.*?),\s*(?P<num>\d+)\)"
)
RE_PLAN_ID = re.compile(r"\[plan_id=(?P<id>\d+)\]")
RE_ENSURE_REQUIREMENTS = re.compile(r"ENSURE_REQUIREMENTS")

# AdaptiveSparkPlan isFinalPlan=false
RE_IS_FINAL_PLAN = re.compile(r"isFinalPlan\s*=\s*(?P<value>true|false)", re.IGNORECASE)

# ---------------------------------
# 5b) CTE / Reused exchange patterns
# ---------------------------------
# 例: CTERelationRef 16 [references: 3], true
RE_CTE_REF_COUNT = re.compile(
    r"CTERelationRef\s+(?P<cte_id>\d+)\s*\[references:\s*(?P<count>\d+)\]"
)
RE_REUSED_EXCHANGE = re.compile(r"\bReusedExchange\b")

# ---------------------------------
# 5c) Join strategy patterns
# ---------------------------------
# 例: BuildRight / BuildLeft (join build side)
RE_JOIN_BUILD_SIDE = re.compile(r"\bBuild(?P<side>Right|Left)\b")
# 例: Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti
RE_JOIN_TYPE = re.compile(
    r",\s*(?P<jtype>Inner|LeftOuter|RightOuter|FullOuter|LeftSemi|LeftAnti|Cross)\b"
)
# 例: EXECUTOR_BROADCAST (Databricks 独自の broadcast モード)
RE_EXECUTOR_BROADCAST = re.compile(r"\bEXECUTOR_BROADCAST\b")
RE_SINGLE_PARTITION = re.compile(r"\bSinglePartition\b")

# ---------------------------------
# 5d) Filter pushdown kinds
# ---------------------------------
# DataFilters / PartitionFilters / DictionaryFilters は既出 RE_ATTR_LIST
# で拾えるが、Optional/Required の2種は別パターンが必要
RE_OPTIONAL_DATA_FILTERS = re.compile(r"\bOptionalDataFilters\s*:\s*\[(?P<value>[^\]]*)\]")
RE_REQUIRED_DATA_FILTERS = re.compile(r"\bRequiredDataFilters\s*:\s*\[(?P<value>[^\]]*)\]")
# PartitionFilters: [] — empty explicitly
RE_EMPTY_PARTITION_FILTERS = re.compile(r"\bPartitionFilters\s*:\s*\[\s*\]")

# ---------------------------------
# 5e) Implicit CAST detection
# ---------------------------------
# 例: cast(d_year#1 as decimal(38,0))
# as の後は decimal(38,0) や bigint のような1トークン型を許容
RE_IMPLICIT_CAST = re.compile(
    r"cast\(\s*(?P<col>[A-Za-z_][A-Za-z0-9_]*#\d+)"
    r"\s+as\s+"
    r"(?P<to_type>[A-Za-z_][A-Za-z0-9_]*(?:\([^)]*\))?)"
    r"\s*\)",
    re.IGNORECASE,
)

# ---------------------------------
# 5f) Aggregate phase markers
# ---------------------------------
# partial_sum / partial_avg / partial_count / partial_min / partial_max / partial_collect_list / ...
RE_PARTIAL_AGG_FN = re.compile(r"\bpartial_[a-z_]+\s*\(", re.IGNORECASE)
# finalmerge_sum / finalmerge_count など
RE_FINAL_MERGE_AGG_FN = re.compile(r"\bfinalmerge_[a-z_]+\s*\(", re.IGNORECASE)
# AggregatePart=Partial|Final|Complete
RE_AGG_PART_KV = re.compile(r"AggregatePart\s*=\s*(?P<part>Partial|Final|Complete|PartialMerge)")

# ---------------------------------
# 5g) Column-type extraction (crucial for rewrite correctness)
# ---------------------------------

# ReadSchema: struct<colA:int,colB:decimal(38,2),colC:string>
# The `body` capture will be parsed with a depth counter (decimal(38,2) contains
# a comma, which a naive `split(",")` would corrupt).
RE_READ_SCHEMA_STRUCT = re.compile(
    r"ReadSchema\s*:\s*struct<(?P<body>.*?)>\s*(?=$|[,\s])", re.DOTALL
)


def _parse_struct_body(body: str) -> dict[str, str]:
    """Parse a struct<...> body string into {col: type}.

    Handles nested parens so that ``decimal(38,2)`` and ``array<int>`` are
    kept together instead of being split on internal commas.
    """
    result: dict[str, str] = {}
    depth = 0
    cur = []
    fields: list[str] = []
    for ch in body:
        if ch in "(<":
            depth += 1
            cur.append(ch)
        elif ch in ")>":
            depth -= 1
            cur.append(ch)
        elif ch == "," and depth == 0:
            fields.append("".join(cur).strip())
            cur = []
        else:
            cur.append(ch)
    if cur:
        fields.append("".join(cur).strip())
    for f in fields:
        if ":" not in f:
            continue
        name, _, type_str = f.partition(":")
        name = name.strip()
        type_str = type_str.strip()
        if name and type_str:
            result[name] = type_str
    return result


# ---------------------------------
# 6) Relation 属性抽出（テーブル名/カラム/フォーマット）
# ---------------------------------
# 例: Relation main.base.lineitem[L_ORDERKEY#3126,...] parquet
RE_RELATION_FULL = re.compile(
    r"Relation\s+(?P<table>\S+?)\[(?P<columns>[^\]]+)\]\s+(?P<format>\w+)"
)

# ---------------------------------
# 7) Photon Explanation パース用
# ---------------------------------
# 例: pivotfirst(...) is not supported:
# ネストした括弧に対応するため、"is not supported" の直前までを式として取得
RE_PHOTON_UNSUPPORTED = re.compile(r"(?P<expr>.+?)\s+is\s+not\s+supported")
# 例: Unsupported aggregation function pivotfirst for aggregation mode: Partial
# category: "aggregation function", name: "pivotfirst", detail: "for aggregation mode: Partial"
RE_PHOTON_REASON = re.compile(
    r"Unsupported\s+(?P<category>\w+\s+function)\s+(?P<name>\w+)\s*(?P<detail>.*)"
)
# Reference node:
RE_PHOTON_REFERENCE_NODE = re.compile(r"Reference\s+node\s*:", re.IGNORECASE)

# ---------------------------------
# 8) Optimizer Statistics パース用
# ---------------------------------
# 例: missing = table1, table2
# 例: partial = lineitem
# 例: full    =
# フラット形式では "full    =Corrective actions:" のように続くため、
# "Corrective" や大文字始まりの単語で止める
RE_STATS_STATE = re.compile(
    r"(?P<state>missing|partial|full)\s*=\s*(?P<tables>[^\n]*?)(?=\s*(?:missing|partial|full|Corrective|ANALYZE)\s*|$)",
    re.IGNORECASE,
)
# ANALYZE TABLE <table-name> COMPUTE STATISTICS FOR ALL COLUMNS
RE_ANALYZE_TABLE = re.compile(r"ANALYZE\s+TABLE\s+", re.IGNORECASE)


@dataclass
class ExplainNode:
    raw_line: str
    indent: int
    kind: PlanKind
    node_name: str
    family: NodeFamily = NodeFamily.UNKNOWN
    attrs: dict[str, str] = field(default_factory=dict)


@dataclass
class PhotonUnsupportedItem:
    """Photon非対応の関数/操作."""

    expression: str = ""
    reason: str = ""
    category: str = ""  # e.g., "aggregation function"
    detail: str = ""  # e.g., "for aggregation mode: Partial"


@dataclass
class PhotonExplanation:
    """Photon Explanationセクションの解析結果."""

    fully_supported: bool = True
    unsupported_items: list[PhotonUnsupportedItem] = field(default_factory=list)
    reference_nodes: list[str] = field(default_factory=list)
    raw_text: str = ""


@dataclass
class OptimizerStatistics:
    """Optimizer Statisticsセクションの解析結果."""

    missing_tables: list[str] = field(default_factory=list)
    partial_tables: list[str] = field(default_factory=list)
    full_tables: list[str] = field(default_factory=list)
    recommended_action: str = ""
    raw_text: str = ""


@dataclass
class ColumnStat:
    """Per-column statistics parsed from EXPLAIN EXTENDED.

    Populated only when ``ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL
    COLUMNS`` has been run on the underlying table; otherwise all
    fields are None. Used primarily for cardinality classification of
    clustering key candidates (Hierarchical Clustering detection).
    """

    distinct_count: int | None = None
    min_value: str | None = None
    max_value: str | None = None
    null_count: int | None = None


@dataclass
class RelationInfo:
    """Logical PlanのRelationから抽出したテーブル情報."""

    table_name: str = ""
    columns: list[str] = field(default_factory=list)
    format: str = ""


@dataclass
class ExchangeInfo:
    """Physical PlanのExchangeから抽出したシャッフル情報."""

    partitioning_type: str = ""  # "hash" or "range"
    num_partitions: int = 0
    partition_keys: str = ""
    plan_id: int = 0
    ensure_requirements: bool = False


@dataclass
class CteReuseInfo:
    """CTERelationRef から抽出した CTE 参照情報.

    reference_count が 1 なら「実質 inlining 可能」、2以上なら「materialize
    もしくは再計算されているか」を別途 ReusedExchange で判定する。
    """

    cte_id: str = ""
    reference_count: int = 0


@dataclass
class JoinStrategy:
    """Physical Plan の Join ノードから抽出した戦略情報."""

    node_name: str = ""
    join_type: str = ""  # Inner, LeftOuter, ...
    build_side: str = ""  # "Right" | "Left" | ""
    is_broadcast: bool = False
    broadcast_mode: str = ""  # "EXECUTOR_BROADCAST" | "SinglePartition" | ""
    raw_line: str = ""


@dataclass
class FilterPushdownInfo:
    """Scan ノードの push-down 完全性情報.

    - has_*: それぞれの filter 種別が非空で存在するか
    - partition_filters_empty: PartitionFilters: [] が明示されている（= partition
      pruning が効いていない）場合 True
    """

    table_name: str = ""
    has_data_filters: bool = False
    has_partition_filters: bool = False
    has_dictionary_filters: bool = False
    has_optional_filters: bool = False  # runtime filter / DFP など
    has_required_filters: bool = False
    partition_filters_empty: bool = False
    raw_line: str = ""


@dataclass
class ImplicitCastSite:
    """プランに埋め込まれた暗黙 CAST の1箇所を表す.

    context は "join" / "filter" / "aggregate" / "project" / "scan" / "other".
    書き換え時の優先度判定に使用する（join/filter での CAST は型不一致の
    直接証拠、project での CAST は害が少ない）。
    """

    context: str = "other"
    column_ref: str = ""  # e.g., "d_year#123"
    to_type: str = ""  # e.g., "decimal(38,0)"
    from_type: str = ""  # 推測が難しいので空のことが多い
    node_name: str = ""
    raw_expression: str = ""


@dataclass
class AggregatePhaseInfo:
    """Aggregate ノードの phase 情報."""

    node_name: str = ""
    has_partial_functions: bool = False  # partial_sum(...) 等
    has_final_merge: bool = False  # finalmerge_sum(...) 等
    agg_part: str = ""  # "Partial" / "Final" / "Complete" / "PartialMerge"
    raw_line: str = ""


@dataclass
class PhotonFallbackOp:
    """Photon プレフィックスなしで物理プランに現れた演算子.

    Photon Explanation セクションを補完する in-plan のフォールバック検出。
    ColumnarToRow や AdaptiveSparkPlan 等の境界演算子は除外する。
    """

    node_name: str = ""
    raw_line: str = ""


@dataclass
class ExplainSection:
    name: str
    lines: list[str] = field(default_factory=list)
    nodes: list[ExplainNode] = field(default_factory=list)


@dataclass
class ExplainExtended:
    sections: list[ExplainSection] = field(default_factory=list)
    photon_explanation: PhotonExplanation | None = None
    optimizer_statistics: OptimizerStatistics | None = None
    relations: list[RelationInfo] = field(default_factory=list)
    exchanges: list[ExchangeInfo] = field(default_factory=list)
    is_adaptive: bool = False
    is_final_plan: bool = False
    # Phase-1 v2 insights ------------------------------------------------
    cte_references: list[CteReuseInfo] = field(default_factory=list)
    has_reused_exchange: bool = False
    join_strategies: list[JoinStrategy] = field(default_factory=list)
    filter_pushdown: list[FilterPushdownInfo] = field(default_factory=list)
    implicit_cast_sites: list[ImplicitCastSite] = field(default_factory=list)
    aggregate_phases: list[AggregatePhaseInfo] = field(default_factory=list)
    photon_fallback_ops: list[PhotonFallbackOp] = field(default_factory=list)
    # Column types per table, parsed from `ReadSchema: struct<...>` on scan
    # nodes. Key is the fully qualified table name.
    scan_schemas: dict[str, dict[str, str]] = field(default_factory=dict)
    # Per-column statistics per table (v5.16.17). Populated from EXPLAIN
    # EXTENDED when ANALYZE TABLE ... FOR ALL COLUMNS has been run.
    # Outer key: fully qualified table name; inner key: column name.
    scan_column_stats: dict[str, dict[str, ColumnStat]] = field(default_factory=dict)

    def get_section(self, name: str) -> ExplainSection | None:
        for s in self.sections:
            if s.name == name:
                return s
        return None

    def get_section_by_kind(self, kind: PlanKind) -> ExplainSection | None:
        """指定されたPlanKindのセクションを取得（最初にマッチしたもの）."""
        for s in self.sections:
            if _section_kind(s.name) == kind:
                return s
        return None


def _section_kind(name: str) -> PlanKind:
    normalized = name.strip().lower()
    if normalized.endswith("logical plan"):
        return PlanKind.LOGICAL
    if normalized.endswith("physical plan"):
        return PlanKind.PHYSICAL
    return PlanKind.OTHER


def split_sections(text: str) -> list[ExplainSection]:
    """EXPLAIN EXTENDEDテキストをセクションに分割.

    改行なしのフラット形式（UIからコピーした場合等）にも対応。
    """
    # Normalize: セクションヘッダ "== ... ==" の前後に改行を挿入
    # これにより、改行なしのフラット形式でも正しく分割できる
    if "==" in text:
        # セクションヘッダの前後に改行を挿入（既に改行がある場合は重複しても問題なし）
        text = re.sub(r"(==\s*[^=]+?\s*==)", r"\n\1\n", text)

    current: ExplainSection | None = None
    sections: list[ExplainSection] = []

    for line in text.splitlines():
        line = line.rstrip()
        if not line:
            continue

        m = RE_SECTION_DETECT.match(line)
        if m:
            current = ExplainSection(name=m.group("section").strip())
            sections.append(current)
            continue

        if current is None:
            # explainの先頭に説明文がある場合もあるが捨てる
            continue

        current.lines.append(line)

    return sections


def _strip_plan_prefix(line: str) -> tuple[str, int]:
    stripped = line.rstrip()
    if not stripped:
        return "", 0

    m = RE_PLAN_PREFIX.match(stripped)
    if not m:
        return stripped.lstrip(), len(stripped) - len(stripped.lstrip())

    prefix = m.group("prefix")
    rest = stripped[len(prefix) :]
    indent = len(prefix)
    return rest.strip(), indent


def _node_family_for_physical(node_name: str) -> NodeFamily:
    if RE_PHYS_ADAPTIVE.match(node_name):
        return NodeFamily.ADAPTIVE
    if RE_PHYS_PHOTON_RESULT_STAGE.match(node_name) or RE_PHYS_PHOTON_SHUFFLE_MAP_STAGE.match(
        node_name
    ):
        return NodeFamily.STAGE
    if RE_PHYS_PHOTON_SHUFFLE_EXCHANGE_SOURCE.match(
        node_name
    ) or RE_PHYS_PHOTON_SHUFFLE_EXCHANGE_SINK.match(node_name):
        return NodeFamily.EXCHANGE
    if "Shuffle" in node_name:
        return NodeFamily.SHUFFLE
    if "Join" in node_name:
        return NodeFamily.JOIN
    if RE_PHYS_PHOTON_SCAN.match(node_name):
        return NodeFamily.SCAN
    if "Agg" in node_name:
        return NodeFamily.AGG
    if "Window" in node_name:
        return NodeFamily.WINDOW
    if "Sort" in node_name:
        return NodeFamily.SORT
    if "Filter" in node_name:
        return NodeFamily.FILTER
    if "Project" in node_name:
        return NodeFamily.PROJECT
    if "Limit" in node_name or "TopK" in node_name:
        return NodeFamily.LIMIT
    if node_name == "ColumnarToRow":
        return NodeFamily.CAST
    return NodeFamily.UNKNOWN


def _node_family_for_logical(node_name: str) -> NodeFamily:
    if "Join" in node_name:
        return NodeFamily.JOIN
    if "Aggregate" in node_name or node_name.endswith("Agg"):
        return NodeFamily.AGG
    if "Window" in node_name:
        return NodeFamily.WINDOW
    if "Sort" in node_name:
        return NodeFamily.SORT
    if "Filter" in node_name:
        return NodeFamily.FILTER
    if "Project" in node_name:
        return NodeFamily.PROJECT
    if "Relation" in node_name:
        return NodeFamily.SCAN
    return NodeFamily.UNKNOWN


def _extract_attrs(line: str) -> dict[str, str]:
    attrs: dict[str, str] = {}

    for m in RE_ATTR_LIST.finditer(line):
        attrs[m.group("key")] = m.group("value").strip()

    rs_match = RE_ATTR_READ_SCHEMA.search(line)
    if rs_match:
        attrs["ReadSchema"] = rs_match.group("value").strip()

    loc_match = RE_ATTR_LOCATION.search(line)
    if loc_match:
        attrs["Location"] = loc_match.group("value").strip()

    fmt_match = RE_ATTR_FORMAT.search(line)
    if fmt_match:
        attrs["Format"] = fmt_match.group("value").strip()

    for m in RE_KV_BRACKET.finditer(line):
        attrs[m.group("key")] = m.group("value").strip()

    # DFPPlaceholder — Dynamic File Pruning estimated selectivity
    for m in RE_DFP_SELECTIVITY.finditer(line):
        attrs["DFPSelectivity"] = m.group("sel")

    # Runtime Filter — hashedrelationcontains in OptionalDataFilters
    rf_keys = RE_RUNTIME_FILTER.findall(line)
    if rf_keys:
        attrs["RuntimeFilter"] = ", ".join(rf_keys)

    return attrs


# ノードではなくスキップすべきパターン
RE_SKIP_PATTERNS = re.compile(r"^(?:==.*==|\+-|:-|:|\||\*\(|\*\s*|Initial Plan)$")


def _classify_node_name(kind: PlanKind, line_wo_prefix: str) -> str:
    # ノード名は通常「先頭トークン」だが、"PhotonScan parquet ..." のように
    # 2トークン目が重要な場合があるため最小限だけ補正する。
    if not line_wo_prefix:
        return ""

    # ツリー記号や内部セクションヘッダはスキップ
    if RE_SKIP_PATTERNS.match(line_wo_prefix.strip()):
        return ""

    tokens = line_wo_prefix.split()
    head = tokens[0]

    # ツリー記号だけの場合はスキップ
    if head in ("+-", ":-", ":", "|", "=="):
        return ""

    # PhotonScan is a physical operator but real Databricks EXPLAIN EXTENDED
    # can place it in the "Optimized Logical Plan" section too. We must
    # recover the table name regardless of the section kind.
    if head == "PhotonScan" and len(tokens) >= 2:
        # PhotonScan parquet users.schema.table[...] -> PhotonScan parquet users.schema.table
        format_token = tokens[1]
        if len(tokens) >= 3:
            # テーブル名から[以降を除去
            table_token = tokens[2].split("[")[0]
            return f"{head} {format_token} {table_token}"
        return f"{head} {format_token}"

    return head


def parse_plan_lines(lines: Iterable[str], kind: PlanKind) -> list[ExplainNode]:
    nodes: list[ExplainNode] = []
    for raw in lines:
        line = raw.rstrip()
        if not line.strip():
            continue

        line_wo_prefix, indent = _strip_plan_prefix(line)
        if not line_wo_prefix:
            continue

        node_name = _classify_node_name(kind, line_wo_prefix)
        if not node_name:
            continue

        family = (
            _node_family_for_physical(node_name)
            if kind == PlanKind.PHYSICAL
            else _node_family_for_logical(node_name)
            if kind == PlanKind.LOGICAL
            else NodeFamily.UNKNOWN
        )

        nodes.append(
            ExplainNode(
                raw_line=line,
                indent=indent,
                kind=kind,
                node_name=node_name,
                family=family,
                attrs=_extract_attrs(line_wo_prefix),
            )
        )

    return nodes


def parse_photon_explanation(lines: list[str]) -> PhotonExplanation:
    """Photon Explanationセクションをパース.

    抽出する情報:
    - 非対応の関数/操作とその理由
    - Reference node

    フラット形式（タブ区切り）にも対応。
    """
    raw_text = "\n".join(lines)
    result = PhotonExplanation(raw_text=raw_text)

    # "Photon does not fully support" があれば fully_supported = False
    if "does not fully support" in raw_text.lower():
        result.fully_supported = False

    # フラット形式対応: タブで分割して個別の行として処理
    expanded_lines: list[str] = []
    for line in lines:
        # タブで分割
        parts = line.split("\t")
        for part in parts:
            part = part.strip()
            if part:
                # "Reference node:" が途中にある場合は分割
                # 例: "Unsupported ... Partial.Reference node:" -> 2つに分割
                ref_match = RE_PHOTON_REFERENCE_NODE.search(part)
                if ref_match:
                    before = part[: ref_match.start()].strip()
                    after = part[ref_match.start() :].strip()
                    if before:
                        expanded_lines.append(before)
                    if after:
                        expanded_lines.append(after)
                else:
                    expanded_lines.append(part)

    # 非対応項目を抽出
    current_item: PhotonUnsupportedItem | None = None
    reference_node_lines: list[str] = []
    in_reference_node = False

    for line in expanded_lines:
        # Reference node: の検出
        if RE_PHOTON_REFERENCE_NODE.search(line):
            in_reference_node = True
            # "Reference node:" の後に内容がある場合
            after_marker = re.sub(r"Reference\s+node\s*:\s*", "", line, flags=re.IGNORECASE)
            if after_marker.strip():
                reference_node_lines.append(after_marker.strip())
            continue

        if in_reference_node:
            reference_node_lines.append(line)
            continue

        # "... is not supported" パターン
        m = RE_PHOTON_UNSUPPORTED.search(line)
        if m:
            current_item = PhotonUnsupportedItem(expression=m.group("expr"))
            result.unsupported_items.append(current_item)
            continue

        # "Unsupported ..." の理由パターン
        m = RE_PHOTON_REASON.search(line)
        if m and current_item:
            current_item.category = m.group("category")
            current_item.reason = m.group("name")
            current_item.detail = m.group("detail").strip()

    # Reference nodeを結合
    if reference_node_lines:
        result.reference_nodes = reference_node_lines

    return result


def parse_optimizer_statistics(lines: list[str]) -> OptimizerStatistics:
    """Optimizer Statisticsセクションをパース.

    抽出する情報:
    - missing/partial/full のテーブル一覧
    - 推奨アクション（ANALYZE TABLE）
    """
    raw_text = "\n".join(lines)
    result = OptimizerStatistics(raw_text=raw_text)

    # missing/partial/full = ... のパターンを抽出
    # フラット形式でも対応するため、全体テキストから正規表現で抽出
    for m in RE_STATS_STATE.finditer(raw_text):
        state = m.group("state").lower()
        tables_str = m.group("tables").strip()
        # カンマまたはスペースで分割
        tables = [t.strip() for t in re.split(r"[,\s]+", tables_str) if t.strip()]

        if state == "missing":
            result.missing_tables = tables
        elif state == "partial":
            result.partial_tables = tables
        elif state == "full":
            result.full_tables = tables

    # 推奨アクションを抽出
    if RE_ANALYZE_TABLE.search(raw_text):
        # ANALYZE TABLE以降を抽出
        action_match = re.search(r"(ANALYZE\s+TABLE\s+[^\n]+)", raw_text, re.IGNORECASE)
        if action_match:
            result.recommended_action = action_match.group(1).strip()
        else:
            # テンプレートの場合
            result.recommended_action = (
                "ANALYZE TABLE <table-name> COMPUTE STATISTICS FOR ALL COLUMNS"
            )

    return result


def extract_relation_info(node: ExplainNode) -> RelationInfo | None:
    """ExplainNodeからRelation情報を抽出."""
    if "Relation" not in node.node_name:
        return None

    m = RE_RELATION_FULL.search(node.raw_line)
    if m:
        columns_str = m.group("columns")
        # カラム名から#以降の属性IDを除去
        columns = [re.sub(r"#\d+.*$", "", col.strip()) for col in columns_str.split(",")]
        return RelationInfo(
            table_name=m.group("table"),
            columns=columns,
            format=m.group("format"),
        )

    return None


def extract_scan_table_name(node: ExplainNode) -> str:
    """Extract a scan node's target table identifier.

    Physical plan scan node names take shapes like:
      - ``Scan parquet catalog.schema.table_name[cols...]``
      - ``BatchScan catalog.schema.table``
      - ``PhotonScan catalog.schema.table``
    Returns the table identifier (strips trailing ``[cols]`` / adornments),
    or empty string when the name does not match a recognized shape.
    """
    name = (node.node_name or "").strip()
    # Pattern: leading word(s) before the identifier (Scan|PhotonScan|BatchScan[ parquet])
    m = re.match(
        r"^(?:\*\s*)?(?:Photon)?(?:Batch)?Scan(?:\s+\w+)?\s+([A-Za-z0-9_.`\"]+)",
        name,
    )
    if not m:
        return ""
    table = m.group(1).strip()
    # Strip backticks/quotes and trailing metadata like [cols] or ()
    table = table.replace("`", "").replace('"', "")
    return table


def extract_exchange_info(node: ExplainNode) -> ExchangeInfo | None:
    """ExplainNodeからExchange情報を抽出."""
    if "Exchange" not in node.node_name and "Shuffle" not in node.node_name:
        return None

    info = ExchangeInfo()
    line = node.raw_line

    # パーティション種別と数を抽出
    m = RE_EXCHANGE_PARTITIONING.search(line)
    if m:
        info.partitioning_type = m.group("type")
        info.num_partitions = int(m.group("num"))
        info.partition_keys = m.group("expr")

    # plan_id を抽出
    m = RE_PLAN_ID.search(line)
    if m:
        info.plan_id = int(m.group("id"))

    # ENSURE_REQUIREMENTS を検出
    if RE_ENSURE_REQUIREMENTS.search(line):
        info.ensure_requirements = True

    # 何か情報があれば返す
    if info.partitioning_type or info.plan_id:
        return info

    return None


_PHOTON_BOUNDARY_OPS = frozenset(
    {
        "ColumnarToRow",
        "AdaptiveSparkPlan",
        "ReusedExchange",
        "BroadcastExchange",  # BroadcastExchange wraps a Photon side; not a fallback
        "Exchange",
        "Subquery",
        "SubqueryAlias",
        "InMemoryTableScan",
        "CTERelationRef",
        "CTERelationDef",
        "WithCTE",
    }
)


def _is_photon_fallback_candidate(node_name: str) -> bool:
    """Return True when a physical-plan operator looks like a non-Photon fallback.

    Rule: a name that does NOT start with ``Photon`` and is not a known
    Photon⇄JVM boundary / wrapper counts as a candidate. Scan lines whose
    node_name was classified as ``PhotonScan parquet ...`` are already
    excluded by the Photon prefix check.
    """
    if not node_name:
        return False
    head = node_name.split()[0]
    if head.startswith("Photon"):
        return False
    if head in _PHOTON_BOUNDARY_OPS:
        return False
    # Tree glyphs etc.
    if head in ("+-", ":-", ":", "|", "==", "*("):
        return False
    # Stage / plan metadata wrappers
    if head.endswith("Stage"):
        return False
    return True


def extract_cte_references(nodes: list[ExplainNode]) -> tuple[list[CteReuseInfo], bool]:
    """Scan nodes for CTERelationRef reference counts and ReusedExchange presence.

    Returns (cte_references, has_reused_exchange).
    """
    refs: list[CteReuseInfo] = []
    has_reused = False
    for n in nodes:
        line = n.raw_line
        for m in RE_CTE_REF_COUNT.finditer(line):
            refs.append(
                CteReuseInfo(cte_id=m.group("cte_id"), reference_count=int(m.group("count")))
            )
        if RE_REUSED_EXCHANGE.search(line) or RE_REUSED_EXCHANGE.search(n.node_name):
            has_reused = True
    return refs, has_reused


def extract_join_strategies(nodes: list[ExplainNode]) -> list[JoinStrategy]:
    """Extract build side, join type, and broadcast mode from Join nodes.

    Broadcast mode (EXECUTOR_BROADCAST / SinglePartition) may appear on a
    sibling BroadcastExchange line, not the join line itself. We propagate
    by scanning a small window after each join.
    """
    strategies: list[JoinStrategy] = []
    for idx, n in enumerate(nodes):
        # NodeFamily.JOIN is preferred, but the existing family classifier
        # buckets ``PhotonShuffledHashJoin`` under SHUFFLE (substring "Shuffle"
        # wins over "Join"). Recognize both forms here without changing the
        # existing classifier.
        if n.family != NodeFamily.JOIN and "Join" not in n.node_name:
            continue
        line = n.raw_line
        js = JoinStrategy(node_name=n.node_name, raw_line=line)
        js.is_broadcast = "Broadcast" in n.node_name
        bs = RE_JOIN_BUILD_SIDE.search(line)
        if bs:
            js.build_side = bs.group("side")
        jt = RE_JOIN_TYPE.search(line)
        if jt:
            js.join_type = jt.group("jtype")
        # Look ahead a few nodes for broadcast mode markers
        for look in nodes[idx : idx + 5]:
            raw = look.raw_line
            if RE_EXECUTOR_BROADCAST.search(raw):
                js.broadcast_mode = "EXECUTOR_BROADCAST"
                break
            if RE_SINGLE_PARTITION.search(raw) and js.is_broadcast:
                js.broadcast_mode = "SinglePartition"
                break
        strategies.append(js)
    return strategies


def extract_filter_pushdown(nodes: list[ExplainNode]) -> list[FilterPushdownInfo]:
    """For each Scan node, summarize which pushdown kinds are present."""
    out: list[FilterPushdownInfo] = []
    for n in nodes:
        if n.family != NodeFamily.SCAN:
            continue
        line = n.raw_line
        fp = FilterPushdownInfo(
            table_name=extract_scan_table_name(n) or "",
            raw_line=line,
        )
        data = n.attrs.get("DataFilters", "")
        part = n.attrs.get("PartitionFilters", "")
        dic = n.attrs.get("DictionaryFilters", "")
        fp.has_data_filters = bool(data and data.strip())
        fp.has_partition_filters = bool(part and part.strip())
        fp.has_dictionary_filters = bool(dic and dic.strip())
        fp.partition_filters_empty = bool(RE_EMPTY_PARTITION_FILTERS.search(line))
        if RE_OPTIONAL_DATA_FILTERS.search(line):
            m = RE_OPTIONAL_DATA_FILTERS.search(line)
            fp.has_optional_filters = bool(m and m.group("value").strip())
        if RE_REQUIRED_DATA_FILTERS.search(line):
            m = RE_REQUIRED_DATA_FILTERS.search(line)
            fp.has_required_filters = bool(m and m.group("value").strip())
        out.append(fp)
    return out


def _classify_cast_context(node: ExplainNode) -> str:
    """Map an ExplainNode to the CAST context bucket used for prioritization."""
    name = node.node_name
    fam = node.family
    if fam == NodeFamily.JOIN:
        return "join"
    if fam == NodeFamily.FILTER:
        return "filter"
    if fam == NodeFamily.AGG:
        return "aggregate"
    if fam == NodeFamily.PROJECT:
        return "project"
    if fam == NodeFamily.SCAN:
        return "scan"
    # Secondary: key substrings (some Photon ops mix roles)
    n_lower = name.lower()
    if "join" in n_lower:
        return "join"
    if "filter" in n_lower:
        return "filter"
    if "agg" in n_lower:
        return "aggregate"
    if "project" in n_lower:
        return "project"
    return "other"


def extract_implicit_cast_sites(nodes: list[ExplainNode]) -> list[ImplicitCastSite]:
    """Collect every ``cast(col#id as TYPE)`` occurrence with its node context."""
    out: list[ImplicitCastSite] = []
    for n in nodes:
        ctx = _classify_cast_context(n)
        for m in RE_IMPLICIT_CAST.finditer(n.raw_line):
            out.append(
                ImplicitCastSite(
                    context=ctx,
                    column_ref=m.group("col"),
                    to_type=m.group("to_type"),
                    node_name=n.node_name,
                    raw_expression=m.group(0),
                )
            )
    return out


def extract_aggregate_phases(nodes: list[ExplainNode]) -> list[AggregatePhaseInfo]:
    """Detect partial / final / complete phase split per aggregate node."""
    out: list[AggregatePhaseInfo] = []
    for n in nodes:
        if n.family != NodeFamily.AGG and "Agg" not in n.node_name:
            continue
        line = n.raw_line
        info = AggregatePhaseInfo(node_name=n.node_name, raw_line=line)
        info.has_partial_functions = bool(RE_PARTIAL_AGG_FN.search(line))
        info.has_final_merge = bool(RE_FINAL_MERGE_AGG_FN.search(line))
        m = RE_AGG_PART_KV.search(line)
        if m:
            info.agg_part = m.group("part")
        out.append(info)
    return out


def extract_scan_schemas(nodes: list[ExplainNode]) -> dict[str, dict[str, str]]:
    """Parse ``ReadSchema: struct<...>`` annotations from scan lines into
    ``{table_name: {column: type}}``.

    The function does NOT rely on ``NodeFamily.SCAN`` or section kind: in
    real Databricks EXPLAIN EXTENDED output, ``PhotonScan`` nodes carrying
    ``ReadSchema`` can appear under "Optimized Logical Plan" (where the
    family classifier buckets them as UNKNOWN) — we want to catch those too.
    """
    out: dict[str, dict[str, str]] = {}
    for n in nodes:
        if "ReadSchema" not in n.raw_line:
            continue
        table = extract_scan_table_name(n)
        if not table:
            continue
        m = RE_READ_SCHEMA_STRUCT.search(n.raw_line)
        if not m:
            continue
        body = m.group("body")
        schema = _parse_struct_body(body)
        if schema:
            existing = out.setdefault(table, {})
            for col, ty in schema.items():
                existing.setdefault(col, ty)
    return out


# Regex for per-column statistics in EXPLAIN EXTENDED output.
# Databricks/Spark typically emits column stats in one of these forms:
#   1) ``ColumnStat(distinctCount=Some(12), min=Some(1), max=Some(12), nullCount=Some(0), ...)``
#      (appears within a comma-separated relation annotation)
#   2) Inline shorthand: ``<col>: {distinctCount=12, min=1, max=12, nullCount=0}``
# We capture distinctCount plus min/max/nullCount for completeness.
_RE_DISTINCT_COUNT = re.compile(r"distinctCount\s*=\s*(?:Some\()?\s*(?P<v>\d+)", re.IGNORECASE)
_RE_NULL_COUNT = re.compile(r"nullCount\s*=\s*(?:Some\()?\s*(?P<v>\d+)", re.IGNORECASE)
_RE_MIN_VAL = re.compile(r"min\s*=\s*(?:Some\()?\s*(?P<v>[^,)\s]+)", re.IGNORECASE)
_RE_MAX_VAL = re.compile(r"max\s*=\s*(?:Some\()?\s*(?P<v>[^,)\s]+)", re.IGNORECASE)
# Column-annotated block: ``colName: ColumnStat(...)`` or ``colName: {...}``.
# Column name accepts three forms:
#   - bare identifier           : ``foo_bar``
#   - backtick-quoted           : ```My Column``` (allows spaces/symbols)
#   - dotted/qualified          : ``schema.table.col`` (taken as one unit)
# ``distinctCount`` is still required inside the body so unrelated
# ``: { ... }`` pairs don't get pulled in.
_RE_COL_STAT_BLOCK = re.compile(
    r"(?P<col>`[^`]+`|[A-Za-z_][\w.]*)\s*:\s*(?:ColumnStat\(|\{)(?P<body>[^})]+)[})]",
    re.IGNORECASE,
)


def _parse_column_stat_body(body: str) -> ColumnStat:
    cs = ColumnStat()
    m = _RE_DISTINCT_COUNT.search(body)
    if m:
        try:
            cs.distinct_count = int(m.group("v"))
        except ValueError:
            pass
    m = _RE_NULL_COUNT.search(body)
    if m:
        try:
            cs.null_count = int(m.group("v"))
        except ValueError:
            pass
    m = _RE_MIN_VAL.search(body)
    if m:
        cs.min_value = m.group("v").strip()
    m = _RE_MAX_VAL.search(body)
    if m:
        cs.max_value = m.group("v").strip()
    return cs


def extract_scan_column_stats(
    nodes: list[ExplainNode],
) -> dict[str, dict[str, ColumnStat]]:
    """Parse per-column statistics from scan/relation lines.

    Only populated when ``ANALYZE TABLE ... COMPUTE STATISTICS FOR ALL
    COLUMNS`` has been run on the underlying table. Uses best-effort
    regex matching on common EXPLAIN output shapes — returns an empty
    dict (not None) when no stats are found so callers can fall back to
    name/type heuristics.
    """
    out: dict[str, dict[str, ColumnStat]] = {}
    for n in nodes:
        line = n.raw_line
        if "ColumnStat(" not in line and "distinctCount" not in line:
            continue
        table = extract_scan_table_name(n)
        if not table:
            continue
        for m in _RE_COL_STAT_BLOCK.finditer(line):
            col = m.group("col").strip("`")
            # For dotted names (``schema.table.col``) take only the leaf
            # so lookups keyed by bare column names still match.
            if "." in col:
                col = col.rsplit(".", 1)[-1]
            body = m.group("body")
            if "distinctCount" not in body:
                continue
            cs = _parse_column_stat_body(body)
            if cs.distinct_count is None:
                continue
            out.setdefault(table, {}).setdefault(col, cs)
    return out


def extract_photon_fallback_ops(nodes: list[ExplainNode]) -> list[PhotonFallbackOp]:
    """Detect non-Photon-prefixed operators in the physical plan.

    These are operators that were NOT replaced by their Photon counterpart
    and therefore execute on the JVM — typically correlated with performance
    cliffs. ColumnarToRow and plan-wrapper ops are excluded.
    """
    out: list[PhotonFallbackOp] = []
    for n in nodes:
        if n.kind != PlanKind.PHYSICAL:
            continue
        if _is_photon_fallback_candidate(n.node_name):
            out.append(PhotonFallbackOp(node_name=n.node_name, raw_line=n.raw_line))
    return out


def parse_explain_extended(text: str) -> ExplainExtended:
    """EXPLAIN EXTENDEDテキストを完全にパース.

    すべてのセクションから有用な情報を抽出:
    - Logical/Physical Plan のノード
    - Photon Explanation の非対応情報
    - Optimizer Statistics の統計状態
    - Exchange のシャッフル情報
    - Relation のテーブル情報
    """
    sections = split_sections(text)
    result = ExplainExtended(sections=sections)

    for section in sections:
        section_name_lower = section.name.lower()
        kind = _section_kind(section.name)

        # Logical/Physical Plan のノードをパース
        if kind in (PlanKind.LOGICAL, PlanKind.PHYSICAL):
            section.nodes = parse_plan_lines(section.lines, kind)

            # ノードから追加情報を抽出
            for node in section.nodes:
                # Relation 情報
                rel_info = extract_relation_info(node)
                if rel_info:
                    result.relations.append(rel_info)

                # Exchange 情報
                exch_info = extract_exchange_info(node)
                if exch_info:
                    result.exchanges.append(exch_info)

                # AdaptiveSparkPlan の検出
                if "AdaptiveSparkPlan" in node.node_name:
                    result.is_adaptive = True
                    m = RE_IS_FINAL_PLAN.search(node.raw_line)
                    if m:
                        result.is_final_plan = m.group("value").lower() == "true"

            # Phase-1 v2: aggregate richer insights from physical nodes
            if kind == PlanKind.PHYSICAL:
                cte_refs, has_reused = extract_cte_references(section.nodes)
                result.cte_references.extend(cte_refs)
                result.has_reused_exchange = result.has_reused_exchange or has_reused
                result.join_strategies.extend(extract_join_strategies(section.nodes))
                result.filter_pushdown.extend(extract_filter_pushdown(section.nodes))
                result.implicit_cast_sites.extend(extract_implicit_cast_sites(section.nodes))
                result.aggregate_phases.extend(extract_aggregate_phases(section.nodes))
                result.photon_fallback_ops.extend(extract_photon_fallback_ops(section.nodes))

            # Scan schemas (ReadSchema: struct<...>) can appear in
            # Optimized Logical Plan or Physical Plan — extract from every
            # plan section so we don't miss them when Databricks emits
            # PhotonScan under the Optimized Logical Plan.
            for tbl, schema in extract_scan_schemas(section.nodes).items():
                existing = result.scan_schemas.setdefault(tbl, {})
                for col, ty in schema.items():
                    existing.setdefault(col, ty)

            # Per-column statistics (distinctCount/min/max/nullCount).
            # Only populated when ANALYZE TABLE ... FOR ALL COLUMNS
            # has been run; empty dict otherwise.
            for tbl, col_stats in extract_scan_column_stats(section.nodes).items():
                existing_stats = result.scan_column_stats.setdefault(tbl, {})
                for col, cs in col_stats.items():
                    existing_stats.setdefault(col, cs)

        # Photon Explanation をパース
        elif "photon explanation" in section_name_lower:
            result.photon_explanation = parse_photon_explanation(section.lines)

        # Optimizer Statistics をパース
        elif "optimizer statistics" in section_name_lower or "statistics" in section_name_lower:
            result.optimizer_statistics = parse_optimizer_statistics(section.lines)

    return result
