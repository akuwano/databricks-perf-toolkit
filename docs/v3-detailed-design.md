# v3 詳細設計: プロファイル比較・ナレッジ蓄積プラットフォーム

## 設計方針

- **MLflow非依存・Delta Table中心**
- 既存6テーブルはそのまま活かし、`header` を比較・検索のハブに拡張
- 差分計算は原則ビューで実施し、重い比較結果だけを `profiler_comparison_metrics` に永続化
- Genie向けは「自然言語で意味が分かる列名」を優先
- ナレッジは「文書本体」と「タグ」を分離し、推奨事項・比較結果・手動知見を横断保存
- 既存のグローバルキー `analysis_id` は維持、比較軸は `query_fingerprint` + `experiment_id` + `variant` を追加

## ロードマップ

| Phase | 内容 | 依存 |
|-------|------|------|
| 1 | header スキーマ拡張 + AnalysisContext モデル + fingerprint生成 | なし |
| 2 | 2件 before/after 比較 API + 差分計算 | Phase 1 |
| 3 | 比較結果の永続化 + SQL View | Phase 2 |
| 4 | Genie Space 連携 (curated view + Conversation API) | Phase 3 |
| 5 | ナレッジ蓄積 (自動保存・タグ・検索) | Phase 3 |

---

## 1. Delta DDL

### 1.1 既存 header テーブルへの追加カラム

```sql
ALTER TABLE {catalog}.{schema}.profiler_analysis_header ADD COLUMNS (
  query_fingerprint STRING COMMENT 'Normalized SQL fingerprint for grouping semantically same queries',
  query_fingerprint_version STRING COMMENT 'Fingerprint algorithm version, e.g. v1',
  experiment_id STRING COMMENT 'Logical experiment or tuning campaign id',
  variant STRING COMMENT 'Variant name such as baseline / optimized / candidate_a',
  variant_group STRING COMMENT 'Optional higher-level grouping for variants',
  baseline_flag BOOLEAN COMMENT 'Whether this analysis should be treated as baseline',
  tags_json STRING COMMENT 'Arbitrary JSON tags: env, dataset, branch, owner, jira',
  source_run_id STRING COMMENT 'External run or execution id if available',
  source_job_id STRING COMMENT 'Databricks job id if the analysis comes from a job',
  source_job_run_id STRING COMMENT 'Databricks job run id',
  analysis_notes STRING COMMENT 'Free-text operator notes',
  query_text_normalized STRING COMMENT 'Normalized SQL text used for fingerprinting',
  metric_direction_version STRING COMMENT 'Version of metric direction rules used',
  knowledge_version STRING COMMENT 'Version of recommendation knowledge used'
);
```

### 1.2 header の将来DDL（フルスキーマ）

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  analysis_id STRING NOT NULL,
  analyzed_at TIMESTAMP NOT NULL,
  query_id STRING,
  query_status STRING,
  query_text STRING,
  query_text_normalized STRING,
  query_fingerprint STRING,
  query_fingerprint_version STRING,
  experiment_id STRING,
  variant STRING,
  variant_group STRING,
  baseline_flag BOOLEAN,
  tags_json STRING,
  source_run_id STRING,
  source_job_id STRING,
  source_job_run_id STRING,
  analysis_notes STRING,
  metric_direction_version STRING,
  knowledge_version STRING,

  -- (既存メトリクスカラムは省略、変更なし)

  report_markdown STRING,
  extra_metrics_json STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (query_fingerprint, experiment_id, analyzed_at)
```

### 1.3 新規: profiler_comparison_pairs

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL COMMENT 'UUID for one comparison request',
  pair_status STRING COMMENT 'PENDING / COMPLETED / FAILED',
  pair_type STRING COMMENT 'baseline_vs_candidate / latest_vs_previous / explicit_pair',

  baseline_analysis_id STRING NOT NULL,
  candidate_analysis_id STRING NOT NULL,

  query_fingerprint STRING,
  experiment_id STRING,
  baseline_variant STRING,
  candidate_variant STRING,

  baseline_analyzed_at TIMESTAMP,
  candidate_analyzed_at TIMESTAMP,

  comparison_scope STRING COMMENT 'header_only / with_actions / with_scans / full',
  comparison_reason STRING COMMENT 'why this pair was created',
  requested_by STRING,
  request_source STRING COMMENT 'api / batch / notebook / manual',
  tags_json STRING,

  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (query_fingerprint, experiment_id, created_at)
```

### 1.4 新規: profiler_comparison_metrics

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  comparison_id STRING NOT NULL,
  baseline_analysis_id STRING NOT NULL,
  candidate_analysis_id STRING NOT NULL,
  query_fingerprint STRING,
  experiment_id STRING,
  baseline_variant STRING,
  candidate_variant STRING,

  metric_name STRING NOT NULL COMMENT 'e.g. total_time_ms',
  metric_group STRING COMMENT 'latency / io / cache / spill / shuffle / sql_complexity',
  direction_when_increase STRING COMMENT 'IMPROVES / WORSENS / NEUTRAL',
  baseline_value DOUBLE,
  candidate_value DOUBLE,
  absolute_diff DOUBLE,
  relative_diff_ratio DOUBLE COMMENT '(candidate - baseline) / baseline',
  percent_diff DOUBLE COMMENT 'relative diff in percent',
  changed_flag BOOLEAN,
  improvement_flag BOOLEAN,
  regression_flag BOOLEAN,
  severity STRING COMMENT 'CRITICAL / HIGH / MEDIUM / LOW / NONE',
  summary_text STRING COMMENT 'Human readable explanation for this metric diff',

  created_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (query_fingerprint, metric_name, created_at)
```

### 1.5 新規: profiler_knowledge_documents

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  document_id STRING NOT NULL,
  knowledge_type STRING COMMENT 'recommendation / finding / regression_case / tuning_pattern / manual_note',
  source_type STRING COMMENT 'analysis / comparison / human / imported',
  source_analysis_id STRING,
  source_comparison_id STRING,
  query_fingerprint STRING,
  experiment_id STRING,
  variant STRING,

  title STRING NOT NULL,
  summary STRING,
  body_markdown STRING,
  problem_category STRING COMMENT 'scan / spill / shuffle / photon / join / cache / skew',
  root_cause TEXT,
  recommendation TEXT,
  expected_impact STRING COMMENT 'high / medium / low',
  confidence_score DOUBLE,
  applicability_scope STRING COMMENT 'query / fingerprint / workload / warehouse',
  status STRING COMMENT 'draft / active / archived',
  tags_json STRING,

  created_by STRING,
  created_at TIMESTAMP NOT NULL,
  updated_at TIMESTAMP
)
USING DELTA
CLUSTER BY (knowledge_type, problem_category, created_at)
```

### 1.6 新規: profiler_knowledge_tags

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  document_id STRING NOT NULL,
  tag_name STRING NOT NULL,
  tag_value STRING,
  created_at TIMESTAMP NOT NULL
)
USING DELTA
CLUSTER BY (tag_name, tag_value, created_at)
```

### 1.7 新規: profiler_metric_directions

```sql
CREATE TABLE IF NOT EXISTS {fqn} (
  metric_name STRING NOT NULL,
  metric_group STRING,
  display_name STRING,
  unit STRING COMMENT 'ms / bytes / ratio / count / boolean',
  increase_effect STRING NOT NULL COMMENT 'IMPROVES / WORSENS / NEUTRAL',
  decrease_effect STRING NOT NULL COMMENT 'IMPROVES / WORSENS / NEUTRAL',
  preferred_trend STRING COMMENT 'UP / DOWN / STABLE',
  regression_threshold_ratio DOUBLE COMMENT 'e.g. 0.10 = 10% worse',
  improvement_threshold_ratio DOUBLE COMMENT 'e.g. 0.10 = 10% better',
  notes STRING,
  active_flag BOOLEAN,
  version STRING,
  created_at TIMESTAMP
)
USING DELTA
CLUSTER BY (metric_group, metric_name)
```

### 1.8 CLUSTER BY 見直し

```sql
-- header: query_id中心 → 比較軸中心
ALTER TABLE {catalog}.{schema}.profiler_analysis_header
CLUSTER BY (query_fingerprint, experiment_id, analyzed_at);

-- 子テーブル: analysis_id中心に統一
ALTER TABLE {catalog}.{schema}.profiler_analysis_actions
CLUSTER BY (analysis_id, action_rank);

ALTER TABLE {catalog}.{schema}.profiler_analysis_table_scans
CLUSTER BY (analysis_id, table_name);

ALTER TABLE {catalog}.{schema}.profiler_analysis_hot_operators
CLUSTER BY (analysis_id, bottleneck_type, operator_rank);

ALTER TABLE {catalog}.{schema}.profiler_analysis_stages
CLUSTER BY (analysis_id, stage_id);

ALTER TABLE {catalog}.{schema}.profiler_analysis_raw
CLUSTER BY (analysis_id, analyzed_at);
```

---

## 2. 比較用 SQL View

### 2.1 vw_latest_analysis_by_fingerprint

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_latest_analysis_by_fingerprint AS
WITH ranked AS (
  SELECT
    h.*,
    ROW_NUMBER() OVER (
      PARTITION BY h.query_fingerprint,
                   COALESCE(h.experiment_id, '__default__'),
                   COALESCE(h.variant, '__default__')
      ORDER BY h.analyzed_at DESC, h.created_at DESC
    ) AS row_num
  FROM {catalog}.{schema}.profiler_analysis_header h
  WHERE h.query_fingerprint IS NOT NULL
)
SELECT * FROM ranked WHERE row_num = 1
```

### 2.2 vw_comparison_diff

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_comparison_diff AS
WITH pairs AS (
  SELECT
    p.comparison_id,
    p.query_fingerprint,
    p.experiment_id,
    p.baseline_variant,
    p.candidate_variant,
    b.analysis_id AS baseline_analysis_id,
    c.analysis_id AS candidate_analysis_id,
    STACK(
      14,
      'total_time_ms', cast(b.total_time_ms as double), cast(c.total_time_ms as double),
      'execution_time_ms', cast(b.execution_time_ms as double), cast(c.execution_time_ms as double),
      'read_bytes', cast(b.read_bytes as double), cast(c.read_bytes as double),
      'read_remote_bytes', cast(b.read_remote_bytes as double), cast(c.read_remote_bytes as double),
      'read_cache_bytes', cast(b.read_cache_bytes as double), cast(c.read_cache_bytes as double),
      'spill_to_disk_bytes', cast(b.spill_to_disk_bytes as double), cast(c.spill_to_disk_bytes as double),
      'spill_bytes', cast(b.spill_bytes as double), cast(c.spill_bytes as double),
      'bytes_read_from_cache_percentage', cast(b.bytes_read_from_cache_percentage as double), cast(c.bytes_read_from_cache_percentage as double),
      'photon_ratio', b.photon_ratio, c.photon_ratio,
      'remote_read_ratio', b.remote_read_ratio, c.remote_read_ratio,
      'bytes_pruning_ratio', b.bytes_pruning_ratio, c.bytes_pruning_ratio,
      'shuffle_impact_ratio', b.shuffle_impact_ratio, c.shuffle_impact_ratio,
      'cloud_storage_retry_ratio', b.cloud_storage_retry_ratio, c.cloud_storage_retry_ratio,
      'oom_fallback_count', cast(b.oom_fallback_count as double), cast(c.oom_fallback_count as double)
    ) AS (metric_name, baseline_value, candidate_value)
  FROM {catalog}.{schema}.profiler_comparison_pairs p
  INNER JOIN {catalog}.{schema}.profiler_analysis_header b
    ON p.baseline_analysis_id = b.analysis_id
  INNER JOIN {catalog}.{schema}.profiler_analysis_header c
    ON p.candidate_analysis_id = c.analysis_id
),
scored AS (
  SELECT
    p.*,
    d.metric_group,
    d.increase_effect,
    d.decrease_effect,
    d.regression_threshold_ratio,
    d.improvement_threshold_ratio,
    candidate_value - baseline_value AS absolute_diff,
    CASE
      WHEN baseline_value IS NULL OR baseline_value = 0 THEN NULL
      ELSE (candidate_value - baseline_value) / baseline_value
    END AS relative_diff_ratio
  FROM pairs p
  LEFT JOIN {catalog}.{schema}.profiler_metric_directions d
    ON p.metric_name = d.metric_name
)
SELECT
  comparison_id,
  query_fingerprint,
  experiment_id,
  baseline_variant,
  candidate_variant,
  baseline_analysis_id,
  candidate_analysis_id,
  metric_name,
  metric_group,
  baseline_value,
  candidate_value,
  absolute_diff,
  relative_diff_ratio,
  relative_diff_ratio * 100 AS percent_diff,
  CASE
    WHEN absolute_diff > 0 THEN increase_effect
    WHEN absolute_diff < 0 THEN decrease_effect
    ELSE 'NEUTRAL'
  END AS change_effect,
  CASE
    WHEN absolute_diff > 0 AND increase_effect = 'WORSENS'
         AND COALESCE(relative_diff_ratio, 0) >= COALESCE(regression_threshold_ratio, 0) THEN true
    WHEN absolute_diff < 0 AND decrease_effect = 'WORSENS'
         AND ABS(COALESCE(relative_diff_ratio, 0)) >= COALESCE(regression_threshold_ratio, 0) THEN true
    ELSE false
  END AS regression_flag,
  CASE
    WHEN absolute_diff > 0 AND increase_effect = 'IMPROVES'
         AND COALESCE(relative_diff_ratio, 0) >= COALESCE(improvement_threshold_ratio, 0) THEN true
    WHEN absolute_diff < 0 AND decrease_effect = 'IMPROVES'
         AND ABS(COALESCE(relative_diff_ratio, 0)) >= COALESCE(improvement_threshold_ratio, 0) THEN true
    ELSE false
  END AS improvement_flag
FROM scored
```

### 2.3 vw_regression_candidates

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_regression_candidates AS
WITH metric_flags AS (
  SELECT
    comparison_id,
    query_fingerprint,
    experiment_id,
    baseline_variant,
    candidate_variant,
    COUNT_IF(regression_flag) AS regression_metric_count,
    COUNT_IF(improvement_flag) AS improvement_metric_count,
    MAX(CASE WHEN metric_name = 'total_time_ms' AND regression_flag THEN 1 ELSE 0 END) AS total_time_regressed,
    MAX(CASE WHEN metric_name = 'spill_to_disk_bytes' AND regression_flag THEN 1 ELSE 0 END) AS spill_regressed,
    MAX(CASE WHEN metric_name = 'remote_read_ratio' AND regression_flag THEN 1 ELSE 0 END) AS remote_read_regressed,
    MAX(CASE WHEN metric_name = 'photon_ratio' AND regression_flag THEN 1 ELSE 0 END) AS photon_regressed
  FROM {catalog}.{schema}.vw_comparison_diff
  GROUP BY ALL
)
SELECT
  p.*,
  f.regression_metric_count,
  f.improvement_metric_count,
  CASE
    WHEN f.total_time_regressed = 1 AND f.spill_regressed = 1 THEN 'HIGH'
    WHEN f.total_time_regressed = 1 AND f.remote_read_regressed = 1 THEN 'HIGH'
    WHEN f.total_time_regressed = 1 AND f.photon_regressed = 1 THEN 'MEDIUM'
    WHEN f.regression_metric_count >= 3 THEN 'MEDIUM'
    ELSE 'LOW'
  END AS regression_severity
FROM {catalog}.{schema}.profiler_comparison_pairs p
INNER JOIN metric_flags f
  ON p.comparison_id = f.comparison_id
WHERE f.regression_metric_count > 0
```

---

## 3. Python クラス設計

### 3.1 models.py への追加 dataclass

```python
# --- databricks-apps/core/models.py に追加 ---

@dataclass
class AnalysisContext:
    """Context for tracking and comparing analyses."""
    query_fingerprint: str = ""
    query_fingerprint_version: str = "v1"
    experiment_id: str = ""
    variant: str = ""
    variant_group: str = ""
    baseline_flag: bool = False
    tags: dict[str, Any] = field(default_factory=dict)
    source_run_id: str = ""
    source_job_id: str = ""
    source_job_run_id: str = ""
    analysis_notes: str = ""
    query_text_normalized: str = ""


@dataclass
class ComparisonRequest:
    """Request to compare two analyses."""
    baseline_analysis_id: str = ""
    candidate_analysis_id: str = ""
    comparison_scope: str = "full"  # header_only / with_actions / with_scans / full
    comparison_reason: str = ""
    requested_by: str = ""
    request_source: str = "manual"  # api / batch / notebook / manual
    tags: dict[str, Any] = field(default_factory=dict)


@dataclass
class MetricDiff:
    """Difference for a single metric between baseline and candidate."""
    metric_name: str = ""
    metric_group: str = ""
    direction_when_increase: str = ""  # IMPROVES / WORSENS / NEUTRAL
    baseline_value: float | None = None
    candidate_value: float | None = None
    absolute_diff: float | None = None
    relative_diff_ratio: float | None = None
    changed_flag: bool = False
    improvement_flag: bool = False
    regression_flag: bool = False
    severity: str = "NONE"  # CRITICAL / HIGH / MEDIUM / LOW / NONE
    summary_text: str = ""


@dataclass
class ComparisonResult:
    """Result of comparing two analyses."""
    comparison_id: str = ""
    baseline_analysis_id: str = ""
    candidate_analysis_id: str = ""
    query_fingerprint: str = ""
    experiment_id: str = ""
    baseline_variant: str = ""
    candidate_variant: str = ""
    metric_diffs: list[MetricDiff] = field(default_factory=list)
    regression_detected: bool = False
    regression_severity: str = "NONE"
    summary: str = ""


@dataclass
class KnowledgeDocument:
    """Knowledge entry derived from analysis or comparison."""
    document_id: str = ""
    knowledge_type: str = ""  # recommendation / finding / regression_case / tuning_pattern / manual_note
    source_type: str = ""  # analysis / comparison / human / imported
    source_analysis_id: str = ""
    source_comparison_id: str = ""
    query_fingerprint: str = ""
    experiment_id: str = ""
    variant: str = ""
    title: str = ""
    summary: str = ""
    body_markdown: str = ""
    problem_category: str = ""  # scan / spill / shuffle / photon / join / cache / skew
    root_cause: str = ""
    recommendation: str = ""
    expected_impact: str = ""  # high / medium / low
    confidence_score: float = 0.0
    applicability_scope: str = ""  # query / fingerprint / workload / warehouse
    status: str = "draft"  # draft / active / archived
    tags: dict[str, Any] = field(default_factory=dict)


@dataclass
class KnowledgeTag:
    """Tag for a knowledge document."""
    document_id: str = ""
    tag_name: str = ""
    tag_value: str = ""
```

### 3.2 ProfileAnalysis への追加

```python
@dataclass
class ProfileAnalysis:
    # ... 既存フィールド ...
    analysis_context: AnalysisContext = field(default_factory=AnalysisContext)  # 追加
```

### 3.3 新規: core/comparison.py

```python
"""Comparison service for before/after profile analysis."""

from __future__ import annotations

import uuid
from typing import Iterable

from .models import ComparisonRequest, ComparisonResult, MetricDiff, ProfileAnalysis

# metric_name -> (metric_group, increase_effect)
COMPARABLE_METRICS = {
    "total_time_ms": ("latency", "WORSENS"),
    "execution_time_ms": ("latency", "WORSENS"),
    "read_bytes": ("io", "WORSENS"),
    "read_remote_bytes": ("io", "WORSENS"),
    "read_cache_bytes": ("cache", "IMPROVES"),
    "spill_to_disk_bytes": ("spill", "WORSENS"),
    "spill_bytes": ("spill", "WORSENS"),
    "bytes_read_from_cache_percentage": ("cache", "IMPROVES"),
    "photon_ratio": ("engine", "IMPROVES"),
    "remote_read_ratio": ("io", "WORSENS"),
    "bytes_pruning_ratio": ("io", "IMPROVES"),
    "shuffle_impact_ratio": ("shuffle", "WORSENS"),
    "cloud_storage_retry_ratio": ("cloud_storage", "WORSENS"),
    "oom_fallback_count": ("engine", "WORSENS"),
}


class ComparisonService:
    """Compares two ProfileAnalysis results and produces metric diffs."""

    def compare_analyses(
        self,
        baseline: ProfileAnalysis,
        candidate: ProfileAnalysis,
        request: ComparisonRequest,
    ) -> ComparisonResult:
        result = ComparisonResult(
            comparison_id=str(uuid.uuid4()),
            baseline_analysis_id=request.baseline_analysis_id,
            candidate_analysis_id=request.candidate_analysis_id,
            query_fingerprint=baseline.analysis_context.query_fingerprint,
            experiment_id=baseline.analysis_context.experiment_id,
            baseline_variant=baseline.analysis_context.variant,
            candidate_variant=candidate.analysis_context.variant,
        )

        for metric_name, (group, increase_effect) in COMPARABLE_METRICS.items():
            bv = self._extract_metric(baseline, metric_name)
            cv = self._extract_metric(candidate, metric_name)
            result.metric_diffs.append(
                self._build_metric_diff(metric_name, group, increase_effect, bv, cv)
            )

        regressions = [m for m in result.metric_diffs if m.regression_flag]
        result.regression_detected = bool(regressions)
        result.regression_severity = self._summarize_severity(regressions)
        result.summary = self._build_summary(result.metric_diffs)
        return result

    def _extract_metric(self, analysis: ProfileAnalysis, name: str) -> float | None:
        for obj in (analysis.query_metrics, analysis.bottleneck_indicators):
            if hasattr(obj, name):
                v = getattr(obj, name)
                return float(v) if v is not None else None
        return None

    def _build_metric_diff(
        self, name: str, group: str, increase_effect: str,
        bv: float | None, cv: float | None,
    ) -> MetricDiff:
        if bv is None or cv is None:
            return MetricDiff(metric_name=name, metric_group=group)

        diff = cv - bv
        ratio = None if bv == 0 else diff / bv

        regression = (
            (diff > 0 and increase_effect == "WORSENS" and (ratio or 0) >= 0.10)
            or (diff < 0 and increase_effect == "IMPROVES" and abs(ratio or 0) >= 0.10)
        )
        improvement = (
            (diff < 0 and increase_effect == "WORSENS" and abs(ratio or 0) >= 0.10)
            or (diff > 0 and increase_effect == "IMPROVES" and (ratio or 0) >= 0.10)
        )
        severity = "HIGH" if regression and name in {"total_time_ms", "spill_to_disk_bytes"} else "LOW"

        return MetricDiff(
            metric_name=name, metric_group=group, direction_when_increase=increase_effect,
            baseline_value=bv, candidate_value=cv, absolute_diff=diff,
            relative_diff_ratio=ratio, changed_flag=diff != 0,
            improvement_flag=improvement, regression_flag=regression, severity=severity,
            summary_text=f"{name}: {bv} -> {cv} ({'+' if diff >= 0 else ''}{diff})",
        )

    def _summarize_severity(self, regressions: Iterable[MetricDiff]) -> str:
        regressions = list(regressions)
        if any(m.severity == "HIGH" for m in regressions):
            return "HIGH"
        return "MEDIUM" if regressions else "NONE"

    def _build_summary(self, diffs: list[MetricDiff]) -> str:
        regressed = [m.metric_name for m in diffs if m.regression_flag]
        improved = [m.metric_name for m in diffs if m.improvement_flag]
        return f"Regressed: {regressed}; Improved: {improved}"
```

### 3.4 新規: core/knowledge.py

```python
"""Knowledge service for building and managing knowledge documents."""

from __future__ import annotations

import uuid

from .models import ComparisonResult, KnowledgeDocument, ProfileAnalysis


class KnowledgeService:
    """Builds knowledge documents from analyses and comparisons."""

    def build_from_analysis(self, analysis: ProfileAnalysis) -> KnowledgeDocument:
        top_action = analysis.action_cards[0] if analysis.action_cards else None
        return KnowledgeDocument(
            document_id=str(uuid.uuid4()),
            knowledge_type="recommendation",
            source_type="analysis",
            query_fingerprint=analysis.analysis_context.query_fingerprint,
            experiment_id=analysis.analysis_context.experiment_id,
            variant=analysis.analysis_context.variant,
            title=top_action.problem if top_action else "Profiler analysis finding",
            summary=top_action.likely_cause if top_action else "",
            body_markdown=top_action.fix_sql if top_action and top_action.fix_sql else "",
            problem_category=(
                analysis.hot_operators[0].bottleneck_type if analysis.hot_operators else ""
            ),
            recommendation=top_action.fix if top_action else "",
            expected_impact=top_action.expected_impact if top_action else "",
            confidence_score=0.7,
            applicability_scope="fingerprint",
            status="active",
            tags=analysis.analysis_context.tags,
        )

    def build_from_comparison(self, comparison: ComparisonResult) -> KnowledgeDocument:
        return KnowledgeDocument(
            document_id=str(uuid.uuid4()),
            knowledge_type=(
                "regression_case" if comparison.regression_detected else "tuning_pattern"
            ),
            source_type="comparison",
            source_comparison_id=comparison.comparison_id,
            query_fingerprint=comparison.query_fingerprint,
            experiment_id=comparison.experiment_id,
            variant=comparison.candidate_variant,
            title=f"Comparison: {comparison.baseline_variant} vs {comparison.candidate_variant}",
            summary=comparison.summary,
            body_markdown=comparison.summary,
            problem_category="comparison",
            recommendation="Review regressed metrics and related action cards",
            expected_impact="high" if comparison.regression_detected else "medium",
            confidence_score=0.8,
            applicability_scope="fingerprint",
            status="active",
        )
```

### 3.5 table_writer.py 拡張方針

```python
# --- 追加: WriteContext (write()の追加パラメータ) ---

@dataclass
class WriteContext:
    """Optional context for enriching writes with comparison metadata."""
    experiment_id: str = ""
    variant: str = ""
    variant_group: str = ""
    baseline_flag: bool = False
    tags: dict[str, Any] | None = None
    source_run_id: str = ""
    source_job_id: str = ""
    source_job_run_id: str = ""
    analysis_notes: str = ""


class TableWriter:
    # 既存 write() に write_context パラメータを追加
    def write(self, analysis, report="", raw_profile_json="",
              write_context: WriteContext | None = None) -> str | None: ...

    # 新規メソッド
    def write_comparison_result(self, result: ComparisonResult) -> str: ...
    def write_knowledge_document(self, document: KnowledgeDocument) -> str: ...
    def write_knowledge_tags(self, document_id: str, tags: dict[str, str]) -> None: ...
```

### 3.6 usecases.py 追加オーケストレータ

```python
def run_analysis_and_persist_pipeline(
    data: dict[str, Any],
    llm_config: LLMConfig,
    options: PipelineOptions,
    writer: TableWriter | None = None,
    analysis_context: AnalysisContext | None = None,
) -> PipelineResult:
    """Run analysis + persist to Delta."""
    result = run_analysis_pipeline(data, llm_config, options)
    if analysis_context:
        result.analysis.analysis_context = analysis_context
    if writer:
        writer.write(result.analysis, report=result.report, raw_profile_json=json.dumps(data))
    return result


def run_comparison_pipeline(
    baseline: ProfileAnalysis,
    candidate: ProfileAnalysis,
    request: ComparisonRequest,
) -> ComparisonResult:
    """Compare two analyses."""
    return ComparisonService().compare_analyses(baseline, candidate, request)


def run_comparison_and_knowledge_pipeline(
    baseline: ProfileAnalysis,
    candidate: ProfileAnalysis,
    request: ComparisonRequest,
    writer: TableWriter | None = None,
) -> ComparisonResult:
    """Compare + persist + generate knowledge entry."""
    comparison = run_comparison_pipeline(baseline, candidate, request)
    if writer:
        writer.write_comparison_result(comparison)
        knowledge = KnowledgeService().build_from_comparison(comparison)
        writer.write_knowledge_document(knowledge)
    return comparison
```

---

## 4. Genie Space 用 curated view

### 命名規約
- 省略語を避ける: `exec_ms` ではなく `execution_time_ms`
- Yes/No判定列は `is_` / `has_`
- 集計済み列は `total_`, `average_`, `latest_`, `regression_`

### 4.1 vw_genie_profile_summary

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_profile_summary AS
SELECT
  h.analysis_id,
  h.analyzed_at,
  h.query_fingerprint,
  h.experiment_id,
  h.variant,
  h.query_status,
  h.statement_type AS sql_statement_type,
  h.query_text,
  h.total_time_ms AS total_query_time_ms,
  h.execution_time_ms,
  h.compilation_time_ms,
  h.read_bytes AS bytes_read,
  h.read_remote_bytes AS remote_bytes_read,
  h.read_cache_bytes AS cache_bytes_read,
  h.spill_to_disk_bytes,
  h.rows_read_count AS rows_read,
  h.rows_produced_count AS rows_produced,
  h.bytes_read_from_cache_percentage AS cache_hit_percentage,
  h.photon_ratio AS photon_usage_ratio,
  h.remote_read_ratio,
  h.bytes_pruning_ratio,
  h.shuffle_impact_ratio,
  h.cloud_storage_retry_ratio,
  h.has_data_skew,
  h.oom_fallback_count,
  h.join_count,
  h.subquery_count,
  h.complexity_score AS sql_complexity_score,
  h.critical_alert_count,
  h.high_alert_count,
  h.action_card_count AS recommendation_count,
  h.scanned_table_count,
  h.warehouse_name,
  h.warehouse_size,
  h.warehouse_type,
  h.warehouse_is_serverless AS is_serverless_warehouse
FROM {catalog}.{schema}.profiler_analysis_header h
```

### 4.2 vw_genie_comparison_summary

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_comparison_summary AS
SELECT
  p.comparison_id,
  p.created_at AS comparison_created_at,
  p.query_fingerprint,
  p.experiment_id,
  p.baseline_variant,
  p.candidate_variant,
  r.regression_severity,
  r.regression_metric_count,
  r.improvement_metric_count,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.baseline_value END) AS baseline_total_time_ms,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.candidate_value END) AS candidate_total_time_ms,
  MAX(CASE WHEN d.metric_name = 'total_time_ms' THEN d.percent_diff END) AS total_time_change_percent,
  MAX(CASE WHEN d.metric_name = 'read_bytes' THEN d.percent_diff END) AS bytes_read_change_percent,
  MAX(CASE WHEN d.metric_name = 'spill_to_disk_bytes' THEN d.percent_diff END) AS spill_change_percent,
  MAX(CASE WHEN d.metric_name = 'photon_ratio' THEN d.percent_diff END) AS photon_ratio_change_percent
FROM {catalog}.{schema}.profiler_comparison_pairs p
LEFT JOIN {catalog}.{schema}.vw_comparison_diff d
  ON p.comparison_id = d.comparison_id
LEFT JOIN {catalog}.{schema}.vw_regression_candidates r
  ON p.comparison_id = r.comparison_id
GROUP BY ALL
```

### 4.3 vw_genie_recommendations

```sql
CREATE OR REPLACE VIEW {catalog}.{schema}.vw_genie_recommendations AS
SELECT
  d.document_id,
  d.created_at,
  d.knowledge_type,
  d.source_type,
  d.query_fingerprint,
  d.experiment_id,
  d.variant,
  d.title AS recommendation_title,
  d.summary AS recommendation_summary,
  d.problem_category,
  d.root_cause,
  d.recommendation AS recommended_action,
  d.expected_impact,
  d.confidence_score,
  d.status,
  CONCAT_WS(', ', COLLECT_SET(CONCAT(t.tag_name, '=', COALESCE(t.tag_value, ''))))
    AS tag_list
FROM {catalog}.{schema}.profiler_knowledge_documents d
LEFT JOIN {catalog}.{schema}.profiler_knowledge_tags t
  ON d.document_id = t.document_id
GROUP BY ALL
```

---

## 5. メトリクス方向性定義

### Python定数 (core/comparison.py)

```python
METRIC_DIRECTIONS = {
    "total_time_ms":                    {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "execution_time_ms":                {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "compilation_time_ms":              {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "read_bytes":                       {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "read_remote_bytes":                {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "read_cache_bytes":                 {"increase": "IMPROVES", "decrease": "WORSENS"},
    "bytes_read_from_cache_percentage": {"increase": "IMPROVES", "decrease": "WORSENS"},
    "spill_to_disk_bytes":              {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "spill_bytes":                      {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "photon_ratio":                     {"increase": "IMPROVES", "decrease": "WORSENS"},
    "remote_read_ratio":                {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "bytes_pruning_ratio":              {"increase": "IMPROVES", "decrease": "WORSENS"},
    "shuffle_impact_ratio":             {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "cloud_storage_retry_ratio":        {"increase": "WORSENS",  "decrease": "IMPROVES"},
    "oom_fallback_count":               {"increase": "WORSENS",  "decrease": "IMPROVES"},
}
```

### 初期データ INSERT

```sql
INSERT INTO {catalog}.{schema}.profiler_metric_directions VALUES
('total_time_ms',                    'latency',       'Total Time',               'ms',    'WORSENS',  'IMPROVES', 'DOWN',   0.10, 0.10, 'Lower is better',              true, 'v1', current_timestamp()),
('execution_time_ms',                'latency',       'Execution Time',           'ms',    'WORSENS',  'IMPROVES', 'DOWN',   0.10, 0.10, 'Lower is better',              true, 'v1', current_timestamp()),
('compilation_time_ms',              'latency',       'Compilation Time',         'ms',    'WORSENS',  'IMPROVES', 'DOWN',   0.10, 0.10, 'Lower is better',              true, 'v1', current_timestamp()),
('read_bytes',                       'io',            'Bytes Read',               'bytes', 'WORSENS',  'IMPROVES', 'DOWN',   0.15, 0.15, 'Less data scanned is better',  true, 'v1', current_timestamp()),
('read_remote_bytes',                'io',            'Remote Bytes Read',        'bytes', 'WORSENS',  'IMPROVES', 'DOWN',   0.15, 0.15, 'Less remote read is better',   true, 'v1', current_timestamp()),
('read_cache_bytes',                 'cache',         'Bytes From Cache',         'bytes', 'IMPROVES', 'WORSENS',  'UP',     0.10, 0.10, 'More cache is better',         true, 'v1', current_timestamp()),
('bytes_read_from_cache_percentage', 'cache',         'Cache Hit Percentage',     'ratio', 'IMPROVES', 'WORSENS',  'UP',     0.05, 0.05, 'Higher cache hit is better',   true, 'v1', current_timestamp()),
('spill_to_disk_bytes',              'spill',         'Spill To Disk',            'bytes', 'WORSENS',  'IMPROVES', 'DOWN',   0.05, 0.05, 'Less spill is better',         true, 'v1', current_timestamp()),
('spill_bytes',                      'spill',         'Operator Spill',           'bytes', 'WORSENS',  'IMPROVES', 'DOWN',   0.05, 0.05, 'Less spill is better',         true, 'v1', current_timestamp()),
('photon_ratio',                     'engine',        'Photon Ratio',             'ratio', 'IMPROVES', 'WORSENS',  'UP',     0.05, 0.05, 'Higher Photon is better',      true, 'v1', current_timestamp()),
('remote_read_ratio',                'io',            'Remote Read Ratio',        'ratio', 'WORSENS',  'IMPROVES', 'DOWN',   0.05, 0.05, 'Lower remote read is better',  true, 'v1', current_timestamp()),
('bytes_pruning_ratio',              'io',            'Bytes Pruning Ratio',      'ratio', 'IMPROVES', 'WORSENS',  'UP',     0.05, 0.05, 'Higher pruning is better',     true, 'v1', current_timestamp()),
('shuffle_impact_ratio',             'shuffle',       'Shuffle Impact',           'ratio', 'WORSENS',  'IMPROVES', 'DOWN',   0.10, 0.10, 'Lower shuffle is better',      true, 'v1', current_timestamp()),
('cloud_storage_retry_ratio',        'cloud_storage', 'Cloud Storage Retry',      'ratio', 'WORSENS',  'IMPROVES', 'DOWN',   0.05, 0.05, 'Lower retry is better',        true, 'v1', current_timestamp()),
('oom_fallback_count',               'engine',        'OOM Fallback Count',       'count', 'WORSENS',  'IMPROVES', 'DOWN',   0.00, 0.00, 'Any increase is bad',          true, 'v1', current_timestamp());
```

---

## 6. 既存コードへの差し込みポイント

| ファイル | 変更内容 |
|---------|---------|
| `core/models.py` | `AnalysisContext`, `ComparisonRequest`, `ComparisonResult`, `MetricDiff`, `KnowledgeDocument`, `KnowledgeTag` 追加。`ProfileAnalysis` に `analysis_context` フィールド追加 |
| `core/comparison.py` | **新規** - `ComparisonService` クラス |
| `core/knowledge.py` | **新規** - `KnowledgeService` クラス |
| `core/usecases.py` | `run_comparison_pipeline()`, `run_analysis_and_persist_pipeline()`, `run_comparison_and_knowledge_pipeline()` 追加 |
| `services/table_writer.py` | `_HEADER_DDL` 拡張、新DDL追加、`WriteContext` 追加、`write_comparison_result()`, `write_knowledge_document()`, `write_knowledge_tags()` 追加 |
| `cli/main.py` | `--experiment-id`, `--variant`, `--baseline`, `--compare-with` 引数追加 |
| `app.py` | 比較UI用エンドポイント追加 |

## 7. 補足

- `query_fingerprint` は「SQL整形 + リテラル正規化 + 空白正規化 + SHA256」で生成
- 差分ビューは最初は `header` のみで始め、必要に応じて `table_scans`/`actions`/`hot_operators` 比較を追加
- Genie は JSON列の解釈が弱いため、curated viewでは主要タグを列展開する運用が推奨
