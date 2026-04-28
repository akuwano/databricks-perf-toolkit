"""
LLM client: error classes, retry logic, and OpenAI client factory.
"""

import logging
import time
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from openai import OpenAI

logger = logging.getLogger(__name__)

# LLM configuration
LLM_BASE_TIMEOUT_SECONDS = 180  # 3 minutes base timeout
LLM_TIMEOUT_SECONDS = 180  # kept for backwards compat (overridden dynamically)
LLM_MAX_RETRIES = 3
LLM_RETRY_DELAY_SECONDS = 2
LLM_RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Token budget for prompt size management
MAX_PROMPT_CHARS = 60_000  # ~15K tokens — safe limit for most models
KNOWLEDGE_MAX_CHARS = 25_000  # max chars for knowledge content

# Model-specific max output tokens (known Databricks Foundation Model APIs)
_MODEL_MAX_OUTPUT_TOKENS: dict[str, int] = {
    "databricks-claude-opus-4-7": 32768,
    "databricks-claude-sonnet-4-6": 64000,
    "databricks-claude-sonnet-4-5": 64000,
    "databricks-claude-sonnet-4": 64000,
    "databricks-claude-haiku-4-5": 8192,
    "databricks-gpt-5-5": 16384,
    "databricks-meta-llama-3-3-70b-instruct": 8192,
    "databricks-meta-llama-4-maverick": 32768,
    "databricks-meta-llama-4-scout": 16384,
}

# Default when model is not in the map
_DEFAULT_MAX_OUTPUT_TOKENS = 16384

# Models that reject a non-default `temperature` value. Newer reasoning-tuned
# endpoints either drop the knob entirely (Anthropic Opus 4.7 → "does not
# support the temperature parameter") or pin it to 1 and reject other values
# (OpenAI GPT-5.5 → "Only the default (1) value is supported"). For both we
# omit the parameter from the request rather than try to negotiate.
_MODELS_WITHOUT_TEMPERATURE: set[str] = {
    "databricks-claude-opus-4-7",
    "databricks-gpt-5-5",
}


def supports_temperature(model: str) -> bool:
    """Return False for models that reject the `temperature` parameter."""
    return model not in _MODELS_WITHOUT_TEMPERATURE


def get_model_max_tokens(model: str) -> int:
    """Get max output tokens for a model.

    Looks up by exact match, then longest-prefix match. Falls back to default.
    """
    if model in _MODEL_MAX_OUTPUT_TOKENS:
        return _MODEL_MAX_OUTPUT_TOKENS[model]
    # Longest-prefix match to avoid ambiguity (e.g., "databricks-claude-sonnet-4" vs "4-5")
    best_match: tuple[int, int] = (0, _DEFAULT_MAX_OUTPUT_TOKENS)  # (prefix_len, max_tokens)
    for known_model, max_tok in _MODEL_MAX_OUTPUT_TOKENS.items():
        prefix = known_model.rsplit("-", 1)[0]
        if model.startswith(prefix) and len(prefix) > best_match[0]:
            best_match = (len(prefix), max_tok)
    return best_match[1]


def compute_timeout(prompt_chars: int, max_tokens: int) -> int:
    """Compute dynamic timeout based on prompt size and expected output.

    Heuristic: base 180s + 1s per 1000 prompt chars + 1s per 500 output tokens.
    Capped at 600s (10 minutes).
    """
    base = LLM_BASE_TIMEOUT_SECONDS
    prompt_extra = prompt_chars // 1000
    output_extra = max_tokens // 500
    return min(600, base + prompt_extra + output_extra)


# ---------------------------------------------------------------------------
# Exception hierarchy
# ---------------------------------------------------------------------------


class LLMError(Exception):
    """Base exception for LLM-related errors."""

    pass


class LLMTimeoutError(LLMError):
    """LLM request timed out."""

    pass


class LLMRateLimitError(LLMError):
    """LLM rate limit exceeded."""

    pass


class LLMServiceError(LLMError):
    """LLM service error (5xx)."""

    pass


# ---------------------------------------------------------------------------
# Client factory
# ---------------------------------------------------------------------------


def _ensure_https(host: str) -> str:
    """Ensure host URL has https:// scheme."""
    if not host:
        return host
    if not host.startswith("https://") and not host.startswith("http://"):
        return f"https://{host}"
    return host


def create_openai_client(databricks_host: str, databricks_token: str) -> "OpenAI":
    """Create an OpenAI client configured for Databricks serving endpoints.

    Supports two authentication modes:
    1. PAT token (databricks_token is non-empty) — used by CLI
    2. Databricks SDK Config() — used by Databricks Apps (auto-detects
       service principal credentials from DATABRICKS_CLIENT_ID/SECRET)

    Args:
        databricks_host: Databricks workspace URL (e.g. https://xxx.cloud.databricks.com)
        databricks_token: Databricks API token (empty string to use SDK auth)

    Returns:
        Configured OpenAI client instance
    """
    from openai import OpenAI

    host = _ensure_https(databricks_host)

    if databricks_token:
        # Direct PAT token authentication (CLI usage)
        return OpenAI(
            api_key=databricks_token,
            base_url=f"{host}/serving-endpoints",
        )

    # Databricks SDK authentication (Apps / service principal)
    try:
        from databricks.sdk.core import Config

        cfg = Config()
        host = host or f"https://{cfg.host}"

        # Get OAuth token from SDK
        header_factory = cfg.authenticate
        headers = header_factory()
        auth_token = headers.get("Authorization", "").replace("Bearer ", "")

        if auth_token:
            logger.info("Using Databricks SDK authentication (service principal)")
            return OpenAI(
                api_key=auth_token,
                base_url=f"{host}/serving-endpoints",
            )
    except Exception as e:
        logger.warning("Databricks SDK auth failed: %s", e)

    raise LLMError(
        "No authentication available. Set DATABRICKS_TOKEN or configure "
        "Databricks service principal credentials."
    )


# ---------------------------------------------------------------------------
# Telemetry — structured 1-line log per LLM call (v6.6.x instrumentation)
# ---------------------------------------------------------------------------

# V6 flag bitmask spliced into the LLM_CALL log line. Width tracks
# the live V6_* set in feature_flags.py — slot 8 was V6_COMPACT_TOP_ALERTS
# (retired v6.6.4). Stable order so log search ('1010111' etc.) keeps
# meaning across releases. New flags append; retired flags shrink the
# width and leave a CHANGELOG breadcrumb above.
_TELEMETRY_FLAG_BITS: tuple[str, ...] = (
    "V6_CANONICAL_SCHEMA",
    "V6_REVIEW_NO_KNOWLEDGE",
    "V6_REFINE_MICRO_KNOWLEDGE",
    "V6_ALWAYS_INCLUDE_MINIMUM",
    "V6_SKIP_CONDENSED_KNOWLEDGE",
    "V6_RECOMMENDATION_NO_FORCE_FILL",
    "V6_SQL_SKELETON_EXTENDED",
)


def _flag_bitmask() -> str:
    """Return the V6 flag state as a stable bitmask string ('10110100' etc).

    Order matches ``_TELEMETRY_FLAG_BITS``. Used in log lines so a
    grep / Splunk / log-search query can filter V6-all-on vs partial vs
    legacy without parsing JSON. Lazy-imports feature_flags to avoid a
    hard dependency in unit tests that patch llm_client directly.
    """
    try:
        from . import feature_flags  # noqa: WPS433

        return "".join(
            "1" if feature_flags._is_enabled(name) else "0"
            for name in _TELEMETRY_FLAG_BITS
        )
    except Exception:  # nosec
        return "?" * len(_TELEMETRY_FLAG_BITS)


def _emit_llm_call_telemetry(
    *,
    stage: str,
    model: str,
    prompt_chars: int,
    latency_ms: int,
    finish_reason: str,
    usage: object | None,
    attempt: int,
    max_tokens: int,
    extra: dict | None = None,
) -> None:
    """Emit one structured ``LLM_CALL`` log line per successful response.

    Format: ``LLM_CALL k=v k=v ...`` (logfmt-ish). Fields:
        stage, model, attempt, finish_reason, latency_ms, prompt_chars,
        prompt_tokens, completion_tokens, total_tokens, max_tokens,
        flags=<8-bit bitmask in _TELEMETRY_FLAG_BITS order>,
        plus any caller-supplied keys via ``extra``.

    Values are kept to scalars or short strings so a single line stays
    grep / parse-friendly. Lists in ``extra`` are joined with '|'.
    """
    parts: list[str] = ["LLM_CALL"]
    parts.append(f"stage={stage or '-'}")
    parts.append(f"model={model}")
    parts.append(f"attempt={attempt}")
    parts.append(f"finish_reason={finish_reason or '-'}")
    parts.append(f"latency_ms={latency_ms}")
    parts.append(f"prompt_chars={prompt_chars}")
    if usage is not None:
        parts.append(f"prompt_tokens={getattr(usage, 'prompt_tokens', '-')}")
        parts.append(f"completion_tokens={getattr(usage, 'completion_tokens', '-')}")
        parts.append(f"total_tokens={getattr(usage, 'total_tokens', '-')}")
    else:
        parts.append("prompt_tokens=- completion_tokens=- total_tokens=-")
    parts.append(f"max_tokens={max_tokens}")
    parts.append(f"flags={_flag_bitmask()}")

    if extra:
        for k in sorted(extra.keys()):
            v = extra[k]
            if v is None:
                continue
            if isinstance(v, (list, tuple)):
                v = "|".join(str(x) for x in v)
            # Strip any whitespace so the logfmt invariant holds
            v_str = str(v).replace(" ", "_").replace("\n", " ")
            parts.append(f"{k}={v_str}")

    logger.info(" ".join(parts))


# ---------------------------------------------------------------------------
# Retry-aware LLM call
# ---------------------------------------------------------------------------


def call_llm_with_retry(
    client: "OpenAI",
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float = 0.2,
    *,
    stage: str = "",
    extra_telemetry: dict | None = None,
) -> str:
    """Call LLM with retry logic for transient errors.

    Args:
        client: OpenAI client instance
        model: Model name
        messages: Chat messages
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature
        stage: Optional pipeline stage name ("analyze" / "review" / "refine"
            / "rewrite" / "clustering" / "rerank" / "condensed" / ...).
            Recorded in the structured ``LLM_CALL`` log line so V5 vs V6
            (or flag-by-flag) cost / latency comparisons can be done by
            grepping logs without per-call DB writes.
        extra_telemetry: Optional dict of additional fields to surface
            in the same log line. Common keys: ``knowledge_chars``,
            ``knowledge_sections`` (list[str]), ``sql_chars``,
            ``sql_skeleton_chars``, ``analysis_id``, ``query_id``.
            Values must be JSON-serializable scalars (or list of str
            for ``knowledge_sections``).

    Returns:
        LLM response content

    Raises:
        LLMTimeoutError: Request timed out after retries
        LLMRateLimitError: Rate limit exceeded after retries
        LLMServiceError: Service error after retries
        LLMError: Other LLM errors
    """
    from httpx import TimeoutException
    from openai import APIStatusError, APITimeoutError

    # Log prompt size for diagnostics
    total_chars = sum(len(m.get("content", "")) for m in messages)
    estimated_tokens = total_chars // 4  # rough estimate: 1 token ≈ 4 chars
    timeout = compute_timeout(total_chars, max_tokens)
    logger.info(
        "LLM request: stage=%s model=%s prompt_chars=%s est_tokens=~%s max_tokens=%s timeout=%ss",
        stage or "-",
        model,
        f"{total_chars:,}",
        f"{estimated_tokens:,}",
        max_tokens,
        timeout,
    )

    last_exception: APITimeoutError | TimeoutException | APIStatusError | None = None

    create_kwargs: dict = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "timeout": timeout,
    }
    if supports_temperature(model):
        create_kwargs["temperature"] = temperature

    for attempt in range(LLM_MAX_RETRIES):
        try:
            start_time = time.monotonic()
            response = client.chat.completions.create(**create_kwargs)
            elapsed = time.monotonic() - start_time
            usage = response.usage
            choice = response.choices[0]
            finish_reason = choice.finish_reason
            content = choice.message.content or ""

            # Structured 1-line telemetry — designed to be grep-friendly
            # so V5/V6 (or flag-combination) cost comparisons can be done
            # from log aggregators without instrumenting DB writes yet.
            # Format: ``LLM_CALL key=value key=value ...`` (logfmt-ish).
            _emit_llm_call_telemetry(
                stage=stage,
                model=model,
                prompt_chars=total_chars,
                latency_ms=int(elapsed * 1000),
                finish_reason=finish_reason or "",
                usage=usage,
                attempt=attempt,
                max_tokens=max_tokens,
                extra=extra_telemetry,
            )

            if finish_reason == "length":
                logger.warning(
                    "LLM response truncated (finish_reason=length, max_tokens=%d)", max_tokens
                )
            if not content and finish_reason:
                logger.warning("LLM returned empty content (finish_reason=%s)", finish_reason)

            return content

        except APITimeoutError as e:
            last_exception = e
            logger.warning(
                "LLM timeout after %ds (attempt %d/%d): %s",
                timeout,
                attempt + 1,
                LLM_MAX_RETRIES,
                e,
            )
            # Timeout errors are unlikely to succeed on retry with same payload — fail fast
            break

        except TimeoutException as e:
            last_exception = e
            logger.warning(
                "HTTP timeout after %ds (attempt %d/%d): %s",
                timeout,
                attempt + 1,
                LLM_MAX_RETRIES,
                e,
            )
            # Timeout errors are unlikely to succeed on retry — fail fast
            break

        except APIStatusError as e:
            last_exception = e
            status_code = e.status_code

            if status_code in LLM_RETRYABLE_STATUS_CODES:
                logger.warning(
                    "LLM error %d (attempt %d/%d): %s",
                    status_code,
                    attempt + 1,
                    LLM_MAX_RETRIES,
                    e,
                )
                if attempt < LLM_MAX_RETRIES - 1:
                    # Exponential backoff for rate limits
                    delay = LLM_RETRY_DELAY_SECONDS * (2**attempt)
                    if status_code == 429:
                        delay = max(delay, 5)  # At least 5 seconds for rate limit
                    time.sleep(delay)
                continue
            else:
                # Non-retryable error
                logger.error("LLM API error (non-retryable): %d - %s", status_code, e)
                raise LLMError(f"LLM API error: {status_code}") from e

        except Exception as e:
            logger.error("Unexpected LLM error: %s - %s", type(e).__name__, e)
            raise LLMError(f"Unexpected error: {type(e).__name__}") from e

    # All retries exhausted
    if isinstance(last_exception, (APITimeoutError, TimeoutException)):
        raise LLMTimeoutError(
            f"LLM request timed out after {LLM_MAX_RETRIES} attempts"
        ) from last_exception
    elif isinstance(last_exception, APIStatusError):
        if last_exception.status_code == 429:
            raise LLMRateLimitError(
                f"LLM rate limit exceeded after {LLM_MAX_RETRIES} attempts"
            ) from last_exception
        else:
            raise LLMServiceError(
                f"LLM service error after {LLM_MAX_RETRIES} attempts"
            ) from last_exception
    else:
        raise LLMError("LLM request failed") from last_exception
