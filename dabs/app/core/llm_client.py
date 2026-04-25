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
# Retry-aware LLM call
# ---------------------------------------------------------------------------


def call_llm_with_retry(
    client: "OpenAI",
    model: str,
    messages: list[dict],
    max_tokens: int,
    temperature: float = 0.2,
) -> str:
    """Call LLM with retry logic for transient errors.

    Args:
        client: OpenAI client instance
        model: Model name
        messages: Chat messages
        max_tokens: Maximum tokens in response
        temperature: Sampling temperature

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
        "LLM request: model=%s, prompt_chars=%s, est_tokens=~%s, max_tokens=%s, timeout=%ss",
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
            if usage:
                logger.info(
                    "LLM response: %.1fs, prompt_tokens=%s, completion_tokens=%s, total_tokens=%s",
                    elapsed,
                    f"{usage.prompt_tokens:,}",
                    f"{usage.completion_tokens:,}",
                    f"{usage.total_tokens:,}",
                )
            else:
                logger.info("LLM response: %.1fs (no usage info)", elapsed)

            choice = response.choices[0]
            finish_reason = choice.finish_reason
            content = choice.message.content or ""

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
