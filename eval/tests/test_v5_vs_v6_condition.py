"""Tests for the ``v5_legacy`` A/B condition.

V6 became default-on in v6.6.4. To compare V5-equivalent behavior
against the V6 standard, the A/B runner needs a condition that
explicitly disables every V6 kill switch (the "legacy full-off"
pattern from ``docs/v6/why-default-on.md``).
"""

from __future__ import annotations

from core.feature_flags import ALL_FLAGS
from eval.ab_runner import CONDITIONS


def test_v5_legacy_condition_disables_every_v6_flag():
    """Every flag in ``feature_flags.ALL_FLAGS`` must appear in
    ``CONDITIONS['v5_legacy']`` and be set to a falsy value. If a
    new V6 flag is introduced and this list isn't updated, the
    "V5 mode" comparison silently inherits the new V6 behavior."""
    overrides = CONDITIONS["v5_legacy"]
    for flag in ALL_FLAGS:
        assert flag in overrides, (
            f"v5_legacy missing kill switch for {flag} — add it to "
            "eval/ab_runner.CONDITIONS or the comparison will leak "
            "the new behavior into the 'V5' baseline."
        )
        assert overrides[flag] == "0"


def test_baseline_condition_is_empty():
    """Baseline = "use defaults" = V6 standard. Empty overrides keeps
    the meaning unambiguous; if someone adds entries here we lose
    the "current production behavior" anchor."""
    assert CONDITIONS["baseline"] == {}


def test_run_condition_passes_token_via_env_not_cli():
    """Security: the Databricks PAT must not appear in the subprocess
    argv (``ps aux`` would expose it on shared hosts). Codex / user
    flagged this leak in V6.2 smoke run when running goldens_runner
    via ab_runner. Fix: pass ``DATABRICKS_HOST`` / ``DATABRICKS_TOKEN``
    through env, not ``--host`` / ``--token`` flags."""
    import argparse
    from unittest.mock import patch

    from eval.ab_runner import _run_condition

    args = argparse.Namespace(
        manifest="m.yaml",
        out_dir="eval/ab_summary",
        lang="ja",
        skip_judge=True,
        skip_llm=False,
        tag=None,
        limit=2,
        host="https://example.cloud.databricks.com",
        token="dapi-SHOULD-NOT-LEAK-IN-ARGV",
    )

    captured: dict = {}

    class _FakeProc:
        returncode = 1
        stderr = "intentional fail to short-circuit"
        stdout = ""

    def _fake_run(cmd, *, env, cwd, capture_output, text):
        captured["cmd"] = cmd
        captured["env"] = env
        return _FakeProc()

    with patch("eval.ab_runner.subprocess.run", side_effect=_fake_run):
        _run_condition("baseline", {}, "smoke", args)

    cmd_joined = " ".join(captured["cmd"])
    assert "dapi-SHOULD-NOT-LEAK-IN-ARGV" not in cmd_joined, (
        "Databricks PAT must not appear in subprocess argv — "
        "it leaks via ps aux"
    )
    assert "--token" not in cmd_joined
    # Env still carries it so goldens_runner can pick it up.
    assert captured["env"]["DATABRICKS_TOKEN"] == "dapi-SHOULD-NOT-LEAK-IN-ARGV"
    assert (
        captured["env"]["DATABRICKS_HOST"]
        == "https://example.cloud.databricks.com"
    )
