"""The production graph must block build and both publishers directly."""

from pathlib import Path

import pytest

yaml = pytest.importorskip("yaml")
ROOT = Path(__file__).resolve().parents[1]


def test_publish_graph_fail_closed():
    workflow = yaml.safe_load((ROOT / ".github/workflows/publish.yml").read_text())
    jobs = workflow["jobs"]
    gate = jobs["manifest-gate"]
    expected_mirror = "tepilora-mcp-python" if (ROOT / "src/tepilora_mcp").is_dir() else "tepilora-python"
    assert jobs["lane-ancestry-gate"]["if"] == "github.repository == 'Admintepilora/" + expected_mirror + "'"
    assert set(gate["needs"]) == {"lane-ancestry-gate", "gate-ci-status"}
    assert "if" not in gate and "continue-on-error" not in gate
    for name in ("build", "publish-testpypi", "publish-pypi"):
        assert "manifest-gate" in jobs[name]["needs"]
        assert "continue-on-error" not in jobs[name]
        assert "always(" not in jobs[name].get("if", "")
    for name in ("build", "manifest-gate"):
        checkout = next(s for s in jobs[name]["steps"] if s.get("uses", "").startswith("actions/checkout@"))
        assert checkout["with"]["ref"] == "${{ needs.lane-ancestry-gate.outputs.target_sha }}"
    run = next(s["run"] for s in gate["steps"] if "run" in s)
    assert '"$GITHUB_REPOSITORY"' in run and "readiness_gate.py" in run
    assert "--expect-block" not in run and "readiness_ci_fixture.py" not in run


def test_fixture_graph_has_real_failure_and_no_publish_capabilities():
    workflow = yaml.safe_load((ROOT / ".github/workflows/release-gate-fixture.yml").read_text())
    jobs = workflow["jobs"]
    assert workflow["permissions"] == {"contents": "read"}
    assert "continue-on-error" not in jobs["manifest-gate-negative"]
    assert jobs["negative-build-sentinel"]["needs"] == "manifest-gate-negative"
    assert "manifest-gate-negative" in jobs["negative-publish-sentinel"]["needs"]
    assert jobs["manifest-gate-negative"]["outputs"]["blocked"] == "${{ steps.negative.outputs.blocked }}"
    for name in ("negative-build-sentinel", "negative-publish-sentinel"):
        assert jobs[name]["if"] == "needs.manifest-gate-negative.outputs.blocked != 'true'"
    assertion = next(step for step in jobs["manifest-gate-negative"]["steps"] if step.get("id") == "negative")
    assert "--expect-block" in assertion["run"]
    for job in jobs.values():
        assert "environment" not in job and "permissions" not in job
        for step in job["steps"]:
            assert "gh-action-pypi-publish" not in step.get("uses", "")
            assert "pip install" not in step.get("run", "")
