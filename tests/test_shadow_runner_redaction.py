"""Shadow-runner redaction tests (issue #102 P1).

We don't run the full shadow loop here — just exercise the digest
serialization helper to confirm provider secrets are stripped before the
JSONL append.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import shadow_runner


def test_redact_digest_payload_strips_per_source_secrets() -> None:
    digest = {
        "concepts_covered": 1, "concepts_total": 1, "coverage_pct": 100.0,
        "total_obs": 0,
        "source_results": {
            "fred_daily": {
                "stored": 0,
                "error": (
                    "HTTPSConnectionPool: /fred?series_id=T5YIE"
                    "&api_key=DEADBEEF&file_type=json"
                ),
                "ms": 0,
            },
            "bls": {"stored": 5, "error": "", "ms": 12},
        },
    }

    cleaned = shadow_runner._redact_digest_payload(digest)

    assert "DEADBEEF" not in json.dumps(cleaned)
    assert cleaned["source_results"]["fred_daily"]["error"].count("api_key=***") == 1
    # Untouched per-source entries pass through.
    assert cleaned["source_results"]["bls"]["error"] == ""
    assert cleaned["source_results"]["bls"]["stored"] == 5
    # Original digest is not mutated in place.
    assert "DEADBEEF" in digest["source_results"]["fred_daily"]["error"]


def test_write_digest_redacts_before_append(tmp_path, monkeypatch) -> None:
    digest_file = tmp_path / "daily_digest.jsonl"
    monkeypatch.setattr(shadow_runner, "DIGEST_FILE", digest_file)

    shadow_runner.write_digest({
        "concepts_covered": 1, "concepts_total": 1, "coverage_pct": 100.0,
        "total_obs": 7,
        "confirmed_24h": 1,
        "source_results": {
            "fred_daily": {
                "stored": 0,
                "error": "boom token=SECRETTOKEN x=1",
                "ms": 0,
            },
        },
    })

    persisted = digest_file.read_text().strip()
    assert "SECRETTOKEN" not in persisted
    payload = json.loads(persisted)
    assert payload["source_results"]["fred_daily"]["error"] == "boom token=*** x=1"
