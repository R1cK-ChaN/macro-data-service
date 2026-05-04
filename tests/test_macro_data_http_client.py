from __future__ import annotations

from typing import Any

from macro_data.client import HttpMacroDataClient, MacroDataHttpConfig


def test_http_config_reads_dev_env_file(monkeypatch, tmp_path) -> None:
    env_file = tmp_path / "dev.env"
    env_file.write_text(
        "\n".join([
            "ANALYST_MACRO_DATA_BASE_URL=https://macro-data.example",
            "ANALYST_MACRO_DATA_API_TOKEN=secret",
            "ANALYST_MACRO_DATA_TIMEOUT_SECONDS=7",
        ]),
        encoding="utf-8",
    )
    monkeypatch.delenv("ANALYST_MACRO_DATA_BASE_URL", raising=False)
    monkeypatch.delenv("ANALYST_MACRO_DATA_API_TOKEN", raising=False)
    monkeypatch.delenv("ANALYST_MACRO_DATA_TIMEOUT_SECONDS", raising=False)
    monkeypatch.setenv("MACRO_DATA_ENV_FILES", str(env_file))

    from env import clear_env_cache

    clear_env_cache()
    config = MacroDataHttpConfig.from_env()
    clear_env_cache()

    assert config == MacroDataHttpConfig(
        base_url="https://macro-data.example",
        api_token="secret",
        timeout_seconds=7.0,
    )


def test_http_client_sends_x_api_key(monkeypatch) -> None:
    captured: dict[str, Any] = {}

    class _Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, Any]:
            return {"ok": True}

    def fake_post(
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any],
        timeout: float,
    ) -> _Response:
        captured.update({
            "url": url,
            "headers": headers,
            "json": json,
            "timeout": timeout,
        })
        return _Response()

    monkeypatch.setattr("macro_data.client.httpx.post", fake_post)

    client = HttpMacroDataClient(
        MacroDataHttpConfig(
            base_url="http://macro-data.test",
            api_token="secret",
            timeout_seconds=3.0,
        )
    )
    assert client.invoke("list_items", {"family": "calendar"}) == {"ok": True}

    assert captured["url"] == "http://macro-data.test/v1/ops/list_items"
    assert captured["headers"] == {
        "Content-Type": "application/json",
        "X-API-Key": "secret",
    }
    assert captured["json"] == {"arguments": {"family": "calendar"}}
    assert captured["timeout"] == 3.0
