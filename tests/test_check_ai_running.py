import asyncio
import importlib

import pytest


@pytest.mark.asyncio
async def test_test_generate_agent_uses_generate_agent(monkeypatch):
    module = importlib.import_module("check_ai_running")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        async def generate_agent(self, prompt):
            assert prompt == "how to dance on2"
            return "Here is a dance tip"

        async def close(self):
            return None

    monkeypatch.setattr(module, "AgentClient", FakeClient)

    response = await module.test_generate_agent()
    assert response == "Here is a dance tip"


@pytest.mark.asyncio
async def test_probe_ai_returns_response(monkeypatch):
    module = importlib.import_module("check_ai_running")

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.args = args
            self.kwargs = kwargs

        async def generate_agent(self, prompt):
            assert prompt == "how to dance on2"
            return "Here is a dance tip"

        async def close(self):
            return None

    monkeypatch.setattr(module, "AgentClient", FakeClient)

    response = await module.probe_ai()
    assert response == "Here is a dance tip"


def test_main_prints_error_on_failure(monkeypatch, capsys):
    module = importlib.import_module("check_ai_running")

    class FailingClient:
        def __init__(self, *args, **kwargs):
            pass

        async def generate_agent(self, prompt):
            raise RuntimeError("offline")

        async def close(self):
            return None

    monkeypatch.setattr(module, "AgentClient", FailingClient)

    exit_code = asyncio.run(module.main())

    captured = capsys.readouterr()
    assert exit_code == 1
    assert "offline" in captured.out
