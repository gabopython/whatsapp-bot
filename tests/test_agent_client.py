import asyncio
import unittest

import httpx

from agent_client import AgentClient


class AgentClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_generate_retries_after_transport_error(self) -> None:
        class DummyHttpClient:
            def __init__(self) -> None:
                self.calls = 0

            async def post(self, url: str, json: dict[str, object]) -> httpx.Response:
                await asyncio.sleep(0)
                self.calls += 1
                if self.calls == 1:
                    raise httpx.ReadTimeout("timed out")
                return httpx.Response(
                    200,
                    json={"response": "hello"},
                    request=httpx.Request("POST", url),
                )

        client = AgentClient(base_url="http://llm.test")
        await client.client.aclose()
        dummy_http_client = DummyHttpClient()
        client.client = dummy_http_client  # type: ignore[assignment]
        client.retry_delay = 0

        reply = await client.generate("test-model", "hi")

        self.assertEqual(reply, "hello")
        self.assertEqual(dummy_http_client.calls, 2)


if __name__ == "__main__":
    unittest.main()
