import os

import httpx


class AgentClient:
    # Model roles
    MODEL_AGENT = "qwen3:8b"       # Model A: instruction following, tool use, reasoning
    MODEL_ROUTER = "qwen3.5:4b"    # Model B: classification, labeling, routing

    def __init__(self, base_url: str = "http://192.168.1.121:11434"):
        self.base_url = base_url
        timeout = float(os.getenv("LLM_TIMEOUT_SECONDS", "180"))
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(timeout))

    async def generate(self, model: str, prompt: str) -> str:
        response = await self.client.post(
            f"{self.base_url}/api/generate",
            json={"model": model, "prompt": prompt, "stream": False}
        )
        response.raise_for_status()
        return response.json()["response"]

    async def generate_agent(self, prompt: str) -> str:
        """Model A — instruction following, tool use, multi-step reasoning."""
        return await self.generate(self.MODEL_AGENT, prompt)

    async def generate_router(self, prompt: str) -> str:
        """Model B — classification, labeling, routing decisions."""
        return await self.generate(self.MODEL_ROUTER, prompt)

    async def close(self):
        await self.client.aclose()
