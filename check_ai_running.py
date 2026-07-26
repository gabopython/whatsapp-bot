import asyncio
import sys

from agent_client import AgentClient

PROMPT = "how to dance on2"


async def test_generate_agent() -> str:
    client = AgentClient()
    try:
        return await client.generate_agent(PROMPT)
    finally:
        await client.close()


async def probe_ai() -> str:
    return await test_generate_agent()


async def main() -> int:
    try:
        response = await probe_ai()
    except Exception as exc:  # pragma: no cover - exercised in tests
        print(f"AI check failed: {exc}")
        return 1

    print(response)
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
