import asyncio

from agent_client import AgentClient


async def test_generate_agent() -> str:
    client = AgentClient()
    prompt = "hi"

    try:
        print(f"Sending prompt to {getattr(client, 'MODEL_AGENT', 'unknown')}...")
        response = await client.generate_agent(prompt)
        print("\n--- AI Response ---")
        print(response)
        print("-------------------\n")
        return response
    except Exception as exc:
        print(f"An error occurred: {exc}")
        raise
    finally:
        await client.close()


async def probe_ai() -> str:
    return await test_generate_agent()


async def main() -> int:
    try:
        await probe_ai()
        return 0
    except Exception as exc:
        print(exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))