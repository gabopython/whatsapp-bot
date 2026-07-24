from __future__ import annotations

from typing import Awaitable, Callable

from agent_client import AgentClient


async def handle_incoming_message(
    phone: str,
    body: str,
    agent_client: AgentClient | None = None,
    send_reply: Callable[[str, str], Awaitable[None]] | None = None,
) -> None:
    """Generate an AI reply for an incoming message and send it back."""
    if not body or not body.strip():
        return

    client = agent_client or AgentClient()
    reply = await client.generate_agent(
        f"You are a helpful assistant. Reply to the user in a concise and friendly way.\n"
        f"User message: {body.strip()}"
    )

    if send_reply is not None:
        await send_reply(phone, reply.strip() or "Thanks for your message.")
