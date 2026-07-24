from __future__ import annotations

import logging
from typing import Awaitable, Callable

import httpx

from agent_client import AgentClient

logger = logging.getLogger(__name__)


async def handle_incoming_message(
    phone: str,
    body: str,
    conversation_history: str | None = None,
    agent_client: AgentClient | None = None,
    send_reply: Callable[[str, str], Awaitable[None]] | None = None,
    persist_reply: Callable[[str, str], Awaitable[None]] | None = None,
) -> None:
    """Generate an AI reply for an incoming message and send it back."""
    if not body or not body.strip():
        return

    client = agent_client or AgentClient()
    messages = (conversation_history or f"them:{body.strip()}").strip()
    try:
        reply = await client.generate_agent(
            f"u r mi assitant ur goal is to ask for polygel nails price. Reply to the user in a concise and friendly way. \n"
            f"Conversation messages:\n{messages}\n"
            f"Latest user message: {body.strip()}"
        )
    except httpx.HTTPError:
        logger.exception("Failed to generate LLM reply")
        reply = "Thanks for your message. I'll get back to you shortly."

    reply_body = reply.strip() or "Thanks for your message."

    if persist_reply is not None:
        await persist_reply(phone, reply_body)

    if send_reply is not None:
        await send_reply(phone, reply_body)
