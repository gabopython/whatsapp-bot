from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable

import httpx

from agent_client import AgentClient

logger = logging.getLogger(__name__)


def _conversation_has_tag(tags: str, tag: str) -> bool:
    """Case-insensitive check for a comma-separated tag list."""
    return tag.strip().lower() in {t.strip().lower() for t in tags.split(",") if t.strip()}


async def handle_incoming_message(
    phone: str,
    body: str,
    conversation_history: str | None = None,
    agent_client: AgentClient | None = None,
    send_reply: Callable[[str, str], Awaitable[None]] | None = None,
    persist_reply: Callable[[str, str], Awaitable[None]] | None = None,
    tags: str = "",
    delay_seconds: float = 60.0,
) -> None:
    """Generate an AI reply for an incoming message and send it back.

    No message is sent when the conversation is tagged "noia" or when the
    AI generation fails/returns empty output. Those cases are only logged
    to the console.
    """
    if not body or not body.strip():
        return

    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    messages = (conversation_history or f"them:{body.strip()}").strip()
    if _conversation_has_tag(tags, "noia"):
        print(messages)
        print("=" * 80)
        print("noia    ", phone)
        return

    # If the incoming message contains an audio media tag, log and return
    # Expected marker in body: 'media:audio' (case-insensitive)
    if "media:audio" in body.lower():
        print("audio message", phone)
        print(body)
        return

    client = agent_client or AgentClient()

    prompt = (
        f"Eres Gaby, jefe de Recursos Humanos de Big Dance especializada en reclutamiento de bailarinas de bachata para convertirse en instructoras.\n\n"
        f"Sigue siempre este orden exacto, solicitando una sola cosa por mensaje:\n"
        f"1. Primero solicita la información básica: nombre, edad y sector de residencia.\n"
        f"2. Después pide que envíe username de su Instagram y Tiktok.\n"
        f"3. Luego solicita su disponibilidad en la semana para dar clases.\n"
        f"4. Solicita un video vertical de máximo 30 segundos donde la postulante hable directamente a cámara explicando sus preferencias en:\n"
        f"   - Bachata (fusión, actual, tradicional, etc.)\n"
        f"   - Salsa (línea, cubana, on2, etc.)\n\n"
        f"5. El siguiente paso es una entrevista presencial en el cc Recreo, pregunta día y hora.\n"
        f"\nSi un dato no es compartido no insistas y continua\n\n"
        f"No digas la ubicación de Big Dance, solo si preguntan: multifamiliares Luluncoto a lado del colegio Montufar Quito\n\n"
        f"Sé amable y profesional. Espera la respuesta antes de pasar al siguiente paso y responde máximo con 20 palabras con saltos de línea.\n\n"
        f"Conversation messages:\n{messages}\n"
    )

    print("=" * 80)
    print(f"[PROMPT for {phone}]")
    print(prompt)
    print("=" * 80)

    try:
        reply = await client.generate_agent(prompt)
    except httpx.HTTPError:
        logger.exception("Failed to generate LLM reply")
        print("ia failed1")
        return

    reply_body = reply.strip() if reply else ""
    if not reply_body:
        print("ia failed")
        return

    if persist_reply is not None:
        await persist_reply(phone, reply_body)

    if send_reply is not None:
        await send_reply(phone, reply_body)