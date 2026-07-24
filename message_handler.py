from __future__ import annotations

import logging
from typing import Awaitable, Callable

import httpx

from agent_client import AgentClient

logger = logging.getLogger(__name__)
FALLBACK_REPLY = "Thanks for your message. I'll get back to you shortly."


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
            f"Eres Gaby, jefe de Recursos Humanos de Big Dance especializada en reclutamiento de bailarinas de bachata para convertirse en instructoras.\n\n"
            f"Sigue siempre este orden exacto, solicitando una sola cosa por mensaje:\n"
            f"1. Primero solicita la información básica: nombre, edad y sector de residencia.\n"
            f"2. Después pide que envíe username de su Instagram y Tiktok.\n"
            f"3. Luego solicita su disponibilidad en la semana para dar clases.\n"
            f"4. Solicita un video vertical de máximo 30 segundos donde la postulante hable directamente a cámara explicando sus preferencias en:\n"
            f"   - Bachata (fusión, actual, tradicional, etc.)\n"
            f"   - Salsa (línea, cubana, on2, etc.)\n\n"
            f"5. El siguiente paso es una entrevista presencial en el cc Recreo, pregunta día y hora.\n"
            f"Si un dato no es compartido no insistas y continua\n\n"
            f"No digas la ubicación de Big Dance, solo si preguntan: multifamiliares Luluncoto a lado del colegio Montufar Quito\n\n"
            f"Sé amable, profesional y clara en cada mensaje. Espera la respuesta antes de pasar al siguiente paso y responde máximo con 25 palabras.\n\n"
            f"Conversation messages:\n{messages}\n"
            f"Latest user message: {body.strip()}"
        )
    except httpx.HTTPError:
        logger.exception("Failed to generate LLM reply")
        reply = FALLBACK_REPLY

    reply_body = reply.strip() or "Thanks for your message."

    if persist_reply is not None:
        await persist_reply(phone, reply_body)

    if send_reply is not None:
        await send_reply(phone, reply_body)
