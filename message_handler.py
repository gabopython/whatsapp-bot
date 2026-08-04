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

    if not _conversation_has_tag(tags, "ubicacion"):
        # Extract only the last user message after the last 'me:' marker.
        last_message = body.strip()
        if conversation_history:
            idx = conversation_history.lower().rfind("me:")
            if idx != -1:
                last_message = conversation_history[idx:].strip()
        
        prompt = (
            "Eres un asistente de clasificación. Tu único trabajo es determinar si el usuario está preguntando por una dirección física, ubicación, dónde estamos o direcciones.\n"
            "Responde con exactamente una palabra: SI o NO. No incluyas ningún otro texto.\n\n"
            f"chat: {last_message}"
        )
        try:
            reply = await client.generate_router(prompt)
        except httpx.HTTPError:
            print("router fail ubicacion", phone)
        reply_body = reply.strip() if reply else ""
        print("router ubicacion", reply_body, phone)

        if reply_body == "SI":
            if persist_reply is not None:
                await persist_reply(phone, 'https://maps.app.goo.gl/VtPrgAcFKQ76F5387')

            if send_reply is not None:
                await send_reply(phone, 'https://maps.app.goo.gl/VtPrgAcFKQ76F5387')
            

    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    prompt = (
        "Eres Fer, profe de Bachata en Big Dance. " 
        "Vendes con entusiasmo el curso vacacional de Salsa y Bachata repondes máximo con 15 palabras.\n\n"
        "Debes enviar como primer mensaje: " 
        "Hola soy Fer. Tienes alguna pregunta? o deseas reservar un cupo antes que se cierren las inscripciones del curso vacacional de Salsa y Bachata?\n"
        "Y solo si te preguntan respondes la info general:\n"
        "Horario: Miércoles y viernes, 10 a 12.\n"
        "Inicio: Viernes 7 de agosto (4 semanas).\n"
        "Edad: 14 a 20 años.\n"
        "Lugar: Casa comunal multifamiliares luluncoto.\n"
        "Pago: Banco Pichincha, Ahorro transaccional 2207853165, Gabriel Revelo León.\n"
        "Inversión: $20 USD\n"
        "+4 instructoras\n\n"
        f"chat:\n{messages}\n"
    )

    try:
        reply = await client.generate_agent(prompt)
    except httpx.HTTPError:
        print("ia failed1", phone)
        return

    reply_body = reply.strip() if reply else ""

    if persist_reply is not None:
        await persist_reply(phone, reply_body)

    if send_reply is not None:
        await send_reply(phone, reply_body)