from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable
from xmlrpc import client

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

    last_message = body.strip()
    if conversation_history:
        idx = conversation_history.lower().rfind("me:")
        if idx != -1:
            last_message = conversation_history[idx:].strip()
        

    if not _conversation_has_tag(tags, "ubicacion"):
        prompt = (
            "Eres un asistente de clasificación. Tu único trabajo es determinar si el usuario está preguntando por una dirección física, ubicación, dónde estamos o direcciones.\n"
            "Responde con exactamente una palabra: SI o NO. No incluyas ningún otro texto.\n\n"
            f"chat: {last_message}"
        )
        try:
            reply = await client.generate_router(prompt)
            reply_body = reply.strip() if reply else ""
        except httpx.HTTPError:
            print("router fail ubicacion", phone)
        print("router ubicacion", reply_body, phone)

        if reply_body == "SI":
            if persist_reply is not None:
                await persist_reply(phone, 'https://maps.app.goo.gl/VtPrgAcFKQ76F5387\n\nCasa comunal multifamiliares Luluncoto\na lado del colegio Montúfar')

            if send_reply is not None:
                await send_reply(phone, 'https://maps.app.goo.gl/VtPrgAcFKQ76F5387\n\nCasa comunal multifamiliares Luluncoto\na lado del colegio Montúfar')
            return

    prompt = (
        "Eres un asistente de clasificación. Tu única tarea es determinar si el usuario pregunta cómo pagar, solicita datos bancarios, desea inscribirse o inscribir a su hijo o desea separar un cupo del curso.\n"
        "Responde con exactamente una palabra: SI o NO. No incluyas ningún otro texto.\n\n"
        f"chat: {last_message}"
    )
    try:
        reply = await client.generate_router(prompt)
        reply_body = reply.strip() if reply else ""
    except httpx.HTTPError:
        print("router fail payment", phone)
    print("router payment", reply_body, phone)

    if reply_body == "SI":
        if persist_reply is not None:
            await persist_reply(phone, 'Para separar un cupo del curso vacacional de Salsa y Bachata, debes realizar el pago de $20 USD a la cuenta de Banco Pichincha,\nAhorro transaccional 2207853165,\nGabriel Revelo León\n\nUna vez realizado el pago, envíanos el comprobante para confirmar tu inscripción.')

        if send_reply is not None:
            await send_reply(phone, 'Para separar un cupo del curso vacacional de Salsa y Bachata, debes realizar el pago de $20 USD a la cuenta de Banco Pichincha,\nAhorro transaccional 2207853165,\nGabriel Revelo León\n\nUna vez realizado el pago, envíanos el comprobante para confirmar tu inscripción.')
        return

    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)

    prompt = (
        "Eres Fer, profe de Bachata en Big Dance. " 
        "Vendes con entusiasmo el curso vacacional de Salsa y Bachata repondes máximo con 15 palabras.\n\n"
        "Debes generar urgencia indicando que pronto cierren las inscripciones del curso vacacional de Salsa y Bachata\n"
        "Si te preguntan por información respondes:\n"
        "Horario: Miércoles y viernes, 10 a 12.\n"
        "Inicio: Viernes 7 de agosto (4 semanas).\n"
        "Inversión: $20 USD\n"
        "---------------------------\n"
        "Información adicional(solo responder si preguntan):\n"
        "Edad: 14 a 20 años.\n"
        "Lugar: Casa comunal multifamiliares luluncoto.\n"
        "Pago: Banco Pichincha, Ahorro transaccional 2207853165, Gabriel Revelo León.\n"
        "Inversión: $20 USD\n"
        "+4 instructoras\n\n"
        "---------------------------\n"
        f"Chat:\n{messages}\n"
    )

    try:
        reply = await client.generate_agent(prompt)
        reply_body = reply.strip() if reply else ""
    except httpx.HTTPError:
        print("ia failed1", phone)
        return


    if persist_reply is not None:
        await persist_reply(phone, reply_body)

    if send_reply is not None:
        await send_reply(phone, reply_body)