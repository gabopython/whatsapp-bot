import asyncio
import logging
import mimetypes
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import Integer, String, Text, desc, event, inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from message_handler import handle_incoming_message

load_dotenv()

# Ensure WhatsApp voice notes (ogg/opus) get the correct extension
mimetypes.add_type("audio/ogg", ".ogg")

logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).parent
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{APP_DIR / 'messages.db'}")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify-token")
API_VERSION = os.getenv("API_VERSION", "v25.0")
WEBHOOK_QUEUE_SIZE = int(os.getenv("WEBHOOK_QUEUE_SIZE", "1000"))
REPLY_DELAY_SECONDS = float(os.getenv("REPLY_DELAY_SECONDS", "60"))

MEDIA_DIR = Path(os.getenv("MEDIA_DIR", str(APP_DIR / "media")))
MEDIA_URL_PREFIX = "/media"


def now_iso() -> str:
    return datetime.now().isoformat()


class Base(DeclarativeBase):
    pass


class Conversation(Base):
    __tablename__ = "conversations"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    phone: Mapped[str] = mapped_column(String(32), index=True, nullable=False, unique=True)
    messages: Mapped[str] = mapped_column(Text, nullable=False, default="")
    sent_messages_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    tags: Mapped[str] = mapped_column(Text, nullable=False, default="")
    created_at: Mapped[str] = mapped_column(
        String(64),
        default=now_iso,
        nullable=False,
    )
    updated_at: Mapped[str] = mapped_column(
        String(64),
        default=now_iso,
        nullable=False,
    )


connect_args: dict[str, Any] = {}
engine_kwargs: dict[str, Any] = {"pool_pre_ping": True}
if DATABASE_URL.startswith("sqlite"):
    connect_args["check_same_thread"] = False
else:
    engine_kwargs["pool_size"] = int(os.getenv("DB_POOL_SIZE", "5"))
    engine_kwargs["max_overflow"] = int(os.getenv("DB_MAX_OVERFLOW", "10"))

engine = create_async_engine(DATABASE_URL, connect_args=connect_args, **engine_kwargs)
SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

if DATABASE_URL.startswith("sqlite"):
    @event.listens_for(engine.sync_engine, "connect")
    def enable_sqlite_wal(dbapi_connection: Any, _: Any) -> None:
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.execute("PRAGMA synchronous=NORMAL")
        cursor.close()


webhook_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=WEBHOOK_QUEUE_SIZE)
worker_task: asyncio.Task[None] | None = None
http_client: httpx.AsyncClient | None = None
pending_reply_batches: dict[str, dict[str, Any]] = {}


def _ensure_conversation_columns(sync_conn: Any) -> None:
    inspector = inspect(sync_conn)
    existing_columns = {column["name"] for column in inspector.get_columns("conversations")}

    if "sent_messages_count" not in existing_columns:
        sync_conn.execute(
            text("ALTER TABLE conversations ADD COLUMN sent_messages_count INTEGER NOT NULL DEFAULT 0")
        )

    if "tags" not in existing_columns:
        sync_conn.execute(text("ALTER TABLE conversations ADD COLUMN tags TEXT NOT NULL DEFAULT ''"))


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.run_sync(_ensure_conversation_columns)


def normalize_message_body(message: dict[str, Any]) -> str:
    message_type = message.get("type", "")
    if message_type == "text":
        return message.get("text", {}).get("body", "")

    # Added "audio", "voice", and "document" to supported media types
    if message_type in {"image", "video", "audio", "voice", "document"}:
        media_payload = message.get(message_type) or {}
        caption = (
            message.get("caption", {}).get("body", "")
            or message.get("text", {}).get("body", "")
        )
        media_url = media_payload.get("url") or media_payload.get("id") or ""
        if caption:
            return f"media:{message_type}:{media_url}|{caption}"
        return f"media:{message_type}:{media_url}"

    return f"[{message_type} message]"


def _find_existing_media_file(media_id: str) -> Path | None:
    if not MEDIA_DIR.exists():
        return None
    matches = sorted(MEDIA_DIR.glob(f"{media_id}.*"))
    return matches[0] if matches else None


async def fetch_media_metadata(media_id: str) -> dict[str, Any]:
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")

    response = await http_client.get(f"https://graph.facebook.com/{API_VERSION}/{media_id}")
    if response.is_error:
        logger.warning("Failed to resolve media metadata for %s: %s", media_id, response.text)
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


async def download_and_store_media(media_id: str) -> Path:
    existing = _find_existing_media_file(media_id)
    if existing is not None:
        return existing

    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")

    metadata = await fetch_media_metadata(media_id)
    remote_url = metadata.get("url")
    mime_type = metadata.get("mime_type", "application/octet-stream")
    if not remote_url:
        raise HTTPException(status_code=502, detail="WhatsApp API returned no media URL")

    file_response = await http_client.get(remote_url)
    if file_response.is_error:
        logger.warning("Failed to download media %s: %s", media_id, file_response.text)
        raise HTTPException(status_code=file_response.status_code, detail=file_response.text)

    extension = mimetypes.guess_extension(mime_type.split(";")[0].strip()) or ".bin"
    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    file_path = MEDIA_DIR / f"{media_id}{extension}"
    file_path.write_bytes(file_response.content)
    return file_path


async def resolve_media_url(message: dict[str, Any]) -> str:
    message_type = message.get("type", "")

    # Allow mapping for audio, voice and documents
    if message_type not in {"image", "video", "audio", "voice", "document"}:
        return ""

    media_payload = message.get(message_type) or {}
    media_id = media_payload.get("id") or ""
    if not media_id:
        return media_payload.get("url") or ""

    try:
        file_path = await download_and_store_media(media_id)
    except HTTPException:
        return f"{MEDIA_URL_PREFIX}/{media_id}"

    return f"{MEDIA_URL_PREFIX}/{file_path.name}"


async def build_message_body(message: dict[str, Any]) -> str:
    message_type = message.get("type", "")
    if message_type == "text":
        return message.get("text", {}).get("body", "")

    # Expanded to handle audio/voice messages securely
    if message_type in {"image", "video", "audio", "voice", "document"}:
        media_payload = message.get(message_type) or {}
        caption = (
            message.get("caption", {}).get("body", "")
            or message.get("text", {}).get("body", "")
        )
        media_url = await resolve_media_url(message)
        if caption:
            return f"media:{message_type}:{media_url}|{caption}"
        return f"media:{message_type}:{media_url}"

    return f"[{message_type} message]"


async def save_message(
    session: AsyncSession,
    *,
    direction: str,
    phone: str,
    body: str,
    tags: str = "",
) -> str:
    conversation = await session.scalar(
        select(Conversation).where(Conversation.phone == phone)
    )

    message_line = f"{direction}:{body}"

    if conversation:
        conversation.messages += "\n" + message_line
        conversation.updated_at = now_iso()
        if direction == "me":
            conversation.sent_messages_count += 1
            if conversation.sent_messages_count == 5:
                current_tags = [tag.strip() for tag in conversation.tags.split(",") if tag.strip()]
                if "noia" not in {tag.lower() for tag in current_tags}:
                    conversation.tags = ",".join(current_tags + ["noia"])
        if tags:
            conversation.tags = tags
            if direction == "me" and conversation.sent_messages_count == 5:
                current_tags = [tag.strip() for tag in conversation.tags.split(",") if tag.strip()]
                if "noia" not in {tag.lower() for tag in current_tags}:
                    conversation.tags = ",".join(current_tags + ["noia"])
    else:
        conversation = Conversation(
            phone=phone,
            messages=message_line,
            sent_messages_count=1 if direction == "me" else 0,
            tags=tags,
        )
        if direction == "me" and conversation.sent_messages_count == 5 and tags:
            current_tags = [tag.strip() for tag in conversation.tags.split(",") if tag.strip()]
            if "noia" not in {tag.lower() for tag in current_tags}:
                conversation.tags = ",".join(current_tags + ["noia"])
        session.add(conversation)

    await session.commit()
    await session.refresh(conversation)
    return conversation.phone


async def send_whatsapp_text(to: str, body: str) -> dict[str, Any]:
    if not PHONE_NUMBER_ID or not WHATSAPP_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Missing PHONE_NUMBER_ID or WHATSAPP_TOKEN in .env",
        )
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")

    url = f"https://graph.facebook.com/{API_VERSION}/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "text",
        "text": {"body": body},
    }

    response: httpx.Response | None = None
    for attempt in range(4):
        response = await http_client.post(url, json=payload)
        if response.status_code != 429:
            break
        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after else min(2 ** attempt, 8)
        await asyncio.sleep(delay)

    if response is None:
        raise HTTPException(status_code=502, detail="No response from WhatsApp API")
    if response.is_error:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


async def upload_whatsapp_media(file_bytes: bytes, filename: str, mime_type: str) -> str:
    """Upload local file bytes to the WhatsApp Media endpoint and return the media id."""
    if not PHONE_NUMBER_ID or not WHATSAPP_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Missing PHONE_NUMBER_ID or WHATSAPP_TOKEN in .env",
        )
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")

    url = f"https://graph.facebook.com/{API_VERSION}/{PHONE_NUMBER_ID}/media"
    files = {"file": (filename, file_bytes, mime_type)}
    data = {"messaging_product": "whatsapp", "type": mime_type}

    response = await http_client.post(url, data=data, files=files)
    if response.is_error:
        logger.warning("Failed to upload media %s: %s", filename, response.text)
        raise HTTPException(status_code=response.status_code, detail=response.text)

    result = response.json()
    media_id = result.get("id")
    if not media_id:
        raise HTTPException(status_code=502, detail="WhatsApp API did not return a media id")
    return media_id


async def send_whatsapp_image(
    to: str,
    *,
    image_url: str | None = None,
    media_id: str | None = None,
    caption: str | None = None,
) -> dict[str, Any]:
    if not PHONE_NUMBER_ID or not WHATSAPP_TOKEN:
        raise HTTPException(
            status_code=500,
            detail="Missing PHONE_NUMBER_ID or WHATSAPP_TOKEN in .env",
        )
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")
    if not image_url and not media_id:
        raise HTTPException(status_code=400, detail="Either image_url or media_id must be provided")

    image_payload: dict[str, Any] = {}
    if media_id:
        image_payload["id"] = media_id
    else:
        image_payload["link"] = image_url
    if caption:
        image_payload["caption"] = caption

    url = f"https://graph.facebook.com/{API_VERSION}/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to,
        "type": "image",
        "image": image_payload,
    }

    response: httpx.Response | None = None
    for attempt in range(4):
        response = await http_client.post(url, json=payload)
        if response.status_code != 429:
            break
        retry_after = response.headers.get("Retry-After")
        delay = float(retry_after) if retry_after else min(2 ** attempt, 8)
        await asyncio.sleep(delay)

    if response is None:
        raise HTTPException(status_code=502, detail="No response from WhatsApp API")
    if response.is_error:
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


async def webhook_worker() -> None:
    while True:
        payload = await webhook_queue.get()
        try:
            await store_webhook_messages(payload)
        except Exception:
            logger.exception("Failed to process webhook payload")
        finally:
            webhook_queue.task_done()


async def _schedule_delayed_reply(
    phone: str,
    *,
    body: str,
    conversation_history: str,
    tags: str,
) -> None:
    if not body or not body.strip():
        return

    if "media:audio" in body.lower():
        return

    pending = pending_reply_batches.get(phone)
    if pending is None:
        pending = {
            "body": body,
            "conversation_history": conversation_history,
            "tags": tags,
            "send_reply": lambda to, reply: send_whatsapp_text(to=to, body=reply),
        }
        pending_reply_batches[phone] = pending

        async def _run_delayed_reply() -> None:
            await asyncio.sleep(REPLY_DELAY_SECONDS)
            current_pending = pending_reply_batches.get(phone)
            if current_pending is not pending:
                return
            pending_reply_batches.pop(phone, None)

            async def persist_reply(to: str, reply: str) -> None:
                async with SessionLocal() as reply_session:
                    await save_message(
                        reply_session,
                        direction="me",
                        phone=to,
                        body=reply,
                    )

            await handle_incoming_message(
                phone=phone,
                body=current_pending["body"],
                conversation_history=current_pending["conversation_history"],
                tags=current_pending["tags"],
                send_reply=current_pending["send_reply"],
                persist_reply=persist_reply,
                delay_seconds=0.0,
            )

        asyncio.create_task(_run_delayed_reply())
        return

    pending["body"] = body
    pending["conversation_history"] = conversation_history
    pending["tags"] = tags


async def store_webhook_messages(payload: dict[str, Any]) -> int:
    stored = 0
    async with SessionLocal() as session:
        for entry in payload.get("entry", []):
            for change in entry.get("changes", []):
                value = change.get("value", {})
                for message in value.get("messages", []):
                    phone = message.get("from", "")
                    body = await build_message_body(message)

                    await save_message(
                        session,
                        direction="them",
                        phone=phone,
                        body=body,
                    )
                    conversation = await session.scalar(
                        select(Conversation).where(Conversation.phone == phone)
                    )

                    await _schedule_delayed_reply(
                        phone=phone,
                        body=body,
                        conversation_history=conversation.messages if conversation else body,
                        tags=conversation.tags if conversation else "",
                    )
                    stored += 1
    return stored


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client, worker_task

    MEDIA_DIR.mkdir(parents=True, exist_ok=True)
    await init_db()
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(20.0),
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
        headers={"Authorization": f"Bearer {WHATSAPP_TOKEN}"},
    )
    worker_task = asyncio.create_task(webhook_worker())

    try:
        yield
    finally:
        if worker_task:
            worker_task.cancel()
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        if http_client:
            await http_client.aclose()
        await engine.dispose()


app = FastAPI(title="Scalable WhatsApp Messages App", lifespan=lifespan)

MEDIA_DIR.mkdir(parents=True, exist_ok=True)
app.mount(MEDIA_URL_PREFIX, StaticFiles(directory=str(MEDIA_DIR)), name="media")


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return (APP_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/messages")
async def list_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    phone: str | None = None,
) -> list[dict[str, Any]]:
    statement = select(Conversation)
    if phone:
        statement = statement.where(Conversation.phone == phone)
    statement = statement.order_by(desc(Conversation.updated_at)).limit(limit).offset(offset)

    async with SessionLocal() as session:
        rows = (await session.scalars(statement)).all()
        return [
            {
                "id": row.id,
                "phone": row.phone,
                "messages": row.messages,
                "sent_messages_count": row.sent_messages_count,
                "tags": row.tags,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }
            for row in rows
        ]


@app.post("/send")
async def send_message(
    to: str = Form(...),
    body: str = Form(...),
    tags: str = Form(default=""),
) -> dict[str, Any]:
    response = await send_whatsapp_text(to=to, body=body)

    async with SessionLocal() as session:
        phone = await save_message(
            session,
            direction="me",
            phone=to,
            body=body,
            tags=tags,
        )
    return {"ok": True, "phone": phone, "meta_response": response}


@app.post("/send-image")
async def send_image_message(
    to: str = Form(...),
    caption: str = Form(default=""),
    tags: str = Form(default=""),
    image_url: str | None = Form(default=None),
    file: UploadFile | None = File(default=None),
) -> dict[str, Any]:
    """Send an image either from a public image_url or an uploaded file.

    - If `file` is provided, it's uploaded to WhatsApp's Media API first (to get a media id),
      and a local copy is kept in MEDIA_DIR so it can be viewed from this app too.
    - If `image_url` is provided instead, WhatsApp fetches the image directly from that link.
    """
    if not image_url and not file:
        raise HTTPException(status_code=400, detail="Provide either image_url or a file upload")

    media_id: str | None = None
    stored_url = image_url or ""

    if file is not None:
        file_bytes = await file.read()
        if not file_bytes:
            raise HTTPException(status_code=400, detail="Uploaded file is empty")

        mime_type = (
            file.content_type
            or mimetypes.guess_type(file.filename or "")[0]
            or "application/octet-stream"
        )
        media_id = await upload_whatsapp_media(file_bytes, file.filename or "image", mime_type)

        # Keep a local copy so it can be served/viewed from this app too
        extension = mimetypes.guess_extension(mime_type.split(";")[0].strip()) or ".bin"
        MEDIA_DIR.mkdir(parents=True, exist_ok=True)
        local_path = MEDIA_DIR / f"{media_id}{extension}"
        local_path.write_bytes(file_bytes)
        stored_url = f"{MEDIA_URL_PREFIX}/{local_path.name}"

    response = await send_whatsapp_image(
        to,
        image_url=image_url if not media_id else None,
        media_id=media_id,
        caption=caption or None,
    )

    body = f"media:image:{stored_url}|{caption}" if caption else f"media:image:{stored_url}"

    async with SessionLocal() as session:
        phone = await save_message(
            session,
            direction="me",
            phone=to,
            body=body,
            tags=tags,
        )
    return {"ok": True, "phone": phone, "meta_response": response}


@app.get(f"{MEDIA_URL_PREFIX}/{{media_id}}")
async def get_media(media_id: str) -> FileResponse:
    existing = _find_existing_media_file(media_id)
    if existing is not None:
        return FileResponse(existing)

    file_path = await download_and_store_media(media_id)
    return FileResponse(file_path)


@app.get("/webhook")
async def verify_webhook(request: Request) -> PlainTextResponse:
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN and challenge:
        return PlainTextResponse(challenge)

    raise HTTPException(status_code=403, detail="Webhook verification failed")


@app.post("/tags/{phone}")
async def update_tags(phone: str, tags: str = Form(default="")) -> dict[str, Any]:
    async with SessionLocal() as session:
        conversation = await session.scalar(
            select(Conversation).where(Conversation.phone == phone)
        )
        if not conversation:
            raise HTTPException(status_code=404, detail="Conversation not found")

        conversation.tags = tags
        conversation.updated_at = now_iso()
        await session.commit()
        await session.refresh(conversation)

        return {
            "ok": True,
            "phone": conversation.phone,
            "tags": conversation.tags,
        }


@app.post("/webhook")
async def receive_webhook(request: Request) -> dict[str, Any]:
    payload = await request.json()
    try:
        webhook_queue.put_nowait(payload)
    except asyncio.QueueFull as exc:
        raise HTTPException(status_code=503, detail="Webhook queue is full") from exc

    return {"ok": True, "queued": True}