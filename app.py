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
from fastapi import FastAPI, Form, HTTPException, Query, Request

from message_handler import handle_incoming_message
from fastapi.responses import FileResponse, HTMLResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import Integer, String, Text, desc, event, inspect, select, text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

load_dotenv()

logger = logging.getLogger(__name__)

APP_DIR = Path(__file__).parent
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite+aiosqlite:///{APP_DIR / 'messages.db'}")
PHONE_NUMBER_ID = os.getenv("PHONE_NUMBER_ID", "")
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "")
VERIFY_TOKEN = os.getenv("VERIFY_TOKEN", "verify-token")
API_VERSION = os.getenv("API_VERSION", "v25.0")
WEBHOOK_QUEUE_SIZE = int(os.getenv("WEBHOOK_QUEUE_SIZE", "1000"))

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

    if message_type in {"image", "video"}:
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
    """Look for an already-downloaded file for this media id, regardless of extension."""
    if not MEDIA_DIR.exists():
        return None
    matches = sorted(MEDIA_DIR.glob(f"{media_id}.*"))
    return matches[0] if matches else None


async def fetch_media_metadata(media_id: str) -> dict[str, Any]:
    """Ask the Graph API for the temporary download URL + mime type for a media id."""
    if http_client is None:
        raise HTTPException(status_code=500, detail="HTTP client is not ready")

    response = await http_client.get(f"https://graph.facebook.com/{API_VERSION}/{media_id}")
    if response.is_error:
        logger.warning("Failed to resolve media metadata for %s: %s", media_id, response.text)
        raise HTTPException(status_code=response.status_code, detail=response.text)
    return response.json()


async def download_and_store_media(media_id: str) -> Path:
    """
    Download the actual media bytes from WhatsApp/Graph (the temporary URL requires
    the same Authorization header we already send on http_client) and persist them
    locally so they can be served afterwards without re-hitting Meta's expiring URLs.
    """
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
    """
    Download the media (if not already cached) and return a local, stable URL that
    the frontend can fetch directly (e.g. in an <img> or <video> tag) without needing
    the WhatsApp access token.
    """
    message_type = message.get("type", "")
    if message_type not in {"image", "video"}:
        return ""

    media_payload = message.get(message_type) or {}
    media_id = media_payload.get("id") or ""
    if not media_id:
        return media_payload.get("url") or ""

    try:
        file_path = await download_and_store_media(media_id)
    except HTTPException:
        # Fall back to exposing the on-demand proxy endpoint even if the eager
        # download failed (e.g. transient network issue); it will retry on access.
        return f"{MEDIA_URL_PREFIX}/{media_id}"

    return f"{MEDIA_URL_PREFIX}/{file_path.name}"


async def build_message_body(message: dict[str, Any]) -> str:
    message_type = message.get("type", "")
    if message_type == "text":
        return message.get("text", {}).get("body", "")

    if message_type in {"image", "video"}:
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
        if tags:
            conversation.tags = tags
    else:
        conversation = Conversation(
            phone=phone,
            messages=message_line,
            sent_messages_count=1 if direction == "me" else 0,
            tags=tags,
        )
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


async def webhook_worker() -> None:
    while True:
        payload = await webhook_queue.get()
        try:
            await store_webhook_messages(payload)
        except Exception:
            logger.exception("Failed to process webhook payload")
        finally:
            webhook_queue.task_done()


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

                    async def persist_reply(to: str, reply: str) -> None:
                        await save_message(
                            session,
                            direction="me",
                            phone=to,
                            body=reply,
                        )

                    await handle_incoming_message(
                        phone=phone,
                        body=body,
                        conversation_history=conversation.messages if conversation else body,
                        tags=conversation.tags if conversation else "",
                        send_reply=lambda to, reply: send_whatsapp_text(to=to, body=reply),
                        persist_reply=persist_reply,
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


@app.get(f"{MEDIA_URL_PREFIX}/{{media_id}}")
async def get_media(media_id: str) -> FileResponse:
    """
    Serve a piece of media (image/video/document) by its WhatsApp media id.
    Downloads and caches it locally on first request if it hasn't been fetched yet
    (e.g. if the eager download during webhook processing failed).
    Note: StaticFiles above already serves files by their exact stored filename
    (id + real extension); this route additionally allows lookup by bare id.
    """
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
