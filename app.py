import asyncio
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse
from sqlalchemy import Integer, String, Text, desc, event, select
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


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class Base(DeclarativeBase):
    pass


class Message(Base):
    __tablename__ = "messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    direction: Mapped[str] = mapped_column(String(8), nullable=False)
    phone: Mapped[str] = mapped_column(String(32), index=True, nullable=False)
    body: Mapped[str] = mapped_column(Text, nullable=False)
    whatsapp_message_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    status: Mapped[str] = mapped_column(String(32), default="stored", nullable=False)
    raw_json: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[str] = mapped_column(
        String(64),
        default=now_iso,
        index=True,
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


async def init_db() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
        await conn.exec_driver_sql(
            "CREATE INDEX IF NOT EXISTS ix_messages_phone_created_at "
            "ON messages (phone, created_at DESC)"
        )


async def save_message(
    session: AsyncSession,
    *,
    direction: str,
    phone: str,
    body: str,
    whatsapp_message_id: str | None = None,
    status: str = "stored",
    raw_json: Any = None,
) -> int:
    message = Message(
        direction=direction,
        phone=phone,
        body=body,
        whatsapp_message_id=whatsapp_message_id,
        status=status,
        raw_json=json.dumps(raw_json) if raw_json is not None else None,
    )
    session.add(message)
    await session.commit()
    await session.refresh(message)
    return message.id


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
                    message_type = message.get("type", "")
                    if message_type == "text":
                        body = message.get("text", {}).get("body", "")
                    else:
                        body = f"[{message_type} message]"

                    await save_message(
                        session,
                        direction="in",
                        phone=message.get("from", ""),
                        body=body,
                        whatsapp_message_id=message.get("id"),
                        status="received",
                        raw_json=message,
                    )
                    stored += 1
    return stored


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http_client, worker_task

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


@app.get("/", response_class=HTMLResponse)
async def home() -> str:
    return (APP_DIR / "index.html").read_text(encoding="utf-8")


@app.get("/messages")
async def list_messages(
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    phone: str | None = None,
) -> list[dict[str, Any]]:
    statement = select(Message)
    if phone:
        statement = statement.where(Message.phone == phone)
    statement = statement.order_by(desc(Message.created_at)).limit(limit).offset(offset)

    async with SessionLocal() as session:
        rows = (await session.scalars(statement)).all()
        return [
            {
                "id": row.id,
                "direction": row.direction,
                "phone": row.phone,
                "body": row.body,
                "whatsapp_message_id": row.whatsapp_message_id,
                "status": row.status,
                "created_at": row.created_at,
            }
            for row in rows
        ]


@app.post("/send")
async def send_message(to: str = Form(...), body: str = Form(...)) -> dict[str, Any]:
    response = await send_whatsapp_text(to=to, body=body)
    message_id = None
    if response.get("messages"):
        message_id = response["messages"][0].get("id")

    async with SessionLocal() as session:
        local_id = await save_message(
            session,
            direction="out",
            phone=to,
            body=body,
            whatsapp_message_id=message_id,
            status="sent",
            raw_json=response,
        )
    return {"ok": True, "id": local_id, "meta_response": response}


@app.get("/webhook")
async def verify_webhook(request: Request) -> PlainTextResponse:
    params = request.query_params
    mode = params.get("hub.mode")
    token = params.get("hub.verify_token")
    challenge = params.get("hub.challenge")

    if mode == "subscribe" and token == VERIFY_TOKEN and challenge:
        return PlainTextResponse(challenge)

    raise HTTPException(status_code=403, detail="Webhook verification failed")


@app.post("/webhook")
async def receive_webhook(request: Request) -> dict[str, Any]:
    payload = await request.json()
    try:
        webhook_queue.put_nowait(payload)
    except asyncio.QueueFull as exc:
        raise HTTPException(status_code=503, detail="Webhook queue is full") from exc

    return {"ok": True, "queued": True}
