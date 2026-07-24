# Basic WhatsApp FastAPI App

Small FastAPI app to send WhatsApp Cloud API text messages, receive webhook messages, and store everything in async Postgres. If no Postgres URL is configured, it falls back to SQLite with WAL mode enabled.

## Setup

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env`:

```env
WHATSAPP_TOKEN=your_meta_access_token
PHONE_NUMBER_ID=1286253821226923
VERIFY_TOKEN=choose_a_webhook_verify_token
API_VERSION=v25.0
DATABASE_URL=sqlite+aiosqlite:///messages.db
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10
WEBHOOK_QUEUE_SIZE=1000
LLM_TIMEOUT_SECONDS=180
```

For Postgres, set:

```env
DATABASE_URL=postgresql+asyncpg://postgres:postgres@localhost:5432/whatsapp_messages
```

## Run

```powershell
uvicorn app:app --reload
```

Open:

```text
http://127.0.0.1:8000
```

## Webhook

Use a public tunnel such as ngrok while developing:

```powershell
ngrok http 8000
```

In Meta, set the callback URL to:

```text
https://your-ngrok-url.ngrok-free.app/webhook
```

Use the same `VERIFY_TOKEN` value from `.env`.

## API

Send a message:

```powershell
curl -X POST http://127.0.0.1:8000/send `
  -F "to=593962052098" `
  -F "body=What can I help you today?"
```

List saved messages:

```text
GET http://127.0.0.1:8000/messages
```

Pagination and phone filtering:

```text
GET http://127.0.0.1:8000/messages?limit=50&offset=0
GET http://127.0.0.1:8000/messages?phone=593962052098&limit=25
```

Incoming webhooks return immediately with `{"queued": true}`. Message storage happens in a background `asyncio.Queue` worker so Meta does not have to wait on database writes.
