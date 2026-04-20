import os
from pathlib import Path
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query

from database.SQLite.execute import DB_PATH, list_tables, read_table
from .model import TelegramUpdate

load_dotenv()


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_DEFAULT_CHAT_ID", "")


app = FastAPI(
	title="Camp Logger Telegram Backend",
	description="Webhook backend and utility API for Telegram bot integration.",
	version="0.1.0",
)


def _build_telegram_api_url(method: str) -> str:
	if not BOT_TOKEN:
		raise HTTPException(status_code=500, detail="TELEGRAM_BOT_TOKEN is not configured")

	return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


async def _send_telegram_message(chat_id: int, text: str) -> dict[str, Any]:
	payload = {
		"chat_id": chat_id,
		"text": text,
	}
	async with httpx.AsyncClient(timeout=15.0) as client:
		response = await client.post(_build_telegram_api_url("sendMessage"), json=payload)

	if response.status_code >= 400:
		raise HTTPException(status_code=502, detail=f"Telegram sendMessage failed: {response.text}")

	data = response.json()
	if not data.get("ok", False):
		raise HTTPException(status_code=502, detail=f"Telegram sendMessage not ok: {data}")

	return data


def _help_text() -> str:
	return (
		"Available commands:\n"
		"/start - basic intro\n"
		"/help - show commands\n"
		"/tables - list SQLite tables\n"
		"/table <name> - preview first 5 rows"
	)


def _table_preview(table_name: str, limit: int = 5) -> str:
	if table_name not in list_tables():
		return f"Table '{table_name}' does not exist."

	columns, rows = read_table(table_name)
	if not rows:
		return f"Table '{table_name}' exists but has no rows."

	shown_rows = rows[:limit]
	lines: list[str] = [f"Preview of {table_name} (max {limit} rows):"]
	lines.append(", ".join(columns))

	for row in shown_rows:
		lines.append(", ".join(str(value) for value in row))

	return "\n".join(lines)


def _handle_text_command(text: str) -> str:
	lowered = text.strip().lower()

	if lowered.startswith("/start"):
		return "Bot backend is online. Use /help to view commands."

	if lowered.startswith("/help"):
		return _help_text()

	if lowered.startswith("/tables"):
		tables = list_tables()
		return "Tables:\n" + "\n".join(tables) if tables else "No tables found."

	if lowered.startswith("/table"):
		parts = text.split(maxsplit=1)
		if len(parts) < 2:
			return "Usage: /table <table_name>"
		return _table_preview(parts[1].strip())

	return "Unknown command. Use /help."


@app.get("/")
def root() -> dict[str, str]:
	return {"message": "Telegram backend is running"}


@app.get("/health")
def health() -> dict[str, str]:
	db_exists = DB_PATH.exists()
	return {
		"status": "ok",
		"database": "ready" if db_exists else "missing",
	}


@app.get("/api/tables")
def get_tables() -> dict[str, list[str]]:
	return {"tables": list_tables()}


@app.get("/api/table/{table_name}")
def get_table_data(table_name: str, limit: int = Query(default=100, ge=1, le=1000)) -> dict[str, Any]:
	if table_name not in list_tables():
		raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

	columns, rows = read_table(table_name)
	records = [dict(zip(columns, row)) for row in rows[:limit]]
	return {
		"table": table_name,
		"count": len(records),
		"records": records,
	}


@app.post("/telegram/webhook/{secret}")
async def telegram_webhook(
	secret: str,
	update: TelegramUpdate,
	x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, Any]:
	if WEBHOOK_SECRET and secret != WEBHOOK_SECRET:
		raise HTTPException(status_code=403, detail="Invalid webhook path secret")

	if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
		raise HTTPException(status_code=403, detail="Invalid Telegram secret header")

	message = update.message
	if not message or not message.text:
		return {"ok": True, "handled": False, "reason": "No text message"}

	reply_text = _handle_text_command(message.text)
	await _send_telegram_message(chat_id=message.chat.id, text=reply_text)
	return {"ok": True, "handled": True}


@app.post("/telegram/send-test")
async def send_test_message(chat_id: int | None = None) -> dict[str, Any]:
	target_chat_id = chat_id
	if target_chat_id is None:
		if not DEFAULT_CHAT_ID:
			raise HTTPException(
				status_code=400,
				detail="chat_id is required when TELEGRAM_DEFAULT_CHAT_ID is not configured",
			)
		target_chat_id = int(DEFAULT_CHAT_ID)

	result = await _send_telegram_message(target_chat_id, "Test message from FastAPI backend.")
	return {"ok": True, "telegram": result}


@app.post("/telegram/set-webhook")
async def set_webhook(public_base_url: str) -> dict[str, Any]:
	if not public_base_url.startswith("https://"):
		raise HTTPException(status_code=400, detail="public_base_url must start with https://")

	webhook_url = f"{public_base_url.rstrip('/')}/telegram/webhook/{WEBHOOK_SECRET}"
	payload = {
		"url": webhook_url,
	}
	if WEBHOOK_SECRET:
		payload["secret_token"] = WEBHOOK_SECRET

	async with httpx.AsyncClient(timeout=15.0) as client:
		response = await client.post(_build_telegram_api_url("setWebhook"), json=payload)

	if response.status_code >= 400:
		raise HTTPException(status_code=502, detail=f"Telegram setWebhook failed: {response.text}")

	data = response.json()
	return {
		"ok": data.get("ok", False),
		"webhook_url": webhook_url,
		"telegram": data,
	}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("telegram.tele_main:app", host="0.0.0.0", port=8000, reload=True)
