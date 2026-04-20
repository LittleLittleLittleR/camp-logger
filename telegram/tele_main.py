import os
import logging
from typing import Any

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException, Query

from database.SQLite.execute import DB_PATH, list_tables, read_table
from .model import TelegramUpdate

load_dotenv()
logger = logging.getLogger("telegram.webhook")
TELEGRAM_TIMEOUT_SECONDS = 15.0


BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "")
DEFAULT_CHAT_ID = os.getenv("TELEGRAM_DEFAULT_CHAT_ID", "")
PUBLIC_BASE_URL = os.getenv("TELEGRAM_PUBLIC_BASE_URL", "")
VERCEL_PROJECT_PRODUCTION_URL = os.getenv("VERCEL_PROJECT_PRODUCTION_URL", "")
VERCEL_URL = os.getenv("VERCEL_URL", "")
AUTO_SET_WEBHOOK = os.getenv("TELEGRAM_AUTO_SET_WEBHOOK", "true").strip().lower() in {
	"1",
	"true",
	"yes",
	"on",
}


app = FastAPI(
	title="Camp Logger Telegram Backend",
	description="Webhook backend and utility API for Telegram bot integration.",
	version="0.1.0",
)


@app.on_event("startup")
async def startup_webhook_sync() -> None:
	if not AUTO_SET_WEBHOOK:
		logger.info("Webhook auto-sync disabled by TELEGRAM_AUTO_SET_WEBHOOK")
		return

	await _sync_webhook_if_needed()


def _build_telegram_api_url(method: str) -> str:
	if not BOT_TOKEN:
		raise HTTPException(status_code=500, detail="TELEGRAM_BOT_TOKEN is not configured")

	return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


async def _call_telegram_api(
	method: str,
	*,
	payload: dict[str, Any] | None = None,
	http_method: str = "post",
) -> dict[str, Any]:
	url = _build_telegram_api_url(method)

	async with httpx.AsyncClient(timeout=TELEGRAM_TIMEOUT_SECONDS) as client:
		if http_method == "get":
			response = await client.get(url)
		else:
			response = await client.post(url, json=payload)

	if response.status_code >= 400:
		raise HTTPException(status_code=502, detail=f"Telegram {method} failed: {response.text}")

	data = response.json()
	if not data.get("ok", False):
		raise HTTPException(status_code=502, detail=f"Telegram {method} not ok: {data}")

	return data


async def _send_telegram_message(chat_id: int, text: str) -> dict[str, Any]:
	payload = {
		"chat_id": chat_id,
		"text": text,
	}
	return await _call_telegram_api("sendMessage", payload=payload)


def _build_webhook_url(base_url: str) -> str:
	selected = base_url.strip()
	if not selected:
		raise HTTPException(status_code=400, detail="public_base_url is required")
	if not selected.startswith("https://"):
		raise HTTPException(status_code=400, detail="public_base_url must start with https://")

	base = selected.rstrip("/")
	if WEBHOOK_SECRET:
		return f"{base}/telegram/webhook/{WEBHOOK_SECRET}"
	return f"{base}/telegram/webhook"


def _discover_public_base_url() -> str | None:
	# Priority: explicit override, then stable Vercel production URL, then deployment URL.
	if PUBLIC_BASE_URL.strip():
		return PUBLIC_BASE_URL.strip()

	if VERCEL_PROJECT_PRODUCTION_URL.strip():
		return f"https://{VERCEL_PROJECT_PRODUCTION_URL.strip()}"

	if VERCEL_URL.strip():
		return f"https://{VERCEL_URL.strip()}"

	return None


def _resolve_public_base_url(public_base_url: str | None = None) -> str:
	if public_base_url and public_base_url.strip():
		return public_base_url.strip()

	discovered = _discover_public_base_url()
	if discovered:
		return discovered

	raise HTTPException(
		status_code=400,
		detail=(
			"public_base_url is required. Set TELEGRAM_PUBLIC_BASE_URL or pass public_base_url explicitly"
		),
	)


async def _fetch_webhook_info() -> dict[str, Any]:
	return await _call_telegram_api("getWebhookInfo", http_method="get")


async def _sync_webhook_if_needed() -> None:
	if not BOT_TOKEN:
		logger.warning("Webhook auto-sync skipped: TELEGRAM_BOT_TOKEN is not configured")
		return

	base_url = _discover_public_base_url()
	if not base_url:
		logger.warning(
			"Webhook auto-sync skipped: no public base URL. Set TELEGRAM_PUBLIC_BASE_URL for deterministic behavior"
		)
		return

	target_webhook_url = _build_webhook_url(base_url)

	try:
		info = await _fetch_webhook_info()
		current_webhook_url = info.get("result", {}).get("url", "")
	except Exception:
		logger.exception("Webhook auto-sync failed while fetching webhook info")
		return

	if current_webhook_url == target_webhook_url:
		logger.info("Webhook already up-to-date: %s", target_webhook_url)
		return

	payload: dict[str, Any] = {"url": target_webhook_url}
	if WEBHOOK_SECRET:
		payload["secret_token"] = WEBHOOK_SECRET

	try:
		data = await _call_telegram_api("setWebhook", payload=payload)
		if data.get("ok", False):
			logger.info("Webhook auto-sync completed: %s", target_webhook_url)
		else:
			logger.error("Webhook auto-sync not ok: %s", data)
	except HTTPException as exc:
		logger.error("Webhook auto-sync setWebhook failed: %s", exc.detail)
	except Exception:
		logger.exception("Webhook auto-sync failed while setting webhook")


async def _process_telegram_update(
	update: TelegramUpdate,
	x_telegram_bot_api_secret_token: str | None,
	secret_in_path: str | None,
) -> dict[str, Any]:
	if WEBHOOK_SECRET and secret_in_path is not None and secret_in_path != WEBHOOK_SECRET:
		raise HTTPException(status_code=403, detail="Invalid webhook path secret")

	if WEBHOOK_SECRET and x_telegram_bot_api_secret_token != WEBHOOK_SECRET:
		raise HTTPException(status_code=403, detail="Invalid Telegram secret header")

	message = update.message
	if not message or not message.text:
		logger.info("Webhook update ignored: update_id=%s reason=no-text", update.update_id)
		return {"ok": True, "handled": False, "reason": "No text message"}

	logger.info(
		"Webhook update received: update_id=%s chat_id=%s text=%s",
		update.update_id,
		message.chat.id,
		message.text,
	)

	reply_text = _handle_text_command(message.text)
	await _send_telegram_message(chat_id=message.chat.id, text=reply_text)
	return {"ok": True, "handled": True}


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
	return await _process_telegram_update(update, x_telegram_bot_api_secret_token, secret)


@app.post("/telegram/webhook")
async def telegram_webhook_no_secret(
	update: TelegramUpdate,
	x_telegram_bot_api_secret_token: str | None = Header(default=None),
) -> dict[str, Any]:
	return await _process_telegram_update(update, x_telegram_bot_api_secret_token, None)


@app.post("/telegram/send-test")
async def send_test_message(chat_id: int | None = None) -> dict[str, Any]:
	target_chat_id = chat_id
	if target_chat_id is None:
		if not DEFAULT_CHAT_ID:
			raise HTTPException(
				status_code=400,
				detail="chat_id is required when TELEGRAM_DEFAULT_CHAT_ID is not configured",
			)
		try:
			target_chat_id = int(DEFAULT_CHAT_ID)
		except ValueError as exc:
			raise HTTPException(status_code=500, detail="TELEGRAM_DEFAULT_CHAT_ID must be an integer") from exc

	result = await _send_telegram_message(target_chat_id, "Test message from FastAPI backend.")
	return {"ok": True, "telegram": result}


@app.post("/telegram/set-webhook")
async def set_webhook(public_base_url: str | None = None) -> dict[str, Any]:
	base_url = _resolve_public_base_url(public_base_url)
	webhook_url = _build_webhook_url(base_url)
	payload = {
		"url": webhook_url,
	}
	if WEBHOOK_SECRET:
		payload["secret_token"] = WEBHOOK_SECRET
	data = await _call_telegram_api("setWebhook", payload=payload)
	return {
		"ok": data.get("ok", False),
		"webhook_url": webhook_url,
		"telegram": data,
	}


@app.get("/telegram/webhook-info")
async def get_webhook_info(public_base_url: str | None = Query(default=None)) -> dict[str, Any]:
	data = await _fetch_webhook_info()
	result = data.get("result", {})
	try:
		expected_url = _build_webhook_url(_resolve_public_base_url(public_base_url))
	except HTTPException:
		expected_url = None
	return {
		"ok": True,
		"expected_webhook_url": expected_url,
		"current_webhook_url": result.get("url"),
		"pending_update_count": result.get("pending_update_count"),
		"last_error_date": result.get("last_error_date"),
		"last_error_message": result.get("last_error_message"),
		"has_custom_certificate": result.get("has_custom_certificate"),
		"max_connections": result.get("max_connections"),
		"allowed_updates": result.get("allowed_updates"),
		"telegram": data,
	}


@app.post("/telegram/delete-webhook")
async def delete_webhook(drop_pending_updates: bool = False) -> dict[str, Any]:
	payload = {"drop_pending_updates": drop_pending_updates}
	data = await _call_telegram_api("deleteWebhook", payload=payload)

	return {
		"ok": True,
		"telegram": data,
	}


if __name__ == "__main__":
	import uvicorn

	uvicorn.run("telegram.tele_main:app", host="0.0.0.0", port=8000, reload=True)
