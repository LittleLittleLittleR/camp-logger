import os
import logging
from typing import Any, Optional

import httpx
import io
try:
	from PIL import Image, ImageDraw, ImageFont
	PIL_AVAILABLE = True
except Exception:
	PIL_AVAILABLE = False
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

	reply = _handle_text_command(message.text)

	# If handler returned a photo payload, send the photo; otherwise send as text.
	if isinstance(reply, dict) and reply.get("photo"):
		image_bytes = reply.get("photo")
		caption = reply.get("caption")
		await _send_telegram_photo(chat_id=message.chat.id, image_bytes=image_bytes, caption=caption)
	else:
		# Ensure it's string
		text_to_send = str(reply) if reply is not None else ""
		await _send_telegram_message(chat_id=message.chat.id, text=text_to_send)
	return {"ok": True, "handled": True}


def _help_text() -> str:
	return (
		"Available commands:\n"
		"/start - basic intro\n"
		"/help - show commands\n"
		"/tables - list SQLite tables\n"
		"/table <name> - show rows"
	)


def _table_preview(table_name: str, limit: int = 5) -> str:
	if table_name not in list_tables():
		return f"Table '{table_name}' does not exist."

	columns, rows = read_table(table_name)
	if not rows:
		return f"Table '{table_name}' exists but has no rows."

	# By default show a short text preview; full data can be fetched via the API.
	shown_rows = rows[:limit]
	lines: list[str] = [f"Preview of {table_name} (max {limit} rows):"]
	lines.append(", ".join(columns))

	for row in shown_rows:
		lines.append(", ".join(str(value) for value in row))

	return "\n".join(lines)


def _render_table_image(columns: list[str], rows: list[tuple]) -> bytes:
	if not PIL_AVAILABLE:
		raise RuntimeError("Pillow library is not available. Install Pillow to enable image rendering.")

	# Render a simple table image using Pillow.
	font = ImageFont.load_default()
	tmp_img = Image.new("RGB", (1, 1), "white")
	tmp_draw = ImageDraw.Draw(tmp_img)

	def _measure_text(text: str) -> tuple[int, int]:
		# Pillow 10+ removed getsize; textbbox/getbbox are the stable options.
		try:
			left, top, right, bottom = tmp_draw.textbbox((0, 0), text, font=font)
			return max(1, right - left), max(1, bottom - top)
		except Exception:
			left, top, right, bottom = font.getbbox(text)
			return max(1, right - left), max(1, bottom - top)

	padding = 8
	_, base_text_height = _measure_text("Ay")
	line_height = base_text_height + 6

	# Compute column widths
	col_widths = [_measure_text(str(col))[0] for col in columns]
	for row in rows:
		for i, cell in enumerate(row):
			text = "" if cell is None else str(cell)
			w, _ = _measure_text(text)
			if w > col_widths[i]:
				col_widths[i] = w

	# Add padding to widths
	col_widths = [w + padding * 2 for w in col_widths]

	table_width = sum(col_widths)
	table_height = line_height * (len(rows) + 1) + padding * 2

	img = Image.new("RGB", (table_width, max(table_height, 32)), "white")
	draw = ImageDraw.Draw(img)

	# Draw header
	x = 0
	y = padding
	for i, col in enumerate(columns):
		draw.rectangle([x, y, x + col_widths[i], y + line_height], fill=(230, 230, 230))
		draw.text((x + padding, y + 2), str(col), fill="black", font=font)
		x += col_widths[i]

	# Draw rows
	y += line_height
	for row in rows:
		x = 0
		for i, cell in enumerate(row):
			text = "" if cell is None else str(cell)
			draw.text((x + padding, y + 2), text, fill="black", font=font)
			x += col_widths[i]
		y += line_height

	# Draw vertical grid lines
	x = 0
	for w in col_widths:
		draw.line([(x, padding), (x, y)], fill=(200, 200, 200))
		x += w
	draw.line([(x - 1, padding), (x - 1, y)], fill=(200, 200, 200))

	# Save to bytes
	buf = io.BytesIO()
	img.save(buf, format="PNG")
	buf.seek(0)
	return buf.read()


async def _send_telegram_photo(chat_id: int, image_bytes: bytes, filename: str = "table.png", caption: str | None = None) -> dict[str, Any]:
	url = _build_telegram_api_url("sendPhoto")

	data = {"chat_id": str(chat_id)}
	if caption:
		data["caption"] = caption

	files = {"photo": (filename, image_bytes, "image/png")}

	async with httpx.AsyncClient(timeout=TELEGRAM_TIMEOUT_SECONDS) as client:
		response = await client.post(url, data=data, files=files)

	if response.status_code >= 400:
		raise HTTPException(status_code=502, detail=f"Telegram sendPhoto failed: {response.text}")

	data = response.json()
	if not data.get("ok", False):
		raise HTTPException(status_code=502, detail=f"Telegram sendPhoto not ok: {data}")

	return data


def _handle_text_command(text: str) -> Any:
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
		table_name = parts[1].strip()
		if table_name not in list_tables():
			return f"Table '{table_name}' does not exist."
		columns, rows = read_table(table_name)
		if not rows:
			return f"Table '{table_name}' exists but has no rows."

		# Render image and instruct caller to send a photo to Telegram.
		# Cap rows for image rendering to avoid extremely large images.
		max_rows_for_image = 500
		rows_for_image = rows[:max_rows_for_image]
		try:
			img_bytes = _render_table_image(columns, rows_for_image)
		except Exception as exc:
			# Fall back to text reply if image rendering is not available
			logger.exception("Image rendering failed for table %s: %s", table_name, exc)
			return f"{table_name} ({len(rows)} rows) — unable to render image: {exc}"

		caption = f"{table_name} ({len(rows)} rows)"
		return {"photo": img_bytes, "caption": caption}

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
def get_table_data(table_name: str, limit: Optional[int] = Query(default=None)) -> dict[str, Any]:
	try:
		if table_name not in list_tables():
			raise HTTPException(status_code=404, detail=f"Table '{table_name}' not found")

		columns, rows = read_table(table_name)
		if limit is None:
			selected_rows = rows
		else:
			if limit < 1:
				raise HTTPException(status_code=400, detail="limit must be >= 1")
			selected_rows = rows[:limit]

		records = [dict(zip(columns, row)) for row in selected_rows]
		return {
			"table": table_name,
			"count": len(records),
			"total_rows": len(rows),
			"records": records,
		}
	except HTTPException:
		raise
	except Exception as exc:
		logger.exception("Failed to read table %s", table_name)
		raise HTTPException(status_code=500, detail=str(exc)) from exc


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
