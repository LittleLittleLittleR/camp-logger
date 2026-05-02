from pydantic import BaseModel


class TelegramChat(BaseModel):
	id: int


class TelegramMessage(BaseModel):
	message_id: int | None = None
	text: str | None = None
	chat: TelegramChat


class TelegramCallbackQuery(BaseModel):
	id: str
	data: str | None = None
	message: TelegramMessage | None = None


class TelegramUpdate(BaseModel):
	update_id: int
	message: TelegramMessage | None = None
	callback_query: TelegramCallbackQuery | None = None
