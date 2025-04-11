import logging
import asyncio
import re
import os
from aiogram import Bot, Dispatcher, types
from telethon.sync import TelegramClient
from telethon.tl.types import MessageEntityTextUrl
from aiogram.filters import Command
from aiogram import F
from aiogram import Router
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton
from aiogram.client.default import DefaultBotProperties

API_TOKEN = os.environ.get('API_TOKEN')
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')
CHANNEL_USERNAME = '@KyivOfficeRent'

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router(name=__name__)
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
dp.include_router(router)
telethon_client = TelegramClient('user_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)

async def fetch_channel_messages():
    async with telethon_client:
        try:
            channel = await telethon_client.get_entity(CHANNEL_USERNAME)
            history = []
            async for message in telethon_client.iter_messages(channel, limit=None):
                text = message.message or ""
                if text:
                    history.append((text, message.id, message.entities))
            logger.info(f"Messages loaded: {len(history)}")
            return history
        except Exception as e:
            logger.error(f"An error occurred while receiving messages: {e}")
            return []

def extract_metro_station(text):
    match = re.search(r"\u24C2️([\w\s\-]+)", text)
    return match.group(1).strip() if match else ""

async def parse_and_filter_messages(messages, min_size, max_size, min_price_per_m2=None, max_price_per_m2=None):
    offers = []
    for message, msg_id, entities in messages:
        if not message:
            continue

        bc_match = re.search(r"Бізнес-(?:центр|парк) ([^\n]+)", message)
        bc_name = bc_match.group(1) if bc_match else "Неизвестний БЦ"
        metro_station = extract_metro_station(message)

        offer_matches = re.findall(
            r"(\d+(?:-й|-й поверх| поверх|й поверх)) (\d+(?:\.\d+)?)m2 \((\d+(?:\.\d+)?\$)\)",
            message
        )

        offer_links = {}
        if entities:
            for entity in entities:
                if isinstance(entity, MessageEntityTextUrl):
                    start = entity.offset
                    end = start + entity.length
                    text = message[start:end]
                    offer_links[text.strip()] = entity.url

        for floor, size, price in offer_matches:
            size_number = float(size)
            price_total = float(price[:-1])
            price_per_m2 = round(price_total / size_number, 2)

            if (min_size <= size_number if max_size is None else min_size <= size_number <= max_size):
                if (min_price_per_m2 is None or price_per_m2 >= min_price_per_m2) and (max_price_per_m2 is None or price_per_m2 <= max_price_per_m2):
                    offer_key = f"{floor} {size}m2 ({price})"
                    link = None
                    for offer_text, offer_url in offer_links.items():
                        size_text = f"{size}m2"
                        if size_text in offer_text and price in offer_text and floor in offer_text:
                            link = offer_url
                            break
                    if not link:
                        link = f"https://t.me/{CHANNEL_USERNAME[1:]}/{msg_id}"
                    text = f"<b>{bc_name}</b>\n{floor}, {size}м²\n💵 {price} ({price_per_m2}$/м²)\nⓂ️{metro_station}"
                    offers.append((price_total, text, link))

    offers.sort(key=lambda x: x[0])
    return [(text, link) for _, text, link in offers]

@router.message(Command("start"))
async def start_handler(message: types.Message):
    buttons = [
        [InlineKeyboardButton(text="До 100 м²", callback_data="size_0_100")],
        [InlineKeyboardButton(text="100-200 м²", callback_data="size_100_200")],
        [InlineKeyboardButton(text="200-500 м²", callback_data="size_200_500")],
        [InlineKeyboardButton(text="500-1000 м²", callback_data="size_500_1000")],
        [InlineKeyboardButton(text="1000+ м²", callback_data="size_1000_")]
    ]
    restart_buttons = [
        [KeyboardButton(text="Новий пошук")]
    ]
    await message.answer("Оберіть метраж:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
    await message.answer("Для нового пошуку натисніть кнопку нижче:", reply_markup=ReplyKeyboardMarkup(keyboard=restart_buttons, resize_keyboard=True))

@router.message(F.text == "Новий пошук")
async def restart_handler(message: types.Message):
    await start_handler(message)

@router.callback_query(F.data.startswith("size_"))
async def size_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    buttons = [
        [InlineKeyboardButton(text="Низький (до 20$ за м²)", callback_data=f"price_{min_size}_{max_size}_0_20")],
        [InlineKeyboardButton(text="Середній (20$ - 30$ за м²)", callback_data=f"price_{min_size}_{max_size}_20_30")],
        [InlineKeyboardButton(text="Високий (більше 30$ за м²)", callback_data=f"price_{min_size}_{max_size}_30_100000")]
    ]
    await callback_query.message.answer("Оберіть діапазон ціни:", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("price_"))
async def price_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size, min_price, max_price = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    min_price = int(min_price)
    max_price = int(max_price) if max_price.isdigit() else None

    await callback_query.message.answer("Шукаємо відповідні варіанти...")

    messages = await fetch_channel_messages()
    offers = await parse_and_filter_messages(messages, min_size, max_size, min_price, max_price)

    if not offers:
        await callback_query.message.answer("На жаль, відповідних варіантів не знайдено.")
        return

    for offer_text, link in offers:
        if link:
            markup = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="Перейти ➡️", url=link)]]
            )
            await callback_query.message.answer(offer_text, reply_markup=markup)

async def start_telethon():
    await telethon_client.start()
    print(f"Telegram Client {telethon_client.session} is connected!")

async def main():
    await start_telethon()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
