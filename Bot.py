import logging
import asyncio
import re
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from telethon.sync import TelegramClient
from telethon.tl.types import MessageEntityTextUrl

# API и настройки канала
API_TOKEN = '7504948210:AAG2GlEkeG26tWMcneLTXhfX1pV2yAe5Ye4'
TELEGRAM_API_ID = 20989031
TELEGRAM_API_HASH = 'bee10122874b3bbc0c1bd4edffdcae20'
CHANNEL_USERNAME = '@KyivOfficeRent'

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Инициализация бота и Telethon
bot = Bot(token=API_TOKEN)
dp = Dispatcher(bot)
telethon_client = TelegramClient('SkyMazafaker', TELEGRAM_API_ID, TELEGRAM_API_HASH)

# Функция для получения сообщений с использованием пользовательского аккаунта
async def fetch_channel_messages():
    async with telethon_client:
        try:
            channel = await telethon_client.get_entity(CHANNEL_USERNAME)
            history = []
            async for message in telethon_client.iter_messages(channel, limit=None):
                text = message.message or ""
                if text:
                    history.append((text, message.id, message.entities))
            logger.info(f"Загружено сообщений: {len(history)}")
            return history
        except Exception as e:
            logger.error(f"Ошибка при получении сообщений: {e}")
            return []

# Фильтрация сообщений по метражу и цене
async def parse_and_filter_messages(messages, min_size, max_size, min_price_per_m2=None, max_price_per_m2=None):
    offers = []
    for message, msg_id, entities in messages:
        if not message:
            continue

        logger.info(f"Сообщение: {message[:100]}")
        bc_match = re.search(r"Бізнес-(?:центр|парк) ([^\n]+)", message)
        bc_name = bc_match.group(1) if bc_match else "Неизвестный БЦ"

        offer_matches = re.findall(
            r"(\d+(?:-й|-й поверх| поверх|й поверх)) (\d+(?:\.\d+)?)m2 \((\d+(?:\.\d+)?\$)\)",
            message
        )
        logger.info(f"Результаты парсинга: {offer_matches}")

        # Извлекаем ссылки из форматированного текста
        offer_links = {}
        if entities:
            for entity in entities:
                if isinstance(entity, MessageEntityTextUrl):
                    start = entity.offset
                    end = start + entity.length
                    text = message[start:end]
                    offer_links[text.strip()] = entity.url

        for floor, size, price in offer_matches:
            size = float(size)
            price_total = float(price[:-1])
            price_per_m2 = round(price_total / size, 2)  # Уточнённое вычисление цены за м²

            if (min_size <= size if max_size is None else min_size <= size <= max_size):
                if (min_price_per_m2 is None or price_per_m2 >= min_price_per_m2) and (max_price_per_m2 is None or price_per_m2 <= max_price_per_m2):
                    # Генерация ключа для поиска ссылки
                    offer_key = f"{floor} {size}m2 ({price})"
                    link = None
                    for offer_text, offer_url in offer_links.items():
                        if offer_key in offer_text or floor in offer_text:
                            link = offer_url
                            break
                    if not link:
                        link = f"https://t.me/{CHANNEL_USERNAME[1:]}/{msg_id}"
                    offers.append((f"{bc_name}: {floor}, {size}m², {price} ({price_per_m2}$/m²)", link))
    logger.info(f"Найдено предложений: {len(offers)}")
    return offers

# Обработчик команды /start
@dp.message_handler(commands=['start'])
async def start_handler(message: types.Message):
    keyboard = types.InlineKeyboardMarkup(row_width=1)
    buttons = [
        types.InlineKeyboardButton("До 100 м²", callback_data="size_0_100"),
        types.InlineKeyboardButton("100-200 м²", callback_data="size_100_200"),
        types.InlineKeyboardButton("200-500 м²", callback_data="size_200_500"),
        types.InlineKeyboardButton("500-1000 м²", callback_data="size_500_1000"),
        types.InlineKeyboardButton("1000+ м²", callback_data="size_1000_")
    ]
    keyboard.add(*buttons)

    # Добавляем кнопку "Новый поиск"
    restart_button = types.ReplyKeyboardMarkup(resize_keyboard=True)
    restart_button.add(types.KeyboardButton("Новый поиск"))

    await message.answer("Выберите метраж:", reply_markup=keyboard)
    await message.answer("Для нового поиска нажмите кнопку ниже:", reply_markup=restart_button)

# Обработчик текстовой кнопки "Новый поиск"
@dp.message_handler(lambda message: message.text == "Новый поиск")
async def restart_handler(message: types.Message):
    await start_handler(message)

# Обработчик выбора метража
@dp.callback_query_handler(lambda c: c.data.startswith("size_"))
async def size_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    keyboard.add(
        types.InlineKeyboardButton("Низкий (до 20$ за м²)", callback_data=f"price_{min_size}_{max_size}_0_20"),
        types.InlineKeyboardButton("Средний (20$ - 30$ за м²)", callback_data=f"price_{min_size}_{max_size}_20_30"),
        types.InlineKeyboardButton("Высокий (более 30$ за м²)", callback_data=f"price_{min_size}_{max_size}_30_100000")
    )
    await callback_query.message.answer("Выберите ценовой диапазон:", reply_markup=keyboard)

# Обработчик выбора цены
@dp.callback_query_handler(lambda c: c.data.startswith("price_"))
async def price_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size, min_price, max_price = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    min_price = int(min_price)
    max_price = int(max_price) if max_price.isdigit() else None

    await callback_query.message.answer("Ищем подходящие варианты...")

    messages = await fetch_channel_messages()
    offers = await parse_and_filter_messages(messages, min_size, max_size, min_price, max_price)

    if not offers:
        await callback_query.message.answer("К сожалению, подходящих предложений нет.")
        return

    keyboard = types.InlineKeyboardMarkup(row_width=1)
    for offer_text, link in offers:
        if link:
            keyboard.add(types.InlineKeyboardButton(offer_text, url=link))

    await callback_query.message.answer("Доступные варианты:", reply_markup=keyboard)

# Запуск бота
if __name__ == "__main__":
    telethon_client.start()
    executor.start_polling(dp, skip_updates=True)
