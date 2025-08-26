import logging
import asyncio
import re
import os
from io import BytesIO
from typing import List, Tuple, Optional, Dict, Any

from PIL import Image

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import (
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BufferedInputFile,
)
from aiogram.client.default import DefaultBotProperties
from aiogram import Router

from telethon import TelegramClient
from telethon.tl.types import MessageEntityTextUrl, PhotoSize

# ===================== CONFIG =====================
API_TOKEN = os.environ.get('API_TOKEN')
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')
CHANNEL_USERNAME = '@KyivOfficeRent'

PAGE_SIZE = 5
MAX_PARALLEL_DOWNLOADS = 6
COLLAGE_W, COLLAGE_H = 1280, 720  # быстрый и "телеграммный" размер
JPEG_QUALITY = 65                 # ниже качество -> быстрее
# ==================================================

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Aiogram 3.x
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Один Telethon-клиент на всё время жизни бота
telethon_client = TelegramClient('user_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
# Ограничитель параллельных скачиваний (ускоряем, но не рвём соединение)
telethon_semaphore = asyncio.Semaphore(MAX_PARALLEL_DOWNLOADS)

# Сессии пользователей + кэш коллажей
user_sessions: Dict[int, Dict[str, Any]] = {}
collage_cache: Dict[int, bytes] = {}

# --------------------- УТИЛИТЫ ---------------------
async def ensure_connected():
    if not telethon_client.is_connected():
        try:
            await telethon_client.connect()
        except Exception:
            await telethon_client.start()

async def fetch_channel_messages(limit: Optional[int] = None) -> List[Tuple[str, int, Optional[List[Any]]]]:
    """Забираем текстовые сообщения канала. limit=None — весь канал (медленнее)."""
    try:
        await ensure_connected()
        channel = await telethon_client.get_entity(CHANNEL_USERNAME)
        history = []
        async for message in telethon_client.iter_messages(channel, limit=limit):
            text = message.message or ""
            if text:
                history.append((text, message.id, message.entities))
        logger.info(f"Fetched {len(history)} messages from channel")
        return history
    except Exception as e:
        logger.exception(f"Error fetching messages: {e}")
        return []

def extract_metro_station(text: str) -> str:
    m = re.search(r"\u24C2️([\w\s\-]+)", text)
    return m.group(1).strip() if m else ""

async def parse_and_filter_messages(
    messages,
    min_size: int,
    max_size: Optional[int],
    min_price_per_m2: Optional[int] = None,
    max_price_per_m2: Optional[int] = None,
):
    offers = []
    for message, msg_id, entities in messages:
        if not message:
            continue
        # Название БЦ
        bc_match = re.search(r"Бізнес-(?:центр|парк) ([^\n]+)", message)
        bc_name = bc_match.group(1) if bc_match else "БЦ"
        metro_station = extract_metro_station(message)

        # Находим офферы вида: "7-й поверх 200m2 (4000$)"
        offer_matches = re.findall(
            r"(\d+(?:-й|-й поверх| поверх|й поверх))\s+(\d+(?:\.\d+)?)m2\s*\((\d+(?:\.\d+)?\$)\)",
            message,
        )
        # Собираем ссылки из entities
        offer_links = {}
        if entities:
            for ent in entities:
                if isinstance(ent, MessageEntityTextUrl):
                    start = ent.offset
                    end = start + ent.length
                    txt = message[start:end]
                    offer_links[txt.strip()] = ent.url

        for floor, size, price in offer_matches:
            size_number = float(size)
            price_total = float(price[:-1])
            price_per_m2 = round(price_total / size_number, 2)
            # Фильтрация по метражу
            if (min_size <= size_number if max_size is None else min_size <= size_number <= max_size):
                # Фильтрация по $/м²
                if (min_price_per_m2 is None or price_per_m2 >= min_price_per_m2) and (
                    max_price_per_m2 is None or price_per_m2 <= max_price_per_m2
                ):
                    link = None
                    for t, url in offer_links.items():
                        if f"{size}m2" in t and price in t and floor in t:
                            link = url
                            break
                    if not link:
                        link = f"https://t.me/{CHANNEL_USERNAME[1:]}/{msg_id}"
                    text_out = (
                        f"<b>{bc_name}</b>\n{floor}, {size}м²\n"
                        f"💵 {price} ({price_per_m2}$/м²)\nⓂ️{metro_station}"
                    )
                    offers.append((price_total, text_out, link, msg_id))
    offers.sort(key=lambda x: x[0])
    # Вернём упрощённые записи
    return [(t, l, mid) for _, t, l, mid in offers]

# ---------- ЗАГРУЗКА МАЛЕНЬКИХ ФОТО ДЛЯ СКОРОСТИ ----------
async def _download_small_photo_bytes(msg) -> Optional[bytes]:
    """Пытаемся скачать уменьшенную версию фото (thumbnail)."""
    if not getattr(msg, 'photo', None):
        return None
    # Выбираем самый маленький адекватный размер (около 640px), чтобы скачать быстро
    sizes = getattr(msg.photo, 'sizes', []) or []
    size_choice: Optional[PhotoSize] = None
    if sizes:
        # сортируем по ширине/высоте
        try:
            sizes_sorted = sorted(
                [s for s in sizes if hasattr(s, 'w') and hasattr(s, 'h')], key=lambda s: (s.w * s.h)
            )
            # ищем до ~640px
            for s in sizes_sorted:
                if max(getattr(s, 'w', 0), getattr(s, 'h', 0)) <= 640:
                    size_choice = s
                    break
            if size_choice is None:
                size_choice = sizes_sorted[0]
        except Exception:
            size_choice = sizes[0]
    try:
        # thumb может принимать PhotoSize; если не сработает — fallback без thumb
        if size_choice is not None:
            data = await telethon_client.download_media(msg, file=bytes, thumb=size_choice)
        else:
            data = await telethon_client.download_media(msg, file=bytes)
        if data and isinstance(data, (bytes, bytearray)):
            return bytes(data)
    except Exception as e:
        logger.warning(f"Thumb download failed, fallback to full. Reason: {e}")
        try:
            data = await telethon_client.download_media(msg, file=bytes)
            if data and isinstance(data, (bytes, bytearray)):
                return bytes(data)
        except Exception as e2:
            logger.error(f"Full download failed: {e2}")
    return None

async def fetch_first_3_small_photos(msg_id: int) -> List[bytes]:
    await ensure_connected()
    try:
        channel = await telethon_client.get_entity(CHANNEL_USERNAME)
        message = await telethon_client.get_messages(channel, ids=msg_id)
        if not message:
            return []
        grouped_id = getattr(message, 'grouped_id', None)
        msgs = []
        if grouped_id:
            ids_window = list(range(max(1, msg_id - 20), msg_id + 21))
            all_msgs = await telethon_client.get_messages(channel, ids=ids_window)
            msgs = [m for m in all_msgs if getattr(m, 'grouped_id', None) == grouped_id]
            msgs.sort(key=lambda x: x.id)
        else:
            msgs = [message]

        photos: List[bytes] = []
        for m in msgs:
            if getattr(m, 'photo', None) is not None:
                async with telethon_semaphore:
                    b = await _download_small_photo_bytes(m)
                if b:
                    photos.append(b)
                if len(photos) >= 3:  # берём 3 для красивого коллажа
                    break
        return photos
    except Exception as e:
        logger.exception(f"Error fetching photos for msg {msg_id}: {e}")
        return []

# -------------------- КОЛЛАЖ 1+2 --------------------

def _resize_cover(img: Image.Image, tw: int, th: int) -> Image.Image:
    w, h = img.size
    if w == 0 or h == 0:
        return img
    scale = max(tw / w, th / h)
    nw, nh = int(w * scale), int(h * scale)
    img2 = img.resize((nw, nh), Image.LANCZOS)
    # центрируем и обрезаем
    left = (nw - tw) // 2
    top = (nh - th) // 2
    return img2.crop((left, top, left + tw, top + th))

def make_collage_1_plus_2(images_bytes: List[bytes]) -> Optional[bytes]:
    if len(images_bytes) < 3:
        return None
    try:
        final_w, final_h = COLLAGE_W, COLLAGE_H
        left_w = final_w // 2
        right_w = final_w - left_w
        half_h = final_h // 2

        img1 = Image.open(BytesIO(images_bytes[0])).convert('RGB')
        img2 = Image.open(BytesIO(images_bytes[1])).convert('RGB')
        img3 = Image.open(BytesIO(images_bytes[2])).convert('RGB')

        img1 = _resize_cover(img1, left_w, final_h)
        img2 = _resize_cover(img2, right_w, half_h)
        img3 = _resize_cover(img3, right_w, half_h)

        collage = Image.new('RGB', (final_w, final_h))
        collage.paste(img1, (0, 0))
        collage.paste(img2, (left_w, 0))
        collage.paste(img3, (left_w, half_h))

        out = BytesIO()
        collage.save(out, format='JPEG', quality=JPEG_QUALITY, optimize=True)
        out.seek(0)
        return out.read()
    except Exception as e:
        logger.exception(f"Error creating collage: {e}")
        return None

# -------------------- КЛАВИАТУРЫ --------------------

def build_size_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="До 200 м²", callback_data="size_0_200")],
        [InlineKeyboardButton(text="200-500 м²", callback_data="size_200_500")],
        [InlineKeyboardButton(text="500-1000 м²", callback_data="size_500_1000")],
        [InlineKeyboardButton(text="1000+ м²", callback_data="size_1000_")],
    ])

def build_price_keyboard(min_size, max_size):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Низький (до 20$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_0_20")],
        [InlineKeyboardButton(text="Середній (20$ - 30$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_20_30")],
        [InlineKeyboardButton(text="Високий (більше 30$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_30_100000")],
    ])

def build_pagination_keyboard(page, total):
    max_page = (total - 1) // PAGE_SIZE if total else 0
    buttons = []
    row = []
    if page > 0:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="page_prev"))
    if page < max_page:
        row.append(InlineKeyboardButton(text="Далі ➡️", callback_data="page_next"))
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)

# -------------------- ОТПРАВКА СТРАНИЦЫ --------------------
async def send_page(chat_id, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return
    results = session['results']
    page = session['page']
    start = page * PAGE_SIZE
    end = min(len(results), start + PAGE_SIZE)

    nav_kb = build_pagination_keyboard(page, len(results))

    async def ensure_collage(offer):
        mid = offer['msg_id']
        if mid not in collage_cache:
            photos = await fetch_first_3_small_photos(mid)
            collage = make_collage_1_plus_2(photos)
            if collage:
                collage_cache[mid] = collage
        return offer

    # Параллельная подготовка коллажей для страницы
    await asyncio.gather(*[ensure_collage(results[i]) for i in range(start, end)])

    # Отправка карточек
    for i in range(start, end):
        offer = results[i]
        markup = InlineKeyboardMarkup(
            inline_keyboard=[[InlineKeyboardButton(text="Перейти ➡️", url=offer['link'])]]
        )
        mid = offer['msg_id']
        if mid in collage_cache:
            await bot.send_photo(
                chat_id,
                photo=BufferedInputFile(collage_cache[mid], filename='collage.jpg'),
                caption=offer['text'],
                reply_markup=markup,
            )
        else:
            await bot.send_message(chat_id, offer['text'], reply_markup=markup)

    await bot.send_message(
        chat_id,
        f"Сторінка {page + 1} із {(len(results) - 1) // PAGE_SIZE + 1}",
        reply_markup=nav_kb,
    )

# ---------------------- ХЕНДЛЕРЫ ----------------------
@router.message(Command("start"))
async def start_handler(message: types.Message):
    await message.answer("Оберіть метраж:", reply_markup=build_size_keyboard())
    await message.answer(
        "Для нового пошуку натисніть кнопку нижче:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Новий пошук")]], resize_keyboard=True
        ),
    )

@router.message(F.text == "Новий пошук")
async def restart_handler(message: types.Message):
    await start_handler(message)

@router.callback_query(F.data.startswith("size_"))
async def size_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    await callback_query.message.answer(
        "Оберіть діапазон ціни:", reply_markup=build_price_keyboard(min_size, max_size)
    )

@router.callback_query(F.data.startswith("price_"))
async def price_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size, min_price, max_price = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    min_price = int(min_price)
    max_price = int(max_price) if max_price.isdigit() else None

    await callback_query.message.answer("Шукаємо відповідні варіанти...")

    # Чтобы ускорить первый ответ, можно ограничить историю, например, 1500 сообщений
    messages = await fetch_channel_messages(limit=None)
    parsed = await parse_and_filter_messages(messages, min_size, max_size, min_price, max_price)

    if not parsed:
        await callback_query.message.answer("На жаль, відповідних варіантів не знайдено.")
        return

    user_sessions[callback_query.from_user.id] = {
        'results': [{'text': t, 'link': l, 'msg_id': m} for t, l, m in parsed],
        'page': 0,
    }
    await send_page(callback_query.message.chat.id, callback_query.from_user.id)

@router.callback_query(F.data == "page_next")
async def page_next_handler(callback_query: types.CallbackQuery):
    session = user_sessions.get(callback_query.from_user.id)
    if session and session['page'] < (len(session['results']) - 1) // PAGE_SIZE:
        session['page'] += 1
    await send_page(callback_query.message.chat.id, callback_query.from_user.id)

@router.callback_query(F.data == "page_prev")
async def page_prev_handler(callback_query: types.CallbackQuery):
    session = user_sessions.get(callback_query.from_user.id)
    if session and session['page'] > 0:
        session['page'] -= 1
    await send_page(callback_query.message.chat.id, callback_query.from_user.id)

# ----------------------- ЗАПУСК -----------------------
async def main():
    await telethon_client.start()
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
