import logging
import asyncio
import re
import os
import json
import calendar
from io import BytesIO
from typing import List, Tuple, Optional, Dict, Any
from datetime import datetime
from PIL import Image
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import CommandStart
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram import Router
from telethon import TelegramClient
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

API_TOKEN = os.environ.get('API_TOKEN')
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')
CHANNEL_USERNAME = '@KyivOfficeRent'
HEROKU_API_KEY = os.environ.get('HEROKU_API_KEY')
APP_NAME = os.environ.get('HEROKU_APP_NAME')
PAGE_SIZE = 5
MAX_PARALLEL_DOWNLOADS = 6
COLLAGE_W, COLLAGE_H = 1280, 720
JPEG_QUALITY = 85
DRIVE_FOLDER_ID = '1JN8SMN-8b3xta1SVN-42nu7gT2BURqVn'
CACHE_FILE = os.path.join(os.path.dirname(__file__), 'collage_url_cache.json')
SCOPES = ['https://www.googleapis.com/auth/drive']
CLIENT_SECRET_CONTENT = os.environ.get('CLIENT_SECRET')  # Вміст JSON
CLIENT_SECRET_FILE = None

# Зчитуємо секретні змінні з конфігів
def get_secrets():
    return {
        'refresh_token': os.getenv('REFRESH_TOKEN'),
        'client_id': os.getenv('CLIENT_ID'),
        'client_secret': os.getenv('CLIENT_SECRET'),
        'token_uri': os.getenv('GOOGLE_TOKEN_URI'),
    }

# ==================================================
if CLIENT_SECRET_CONTENT:
    CLIENT_SECRET_FILE = os.path.join(os.path.dirname(__file__), 'client_secret.json')
    with open(CLIENT_SECRET_FILE, 'w', encoding='utf-8') as f:
        f.write(CLIENT_SECRET_CONTENT)
else:
    print("GOOGLE_CLIENT_SECRET_JSON is not set in environment variables")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Aiogram
bot = Bot(token=API_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Telethon
telethon_client = TelegramClient('user_session', TELEGRAM_API_ID, TELEGRAM_API_HASH)
telethon_semaphore = asyncio.Semaphore(MAX_PARALLEL_DOWNLOADS)

# State / caches
user_sessions: Dict[int, Dict[str, Any]] = {}
collage_bytes_cache: Dict[int, bytes] = {}
if os.path.exists(CACHE_FILE):
    try:
        with open(CACHE_FILE, 'r', encoding='utf-8') as f:
            collage_url_cache: Dict[str, str] = json.load(f)
    except Exception:
        collage_url_cache = {}
else:
    collage_url_cache = {}
calc_store: Dict[Tuple[int, int], Dict[str, Any]] = {}

# Функція оновлення secretів у Heroku
def update_heroku_config(new_access_token, new_refresh_token):
    url = f'https://api.heroku.com/apps/{APP_NAME}/config-vars'
    headers = {
        'Accept': 'application/vnd.heroku+json; version=3',
        'Authorization': f'Bearer {HEROKU_API_KEY}'
    }
    data = {
        'ACCESS_TOKEN': new_access_token,
        'REFRESH_TOKEN': new_refresh_token
    }
    response = requests.patch(url, headers=headers, json=data)
    if response.status_code == 200:
        print('Heroku config vars оновлено.')
    else:
        print('Помилка оновлення конфігів:', response.text)

# Функція автоматичного оновлення токена раз на день
async def auto_refresh_token():
    while True:
        try:
            logger.info("🔁 Перевірка токена Google Drive...")

            creds_data = get_secrets()
            creds = Credentials(
                None,
                refresh_token=creds_data['REFRESH_TOKEN'],
                token_uri=creds_data['GOOGLE_TOKEN_URI'],
                client_id=creds_data['CLIENT_ID'],
                client_secret=creds_data['CLIENT_SECRET'],
                scopes=SCOPES
            )

            if creds.expired and creds.refresh_token:
                creds.refresh(Request())
                os.environ['ACCESS_TOKEN'] = creds.token
                os.environ['REFRESH_TOKEN'] = creds.refresh_token
                update_heroku_config(creds.token, creds.refresh_token)
                logger.info("✅ Google токен оновлено та збережено")

        except Exception as e:
            logger.exception("❌ Помилка при автооновленні токена")

        # Спимо 24 години
        await asyncio.sleep(24 * 60 * 60)

# ----------------- Google Drive (OAuth) -----------------
def init_drive_service():
    creds_data = get_secrets()

    creds = Credentials(
        token=None,  # access_token не задаємо вручну
        refresh_token=creds_data['refresh_token'],
        token_uri=creds_data['token_uri'],
        client_id=creds_data['client_id'],
        client_secret=creds_data['client_secret'],
        scopes=SCOPES
    )

    # Спроба оновити access_token з refresh_token
    try:
        creds.refresh(Request())
    except Exception as e:
        logger.exception("❌ Помилка під час оновлення Google токена")
        raise e

    service = build('drive', 'v3', credentials=creds, cache_discovery=False)
    return service

def upload_collage_to_drive(collage_bytes: bytes, filename: str, folder_id: str) -> Optional[str]:
    try:
        service = init_drive_service()
        safe_name = filename.replace("'", "\\'")
        query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"
        resp = service.files().list(q=query, spaces='drive', fields='files(id,name)').execute()
        files = resp.get('files', [])
        if files:
            file_id = files[0]['id']
            return f"https://drive.google.com/uc?export=download&id={file_id}"

        fh = BytesIO(collage_bytes)
        media = MediaIoBaseUpload(fh, mimetype='image/jpeg', resumable=True)
        file_metadata = {'name': filename, 'parents': [folder_id]}
        created = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        file_id = created.get('id')
        service.permissions().create(fileId=file_id, body={'type': 'anyone', 'role': 'reader'}).execute()
        return f"https://drive.google.com/uc?export=download&id={file_id}"
    except Exception as e:
        logger.exception(f"Drive upload error: {e}")
        return None

# ----------------- Parsing -----------------
def extract_metro_station(text: str) -> str:
    m = re.search(r"\u24C2️?([\w\s\-]+)", text)
    return m.group(1).strip() if m else ""

def extract_bc_class(text: str) -> Optional[str]:
    m = re.search(r"Клас[:\s]*([A-Za-zА-Яа-я0-9]+)", text, flags=re.I)
    return m.group(1).strip() if m else None

def extract_price_formula(text: str) -> Optional[str]:
    m = re.search(r"ЦІНА[:\s]*([^\n\r]+)", text, flags=re.I)
    if not m:
        m2 = re.search(r"Ціна[:\s]*([^\n\r]+)", text, flags=re.I)
        if m2:
            return m2.group(1).strip()
    return m.group(1).strip() if m else None

async def parse_and_filter_messages(messages, min_size: int, max_size: Optional[int],
                                    min_price_per_m2: Optional[int] = None, max_price_per_m2: Optional[int] = None):
    offers: List[Dict[str, Any]] = []
    pattern = re.compile(r"(\d+(?:-й|-й поверх| поверх|й поверх))\s+(\d+(?:\.\d+)?)m2\s*\((\d+(?:\.\d+)?\$)\)", flags=re.I)
    for message, msg_id, entities in messages:
        if not message:
            continue
        message = message.replace("В наявності", "").strip()

        bc_match = re.search(r"Бізнес-(?:центр|парк)\s+([^\n\r]+)", message)
        bc_name = bc_match.group(1).strip() if bc_match else "БЦ"

        bc_class = extract_bc_class(message)
        price_formula = extract_price_formula(message)
        metro_station = extract_metro_station(message)

        ents = entities or []

        for m in pattern.finditer(message):
            floor, size, price = m.group(1).strip(), m.group(2), m.group(3)
            pos = m.start()
            try:
                size_number = float(size)
                price_total = float(price.replace('$', '').replace(',', ''))
            except Exception:
                continue
            price_per_m2 = round(price_total / size_number, 2)

            if not (min_size <= size_number if max_size is None else min_size <= size_number <= max_size):
                continue
            if (min_price_per_m2 is not None and price_per_m2 < min_price_per_m2) or (max_price_per_m2 is not None and price_per_m2 > max_price_per_m2):
                continue

            link = None
            min_dist = None
            for ent in ents:
                try:
                    ent_offset = ent.offset
                except Exception:
                    continue
                dist = abs(ent_offset - pos)
                if min_dist is None or dist < min_dist:
                    min_dist = dist
                    chosen_ent = ent

            if ents and min_dist is not None:
                if isinstance(chosen_ent, MessageEntityTextUrl):
                    link = chosen_ent.url
                elif isinstance(chosen_ent, MessageEntityUrl):
                    start = chosen_ent.offset
                    end = start + chosen_ent.length
                    link = message[start:end].strip()

            if not link:
                link = f"https://t.me/{CHANNEL_USERNAME[1:]}/{msg_id}"

            lines = [f"<b>{bc_name}</b>"]
            if bc_class:
                lines.append(f"Клас {bc_class}")
            if price_formula:
                lines.append(f"ЦІНА: {price_formula}")
            lines.append(f"{floor}, {int(size_number) if size_number.is_integer() else size_number}м²")
            lines.append(f"💵 {int(price_total):,}$ ({price_per_m2}$/м²)")
            if metro_station:
                lines.append(f"Ⓜ️{metro_station}")

            text_out = "\n".join(lines)

            offers.append({
                'text': text_out,
                'link': link,
                'msg_id': msg_id,
                'price_total': price_total,
                'price_per_m2': price_per_m2,
                'size': size_number,
                'floor': floor,
                'bc_name': bc_name
            })

    offers.sort(key=lambda x: x['price_total'])
    return offers

# ----------------- Collage -----------------
async def ensure_connected():
    if not telethon_client.is_connected():
        try:
            await telethon_client.connect()
        except Exception:
            await telethon_client.start()

async def _download_small_photo_bytes(msg) -> Optional[bytes]:
    if not getattr(msg, 'photo', None):
        return None
    try:
        data = await telethon_client.download_media(msg, file=bytes)
        if data:
            return bytes(data)
    except Exception as e:
        logger.warning(f"Download media failed: {e}")
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
                if len(photos) >= 3:
                    break
        return photos
    except Exception as e:
        logger.exception(f"Error fetching photos for msg {msg_id}: {e}")
        return []

# ----------------- Keyboards -----------------
def build_size_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="До 200 м²", callback_data="size_0_200")],
        [InlineKeyboardButton(text="200-500 м²", callback_data="size_200_500")],
        [InlineKeyboardButton(text="500-1000 м²", callback_data="size_500_1000")],
        [InlineKeyboardButton(text="1000+ м²", callback_data="size_1000_")]
    ])

def build_price_keyboard(min_size, max_size):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Низький (до 20$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_0_20")],
        [InlineKeyboardButton(text="Середній (20$ - 30$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_20_30")],
        [InlineKeyboardButton(text="Високий (більше 30$ за м²)", callback_data=f"price_{min_size}_{max_size or ''}_30_100000")]
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
    return InlineKeyboardMarkup(inline_keyboard=buttons) if buttons else None

# ----------------- send page -----------------
async def send_page(chat_id, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return
    results = session['results']
    page = session['page']
    start = page * PAGE_SIZE
    end = min(len(results), start + PAGE_SIZE)

    nav_kb = build_pagination_keyboard(page, len(results))

    async def ensure_collage_for_offer(offer):
        bc_key = offer.get('bc_name') or str(offer.get('msg_id'))
        if bc_key in collage_url_cache:
            return
        mid = offer['msg_id']
        photos = await fetch_first_3_small_photos(mid)
        collage = make_collage_1_plus_2(photos)
        if collage:
            collage_bytes_cache[mid] = collage
            filename = f"{bc_key}.jpg"
            url = await asyncio.to_thread(upload_collage_to_drive, collage, filename, DRIVE_FOLDER_ID)
            if url:
                collage_url_cache[bc_key] = url
                with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(collage_url_cache, f, ensure_ascii=False, indent=2)

    await asyncio.gather(*[ensure_collage_for_offer(results[i]) for i in range(start, end)])

    for i in range(start, end):
        offer = results[i]
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="Детальніше ➡️", url=offer['link']),
                InlineKeyboardButton(text="📊 Калькулятор ЦІНИ", callback_data=f"calc_{offer['msg_id']}")
            ]
        ])
        mid = offer['msg_id']
        bc_key = offer.get('bc_name') or str(mid)
        if bc_key in collage_url_cache:
            sent = await bot.send_photo(chat_id, collage_url_cache[bc_key], caption=offer['text'], reply_markup=keyboard)
            has_photo = True
        else:
            sent = await bot.send_message(chat_id, offer['text'], reply_markup=keyboard)
            has_photo = False
        calc_store[(chat_id, sent.message_id)] = {
            'offer': offer,
            'has_photo': has_photo,
            'reply_markup': keyboard
        }

    if nav_kb:
        await bot.send_message(chat_id, f"Сторінка {page + 1} із {(len(results) - 1) // PAGE_SIZE + 1}", reply_markup=nav_kb)
    else:
        await bot.send_message(chat_id, f"Сторінка {page + 1} із 1")

def _resize_cover(img: Image.Image, tw: int, th: int) -> Image.Image:
    w, h = img.size
    if w == 0 or h == 0:
        return img
    scale = max(tw / w, th / h)
    nw, nh = int(w * scale), int(h * scale)
    img2 = img.resize((nw, nh), Image.LANCZOS)
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

# ----------------- Handlers -----------------
@router.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("Оберіть метраж:", reply_markup=build_size_keyboard())
    await message.answer("Для нового пошуку натисніть кнопку нижче:",
                         reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="Новий пошук")]], resize_keyboard=True))

@router.message(F.text == "Новий пошук")
async def restart_handler(message: types.Message):
    await start_handler(message)

@router.callback_query(F.data.startswith("size_"))
async def size_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    await callback_query.message.answer("Оберіть діапазон ціни:", reply_markup=build_price_keyboard(min_size, max_size))

@router.callback_query(F.data.startswith("price_"))
async def price_filter_handler(callback_query: types.CallbackQuery):
    _, min_size, max_size, min_price, max_price = callback_query.data.split("_")
    min_size = int(min_size)
    max_size = int(max_size) if max_size.isdigit() else None
    min_price = int(min_price)
    max_price = int(max_price) if max_price.isdigit() else None

    await callback_query.message.answer("Шукаємо відповідні варіанти...")

    messages = await fetch_channel_messages(limit=None)
    parsed = await parse_and_filter_messages(messages, min_size, max_size, min_price, max_price)

    if not parsed:
        await callback_query.message.answer("На жаль, відповідних варіантів не знайдено.")
        return

    user_sessions[callback_query.from_user.id] = {
        'results': parsed,
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

@router.callback_query(F.data.startswith("calc_"))
async def calc_handler(callback_query: types.CallbackQuery):
    await callback_query.answer()
    chat_id = callback_query.message.chat.id
    bot_msg_id = callback_query.message.message_id

    data = calc_store.get((chat_id, bot_msg_id))
    offer = None
    if data:
        offer = data['offer']
        reply_kb = data.get('reply_markup')
    else:
        # fallback: find by chan_msg_id (from callback data)
        try:
            chan_msg_id = int(callback_query.data.split("_", 1)[1])
        except Exception:
            await callback_query.message.answer("Помилка: не вдалося знайти дані для розрахунку.")
            return
        session = user_sessions.get(callback_query.from_user.id)
        reply_kb = None
        if session:
            for o in session['results']:
                if o['msg_id'] == chan_msg_id:
                    offer = o
                    reply_kb = InlineKeyboardMarkup(inline_keyboard=[
                        [InlineKeyboardButton(text="Детальніше ➡️", url=offer['link']),
                         InlineKeyboardButton(text="📊 Калькулятор ЦІНИ", callback_data=f"calc_{offer['msg_id']}")]
                    ])
                    break

    if not offer:
        await callback_query.message.answer("Помилка: офіс не знайдено для розрахунку.", show_alert=True)
        return

    monthly_payment = float(offer['price_total'])
    now = datetime.now()
    y, m = now.year, now.month
    days_in_month = calendar.monthrange(y, m)[1]
    days_left = days_in_month - now.day

    daily = monthly_payment / days_in_month if days_in_month else 0.0
    sum_until_month_end = daily * days_left
    guarantee = monthly_payment * 2
    commission = 0
    total = sum_until_month_end + guarantee + commission

    def fm(v):
        try:
            return f"{int(round(v)):,}"
        except Exception:
            return f"{v}"

    calc_block = (
        "\n\n📊 Розрахунок:\n"
        f"— сума до кінця місяця: {fm(sum_until_month_end)}$\n"
        f"— гарантійна сума: {fm(guarantee)}$\n"
        f"— комісія агента: {commission}$\n"
        f"— ВСЬОГО: {fm(total)}$"
    )

    new_text = offer['text'] + calc_block

    try:
        if data and data.get('has_photo'):
            await bot.edit_message_caption(chat_id=chat_id, message_id=bot_msg_id, caption=new_text, reply_markup=reply_kb)
        else:
            await bot.edit_message_text(chat_id=chat_id, message_id=bot_msg_id, text=new_text, reply_markup=reply_kb)
    except Exception as e:
        logger.exception(f"Error editing message for calculator: {e}")
        # second attempt without reply_markup
        try:
            if data and data.get('has_photo'):
                await bot.edit_message_caption(chat_id=chat_id, message_id=bot_msg_id, caption=new_text)
            else:
                await bot.edit_message_text(chat_id=chat_id, message_id=bot_msg_id, text=new_text)
        except Exception as e2:
            logger.exception(f"Second attempt failed: {e2}")

# ----------------- Fetch channel messages -----------------
async def fetch_channel_messages(limit=None):
    await ensure_connected()
    channel = await telethon_client.get_entity(CHANNEL_USERNAME)
    msgs = await telethon_client.get_messages(channel, limit=limit)
    return [(m.message, m.id, m.entities) for m in msgs if m.message]

# ----------------- Startup -----------------
async def run_bot():
    logger.info("Starting bot...")

    # Запускаємо задачу оновлення токена
    asyncio.create_task(auto_refresh_token())

    if not os.path.exists(CLIENT_SECRET_FILE):
        logger.warning(f"Client secret not found at {CLIENT_SECRET_FILE}. Drive uploads will fail.")

    await telethon_client.start()
    logger.info("Telethon client started")

    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
