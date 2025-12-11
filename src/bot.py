# bot.py — версия с Google Drive (OAuth2 refresh_token, универсальные коллажи 1–3 фото)
#
# Требуемые пакеты:
# pip install aiogram telethon pillow google-auth google-auth-oauthlib google-api-python-client

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
from telethon.tl.types import MessageEntityTextUrl, MessageEntityUrl

# Google Drive libs
try:
    from google.auth.transport.requests import Request
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow  # не используем, но пусть лежит
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload
    _HAS_GOOGLE = True
except Exception:
    _HAS_GOOGLE = False

# ===================== CONFIG =====================

API_TOKEN = os.environ.get('API_TOKEN')
TELEGRAM_API_ID = os.environ.get('TELEGRAM_API_ID')
TELEGRAM_API_HASH = os.environ.get('TELEGRAM_API_HASH')

CHANNEL_OFFICES = '@KyivOfficeRent'
CHANNEL_WAREHOUSES = '@KievSKLAD123'

PAGE_SIZE = 5
MAX_PARALLEL_DOWNLOADS = 6
COLLAGE_W, COLLAGE_H = 1280, 720
JPEG_QUALITY = 85

# ====== Google Drive (OAuth 2.0, refresh_token) ======
SCOPES = ["https://www.googleapis.com/auth/drive"]

GOOGLE_CLIENT_ID = os.environ.get('GOOGLE_CLIENT_ID')
GOOGLE_CLIENT_SECRET = os.environ.get('GOOGLE_CLIENT_SECRET')
GOOGLE_REFRESH_TOKEN = os.environ.get('GOOGLE_REFRESH_TOKEN')

DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')

BASE_DIR = os.path.dirname(__file__)
TEMP_FOLDER = os.path.join(BASE_DIR, "temp_collages")
os.makedirs(TEMP_FOLDER, exist_ok=True)

USE_DRIVE = True  # включён Drive

CACHE_FILE = os.path.join(BASE_DIR, 'collage_url_cache_local.json')

CLIENT_SECRET_FILE = os.path.join(BASE_DIR, "client_secret.json")
TOKEN_FILE = os.path.join(BASE_DIR, "token.json")

# ==================================================

logging.basicConfig(
    level=logging.WARNING,   # в проде показываем только warning/error
    format="%(asctime)s - %(levelname)s - %(message)s"
)
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

# ----------------- Google Drive (через refresh_token) -----------------
_drive_service = None


def init_drive_service():
    """
    Создаём Google Drive service на основе client_id + client_secret + refresh_token.
    """
    global _drive_service

    if not USE_DRIVE:
        raise RuntimeError("Google Drive disabled (USE_DRIVE=False)")

    if not _HAS_GOOGLE:
        raise RuntimeError("Google packages not installed")

    if _drive_service is not None:
        return _drive_service

    creds = Credentials(
        token=None,
        refresh_token=GOOGLE_REFRESH_TOKEN,
        token_uri="https://oauth2.googleapis.com/token",
        client_id=GOOGLE_CLIENT_ID,
        client_secret=GOOGLE_CLIENT_SECRET,
        scopes=SCOPES,
    )

    try:
        creds.refresh(Request())
    except Exception as e:
        logger.exception("Помилка при оновленні Google токена (refresh_token)")
        raise e

    _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


# def upload_collage_to_drive(collage_bytes: bytes, filename: str, folder_id: str) -> Optional[str]:
#     """
#     Загружаем коллаж в Google Drive и возвращаем URL вида https://drive.google.com/uc?id=FILE_ID.
#     (Используем только как хранилище и кэш, НЕ как URL для Telegram)
#     """
#     if not USE_DRIVE:
#         return None
#     try:
#         service = init_drive_service()

#         safe_name = filename.replace("'", "\\'")
#         query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"

#         resp = service.files().list(
#             q=query,
#             spaces="drive",
#             fields="files(id, name)",
#             pageSize=1,
#         ).execute()
#         files = resp.get("files", [])

#         if files:
#             file_id = files[0]["id"]
#         else:
#             media = MediaIoBaseUpload(
#                 BytesIO(collage_bytes),
#                 mimetype="image/jpeg",
#                 resumable=True,
#             )
#             file_metadata = {
#                 "name": filename,
#                 "parents": [folder_id],
#             }
#             created = service.files().create(
#                 body=file_metadata,
#                 media_body=media,
#                 fields="id",
#             ).execute()
#             file_id = created.get("id")

#             service.permissions().create(
#                 fileId=file_id,
#                 body={"type": "anyone", "role": "reader"},
#             ).execute()

#         url = f"https://drive.google.com/uc?id={file_id}"
#         return url

#     except Exception:
#         logger.exception("Помилка завантаження колажу в Google Drive")
#         return None

async def upload_collage_to_drive(collage_bytes: bytes, filename: str, folder_id: str) -> Optional[str]:
    if not USE_DRIVE:
        return None

    try:
        loop = asyncio.get_running_loop()
        service = init_drive_service()

        def _do_upload():
            safe_name = filename.replace("'", "\\'")
            query = f"name = '{safe_name}' and '{folder_id}' in parents and trashed = false"

            # 1) Check existing file
            resp = service.files().list(
                q=query,
                spaces="drive",
                fields="files(id, name)",
                pageSize=1,
            ).execute()

            files = resp.get("files", [])

            if files:
                return files[0]["id"]

            # 2) Upload new file
            media = MediaIoBaseUpload(
                BytesIO(collage_bytes),
                mimetype="image/jpeg",
                resumable=True,
            )
            file_metadata = {
                "name": filename,
                "parents": [folder_id],
            }

            # upload via chunking
            request = service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
            )

            response = None
            while response is None:
                status, response = request.next_chunk()

            file_id = response.get("id")

            # 3) Set public permission
            service.permissions().create(
                fileId=file_id,
                body={"type": "anyone", "role": "reader"},
            ).execute()

            return file_id

        # Execute heavy Drive operations in a thread (non-blocking)
        file_id = await loop.run_in_executor(None, _do_upload)

        return f"https://drive.google.com/uc?id={file_id}"

    except Exception:
        logger.exception("Помилка завантаження колажу в Google Drive")
        return None


def extract_file_id_from_url(url: str) -> str:
    """
    Извлекаем file_id из ссылки вида https://drive.google.com/uc?id=FILE_ID&...
    Если не получилось — возвращаем исходную строку.
    """
    m = re.search(r"[?&]id=([^&]+)", url)
    if m:
        return m.group(1)
    return url


def download_collage_from_drive(file_id: str) -> Optional[bytes]:
    """
    Скачиваем файл из Google Drive по file_id и возвращаем байты.
    """
    if not USE_DRIVE:
        return None
    try:
        service = init_drive_service()
        request = service.files().get_media(fileId=file_id)
        fh = BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            # status.progress() можно было бы логировать, но в проде не нужно
        fh.seek(0)
        return fh.read()
    except Exception:
        logger.exception("Помилка завантаження колажу з Google Drive")
        return None

# ----------------- Utilities & Parsers -----------------
def slugify(text: str) -> str:
    s = re.sub(r"[^\w\s-]", "", text or "", flags=re.UNICODE)
    s = re.sub(r"\s+", "_", s.strip().lower())
    return s[:120] if s else ""


def extract_metro_station(text: str) -> Optional[str]:
    m = re.search(r"Ⓜ️\s*([^\n\r]+)", text)
    if m:
        return m.group(1).strip()
    return None


def extract_bc_class(text: str) -> Optional[str]:
    m = re.search(r"Клас[:\s]*([A-Za-zА-Яа-я0-9]+)", text, flags=re.I)
    if m:
        return m.group(1).strip()
    return None


def extract_price_formula(text: str) -> Optional[str]:
    m = re.search(r"ЦІНА[:\s]*([^\n\r]+)", text, flags=re.I)
    if not m:
        m2 = re.search(r"Ціна[:\s]*([^\n\r]+)", text, flags=re.I)
        if m2:
            return m2.group(1).strip()
    return m.group(1).strip() if m else None


OFFER_LINE_RE = re.compile(
    r"([^\n\r]+?)\s+(\d+(?:\.\d+)?)m2\s*\(\s*([0-9\.,]+)\$\s*\)\s*(?:\((https?://[^\s\)]+)\))?",
    flags=re.I
)

# ----------------- Photo download helpers -----------------
async def ensure_connected():
    if not telethon_client.is_connected():
        try:
            await telethon_client.connect()
        except Exception:
            await telethon_client.start()
    # если не авторизован – просто предупредим в логах
    try:
        if not await telethon_client.is_user_authorized():
            logger.warning("Telethon client is not authorized!")
    except Exception:
        pass


async def _download_small_photo_bytes(msg) -> Optional[bytes]:
    if not getattr(msg, "photo", None):
        return None
    try:
        data = await telethon_client.download_media(msg, file=bytes)
        if data:
            return bytes(data)
    except Exception as e:
        logger.warning(f"Download media failed: {e}")
    return None


async def fetch_first_3_small_photos_for_channel(channel_username: str, msg_id: int) -> List[bytes]:
    await ensure_connected()
    try:
        channel = await telethon_client.get_entity(channel_username)
        message = await telethon_client.get_messages(channel, ids=msg_id)
        if not message:
            return []
        grouped_id = getattr(message, "grouped_id", None)
        msgs = []
        if grouped_id:
            ids_window = list(range(max(1, msg_id - 20), msg_id + 21))
            all_msgs = await telethon_client.get_messages(channel, ids=ids_window)
            msgs = [m for m in all_msgs if getattr(m, "grouped_id", None) == grouped_id]
            msgs.sort(key=lambda x: x.id)
        else:
            msgs = [message]

        photos: List[bytes] = []
        for m in msgs:
            if getattr(m, "photo", None) is not None:
                async with telethon_semaphore:
                    b = await _download_small_photo_bytes(m)
                if b:
                    photos.append(b)
                if len(photos) >= 3:
                    break
        return photos
    except Exception as e:
        logger.exception(f"Error fetching photos for msg {msg_id} from {channel_username}: {e}")
        return []

# ----------------- Collage layout: универсальный (1–3 фото) -----------------
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


def make_universal_collage(images_bytes: List[bytes]) -> Optional[bytes]:
    if not images_bytes:
        return None

    final_w, final_h = COLLAGE_W, COLLAGE_H
    n = min(3, len(images_bytes))

    try:
        imgs = [Image.open(BytesIO(b)).convert("RGB") for b in images_bytes[:n]]

        if n == 1:
            img1 = _resize_cover(imgs[0], final_w, final_h)
            collage = Image.new("RGB", (final_w, final_h))
            collage.paste(img1, (0, 0))

        elif n == 2:
            left_w = final_w // 2
            right_w = final_w - left_w
            img1 = _resize_cover(imgs[0], left_w, final_h)
            img2 = _resize_cover(imgs[1], right_w, final_h)
            collage = Image.new("RGB", (final_w, final_h))
            collage.paste(img1, (0, 0))
            collage.paste(img2, (left_w, 0))

        else:
            left_w = final_w // 2
            right_w = final_w - left_w
            half_h = final_h // 2
            img1 = _resize_cover(imgs[0], left_w, final_h)
            img2 = _resize_cover(imgs[1], right_w, half_h)
            img3 = _resize_cover(imgs[2], right_w, half_h)
            collage = Image.new("RGB", (final_w, final_h))
            collage.paste(img1, (0, 0))
            collage.paste(img2, (left_w, 0))
            collage.paste(img3, (left_w, half_h))

        out = BytesIO()
        collage.save(out, format="JPEG", quality=JPEG_QUALITY, optimize=True)
        out.seek(0)
        return out.read()

    except Exception as e:
        logger.exception(f"Error creating collage: {e}")
        return None

# ----------------- Keyboards -----------------
def new_search_keyboard():
    kb = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Новий пошук")]],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def main_menu_keyboard():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🏢 Офіс")],
            [KeyboardButton(text="🏭 Склад")],
            [KeyboardButton(text="Новий пошук")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def warehouses_shore_keyboard():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Лівий берег")],
            [KeyboardButton(text="Правий берег")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def warehouses_size_keyboard():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="До 1000 м²")],
            [KeyboardButton(text="Від 1000 м²")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def offices_size_keyboard_reply():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="До 200 м²")],
            [KeyboardButton(text="200–500 м²")],
            [KeyboardButton(text="500–1000 м²")],
            [KeyboardButton(text="1000+ м²")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def offices_price_keyboard_reply():
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="До 20$ за м²")],
            [KeyboardButton(text="20–30$ за м²")],
            [KeyboardButton(text="Більше 30$ за м²")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return kb


def offer_card_keyboard(detail_url: str, msg_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Детальніше ➡️", url=detail_url)],
        [InlineKeyboardButton(text="📊 Калькулятор ЦІНИ", callback_data=f"calc_{msg_id}")]
    ])

# ----------------- Parsing & filtering (офисы/склады) -----------------
async def parse_and_filter_messages_offices(
    messages,
    min_size: int,
    max_size: Optional[int],
    min_price_per_m2: Optional[int] = None,
    max_price_per_m2: Optional[int] = None
):
    offers: List[Dict[str, Any]] = []
    pattern = re.compile(
        r"(\d+(?:-й|-й поверх| поверх|й поверх))\s+(\d+(?:\.\d+)?)m2\s*\((\d+(?:\.\d+)?\$)\)",
        flags=re.I
    )
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
            if (min_price_per_m2 is not None and price_per_m2 < min_price_per_m2) or \
               (max_price_per_m2 is not None and price_per_m2 > max_price_per_m2):
                continue

            link = None
            min_dist = None
            chosen_ent = None
            for ent in ents:
                try:
                    ent_offset = ent.offset
                except Exception:
                    continue
                dist = abs(ent_offset - pos)
                if min_dist is None or dist < min_dist:
                    min_dist = dist
                    chosen_ent = ent

            if chosen_ent:
                if isinstance(chosen_ent, MessageEntityTextUrl):
                    link = chosen_ent.url
                elif isinstance(chosen_ent, MessageEntityUrl):
                    try:
                        start = chosen_ent.offset
                        end = start + chosen_ent.length
                        link = message[start:end].strip()
                    except Exception:
                        link = None

            if not link:
                link = f"https://t.me/{CHANNEL_OFFICES[1:]}/{msg_id}"

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
                'bc_name': bc_name,
                'type': 'office'
            })

    offers.sort(key=lambda x: x['price_total'])
    return offers


async def parse_and_filter_messages_warehouses(messages, shore_filter: Optional[str], size_choice: Optional[str]):
    offers: List[Dict[str, Any]] = []
    for message, msg_id, entities in messages:
        if not message:
            continue
        txt = message.strip()
        lines = [ln.strip() for ln in txt.splitlines() if ln.strip()]
        if not lines:
            continue
        title = lines[0]
        addr = None
        shore = None
        height = None
        power = None
        metro = None
        w_class = None

        for ln in lines[1:12]:
            ln_low = ln.lower()
            if ln.startswith("📍") or "адрес" in ln_low:
                m = re.search(r"[:]\s*(.+)", ln)
                if m:
                    addr = m.group(1).strip()
                else:
                    addr = ln.replace("📍", "").strip()
            if "берег" in ln_low:
                m = re.search(r"[Бб]ерег[:\s]*([^\n\r]+)", ln)
                if m:
                    shore = m.group(1).strip().split()[0]
            if ln.startswith("Ⓜ️") or ln_low.startswith("м"):
                metro = ln.replace("Ⓜ️", "").strip()
            if "висота" in ln_low or "висота стелі" in ln_low:
                m = re.search(r"([\d\.]+)\s*m", ln, flags=re.I)
                if m:
                    try:
                        height = float(m.group(1))
                    except Exception:
                        height = None
            if "потужн" in ln_low or "потужність" in ln_low or "квт" in ln_low:
                m = re.search(r"([\d\.,]+)\s*(кВт|kw|kW|MW|мВт)?", ln, flags=re.I)
                if m:
                    power = m.group(1).replace(",", ".")
            if "клас" in ln_low:
                m = re.search(r"клас[:\s]*([A-Za-zА-Яа-я0-9]+)", ln, flags=re.I)
                if m:
                    w_class = m.group(1).strip()

        for m in OFFER_LINE_RE.finditer(message):
            desc = m.group(1).strip()
            size = float(m.group(2))
            price_total = float(m.group(3).replace(",", ""))
            url = m.group(4)
            price_per_m2 = round(price_total / size, 2) if size else 0.0

            if shore_filter:
                if shore:
                    if shore_filter.lower().startswith("лів") and not shore.lower().startswith("лів"):
                        continue
                    if shore_filter.lower().startswith("прав") and not shore.lower().startswith("прав"):
                        continue
                else:
                    continue

            if size_choice == "<=1000":
                if not (size <= 1000.0):
                    continue
            elif size_choice == ">1000":
                if not (size >= 1000.0):
                    continue

            link = url
            if not link:
                ents = entities or []
                min_dist = None
                chosen_ent = None
                pos = m.start()
                for ent in ents:
                    try:
                        off = ent.offset
                    except Exception:
                        continue
                    dist = abs(off - pos)
                    if min_dist is None or dist < min_dist:
                        min_dist = dist
                        chosen_ent = ent
                if chosen_ent:
                    if isinstance(chosen_ent, MessageEntityTextUrl):
                        link = chosen_ent.url
                    elif isinstance(chosen_ent, MessageEntityUrl):
                        try:
                            start = chosen_ent.offset
                            end = start + chosen_ent.length
                            link = message[start:end].strip()
                        except Exception:
                            link = None
            if not link:
                link = f"https://t.me/{CHANNEL_WAREHOUSES[1:]}/{msg_id}"

            display_name = title or (addr or f"Склад {msg_id}")
            lines_out = [f"<b>{display_name}</b>"]
            if addr:
                lines_out.append(f"📍 {addr}")
            if metro:
                lines_out.append(f"Ⓜ️ {metro}")
            if shore:
                lines_out.append(f"🚩 Берег: {shore}")
            if w_class:
                lines_out.append(f"🏗 Клас: {w_class}")
            if height:
                lines_out.append(f"📏 Висота стелі: {int(height) if float(height).is_integer() else height} м")
            if power:
                lines_out.append(f"⚡ Потужність: {power}")
            lines_out.append(f"{desc}, {int(size) if size.is_integer() else size}м²")
            lines_out.append(f"💵 {int(price_total):,}$ ({price_per_m2}$/м²)")

            text_out = "\n".join(lines_out)
            offers.append({
                'text': text_out,
                'link': link,
                'msg_id': msg_id,
                'price_total': price_total,
                'price_per_m2': price_per_m2,
                'size': size,
                'desc': desc,
                'bc_name': display_name,
                'type': 'warehouse',
                'height': height,
                'w_class': w_class
            })
    offers.sort(key=lambda x: x['price_total'])
    return offers

# ----------------- Ensure collage, cache & send page -----------------
async def ensure_collage_and_cache_for_offer(channel_username: str, offer: Dict[str, Any]):
    """
    Гарантирует, что для оффера есть байты коллажа в collage_bytes_cache.

    Приоритет:
    1) Если уже есть в collage_bytes_cache — ничего не делаем.
    2) Если есть локальный файл temp_collages/slug.jpg — читаем его.
    3) Если есть запись в collage_url_cache (Drive) — скачиваем из Drive и кладём в temp + cache.
    4) Иначе — качаем 1–3 фото из канала, создаём коллаж, сохраняем локально, грузим в Drive, пишем cache.
    """
    msg_id = offer["msg_id"]
    if msg_id in collage_bytes_cache:
        return

    bc_key_raw = offer.get("bc_name") or str(msg_id)
    initial_slug = slugify(bc_key_raw)
    bc_key_slug = initial_slug or f"offer_{msg_id}"

    local_name = f"{bc_key_slug}.jpg"
    local_path = os.path.join(TEMP_FOLDER, local_name)

    # 2) Локальный файл уже есть
    if os.path.exists(local_path):
        try:
            with open(local_path, "rb") as f:
                data = f.read()
            if data:
                collage_bytes_cache[msg_id] = data
                return
        except Exception:
            logger.exception("Ошибка чтения локального файла коллажа")

    # 3) Если есть запись в Drive-кэше — пробуем скачать
    if USE_DRIVE and bc_key_slug in collage_url_cache:
        url = collage_url_cache[bc_key_slug]
        file_id = extract_file_id_from_url(url)
        data = await asyncio.to_thread(download_collage_from_drive, file_id)
        if data:
            try:
                with open(local_path, "wb") as f:
                    f.write(data)
            except Exception:
                logger.exception("Ошибка записи локального файла при скачивании с Drive")
            collage_bytes_cache[msg_id] = data
            return
        # если скачивание с Drive не удалось, пойдём в шаг 4 (создание с нуля)

    # 4) Генерация с нуля: качаем фото из Telegram, создаём коллаж
    photos = await fetch_first_3_small_photos_for_channel(channel_username, msg_id)
    if not photos:
        return

    collage_bytes = make_universal_collage(photos)
    if not collage_bytes:
        return

    # Сохраняем локально
    try:
        with open(local_path, 'wb') as f:
            f.write(collage_bytes)
    except Exception:
        logger.exception("Ошибка записи локального файла коллажа")

    # Кэшируем в памяти
    collage_bytes_cache[msg_id] = collage_bytes

    # Пытаемся загрузить в Drive и обновить JSON-кэш
    if USE_DRIVE:
        url = await asyncio.to_thread(upload_collage_to_drive, collage_bytes, local_name, DRIVE_FOLDER_ID)
        if url:
            collage_url_cache[bc_key_slug] = url
            try:
                with open(CACHE_FILE, 'w', encoding='utf-8') as f:
                    json.dump(collage_url_cache, f, ensure_ascii=False, indent=2)
            except Exception:
                logger.exception("Ошибка записи JSON-кэша коллажей")


async def send_page(chat_id, user_id):
    session = user_sessions.get(user_id)
    if not session:
        return
    results = session.get('results', [])
    page = session.get('page', 0)
    start = page * PAGE_SIZE
    end = min(len(results), start + PAGE_SIZE)

    # подготавливаем коллажи для офферов этой страницы
    tasks = []
    for i in range(start, end):
        offer = results[i]
        ch = CHANNEL_OFFICES if offer.get('type') == 'office' else CHANNEL_WAREHOUSES
        tasks.append(ensure_collage_and_cache_for_offer(ch, offer))
    await asyncio.gather(*tasks)

    for i in range(start, end):
        offer = results[i]
        keyboard = offer_card_keyboard(offer['link'], offer['msg_id'])
        sent = None
        has_photo = False
        try:
            collage_bytes = collage_bytes_cache.get(offer['msg_id'])
            if collage_bytes:
                sent = await bot.send_photo(
                    chat_id,
                    BufferedInputFile(collage_bytes, filename="collage.jpg"),
                    caption=offer['text'],
                    reply_markup=keyboard
                )
                has_photo = True
            else:
                sent = await bot.send_message(chat_id, offer['text'], reply_markup=keyboard)
                has_photo = False

            if sent:
                calc_store[(chat_id, sent.message_id)] = {
                    'offer': offer,
                    'has_photo': has_photo,
                    'reply_markup': keyboard
                }

        except Exception as e:
            logger.exception(f"Error sending offer: {e}")

    total_pages = (len(results) - 1) // PAGE_SIZE + 1 if results else 1
    rows = []
    if page > 0:
        rows.append(InlineKeyboardButton(text="⬅️ Назад", callback_data="page_prev"))
    if page < total_pages - 1:
        rows.append(InlineKeyboardButton(text="Далі ➡️", callback_data="page_next"))
    nav_kb = InlineKeyboardMarkup(inline_keyboard=[rows]) if rows else None

    if nav_kb:
        await bot.send_message(chat_id, f"Сторінка {page + 1} із {total_pages}", reply_markup=nav_kb)
    else:
        await bot.send_message(chat_id, f"Сторінка {page + 1} із {total_pages}")

    await bot.send_message(chat_id, "Щоб почати новий пошук:", reply_markup=new_search_keyboard())

# ----------------- Handlers -----------------
@router.message(CommandStart())
async def start_handler(message: types.Message):
    await message.answer("Оберіть напрямок пошуку:", reply_markup=main_menu_keyboard())


@router.message(F.text == "Новий пошук")
async def new_search_handler(message: types.Message):
    await message.answer("Оберіть напрямок пошуку:", reply_markup=main_menu_keyboard())


@router.message(F.text == "🏢 Офіс")
async def office_entry(message: types.Message):
    user_sessions[message.from_user.id] = {'type': 'office'}
    await message.answer("Оберіть метраж офісу:", reply_markup=offices_size_keyboard_reply())


@router.message(F.text == "До 200 м²")
async def office_size_le_200(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['min_size'] = 0
    session['max_size'] = 200
    user_sessions[message.from_user.id] = session
    await message.answer("Оберіть діапазон ціни за м²:", reply_markup=offices_price_keyboard_reply())


@router.message(F.text == "200–500 м²")
async def office_size_200_500(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['min_size'] = 200
    session['max_size'] = 500
    user_sessions[message.from_user.id] = session
    await message.answer("Оберіть діапазон ціни за м²:", reply_markup=offices_price_keyboard_reply())


@router.message(F.text == "500–1000 м²")
async def office_size_500_1000(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['min_size'] = 500
    session['max_size'] = 1000
    user_sessions[message.from_user.id] = session
    await message.answer("Оберіть діапазон ціни за м²:", reply_markup=offices_price_keyboard_reply())


@router.message(F.text == "1000+ м²")
async def office_size_1000_plus(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['min_size'] = 1000
    session['max_size'] = None
    user_sessions[message.from_user.id] = session
    await message.answer("Оберіть діапазон ціни за м²:", reply_markup=offices_price_keyboard_reply())


@router.message(F.text == "До 20$ за м²")
async def office_price_low(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    min_size = session.get('min_size', 0)
    max_size = session.get('max_size', None)
    min_price = 0
    max_price = 20
    await message.answer(
        "Шукаємо відповідні варіанти...",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⏳ Зачекайте...")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    messages = await fetch_channel_messages(limit=None)
    parsed_offices = await parse_and_filter_messages_offices(messages, min_size, max_size, min_price, max_price)
    if not parsed_offices:
        await message.answer("На жаль, відповідних варіантів не знайдено.", reply_markup=new_search_keyboard())
        return
    user_sessions[message.from_user.id] = {'results': parsed_offices, 'page': 0}
    await send_page(message.chat.id, message.from_user.id)


@router.message(F.text == "20–30$ за м²")
async def office_price_mid(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    min_size = session.get('min_size', 0)
    max_size = session.get('max_size', None)
    min_price = 20
    max_price = 30
    await message.answer(
        "Шукаємо відповідні варіанти...",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⏳ Зачекайте...")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    messages = await fetch_channel_messages(limit=None)
    parsed_offices = await parse_and_filter_messages_offices(messages, min_size, max_size, min_price, max_price)
    if not parsed_offices:
        await message.answer("На жаль, відповідних варіантів не знайдено.", reply_markup=new_search_keyboard())
        return
    user_sessions[message.from_user.id] = {'results': parsed_offices, 'page': 0}
    await send_page(message.chat.id, message.from_user.id)


@router.message(F.text == "Більше 30$ за м²")
async def office_price_high(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    min_size = session.get('min_size', 0)
    max_size = session.get('max_size', None)
    min_price = 30
    max_price = 1000000
    await message.answer(
        "Шукаємо відповідні варіанти...",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="⏳ Зачекайте...")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
    )
    messages = await fetch_channel_messages(limit=None)
    parsed_offices = await parse_and_filter_messages_offices(messages, min_size, max_size, min_price, max_price)
    if not parsed_offices:
        await message.answer("На жаль, відповідних варіантів не знайдено.", reply_markup=new_search_keyboard())
        return
    user_sessions[message.from_user.id] = {'results': parsed_offices, 'page': 0}
    await send_page(message.chat.id, message.from_user.id)


@router.message(F.text == "🔙 Назад")
async def back_handler(message: types.Message):
    await message.answer("Повертаємось до вибору:", reply_markup=main_menu_keyboard())


@router.message(F.text == "🏭 Склад")
async def warehouse_entry(message: types.Message):
    await message.answer("Оберіть берег:", reply_markup=warehouses_shore_keyboard())


@router.message(F.text == "Лівий берег")
async def warehouse_shore_left(message: types.Message):
    user_sessions[message.from_user.id] = {'type': 'warehouse', 'shore': 'Лівий'}
    await message.answer("Оберіть метраж:", reply_markup=warehouses_size_keyboard())


@router.message(F.text == "Правий берег")
async def warehouse_shore_right(message: types.Message):
    user_sessions[message.from_user.id] = {'type': 'warehouse', 'shore': 'Правий'}
    await message.answer("Оберіть метраж:", reply_markup=warehouses_size_keyboard())


@router.message(F.text == "До 1000 м²")
async def warehouse_size_le_1000(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['size_choice'] = "<=1000"
    user_sessions[message.from_user.id] = session
    await message.answer("Шукаємо склади — будь ласка зачекайте...")
    messages = await fetch_channel_messages_for(CHANNEL_WAREHOUSES, limit=None)
    shore = session.get('shore')
    size_choice = session.get('size_choice')
    parsed = await parse_and_filter_messages_warehouses(messages, shore, size_choice)
    if not parsed:
        await message.answer("На жаль, відповідних складів не знайдено.", reply_markup=new_search_keyboard())
        return
    user_sessions[message.from_user.id] = {'results': parsed, 'page': 0}
    await send_page(message.chat.id, message.from_user.id)


@router.message(F.text == "Від 1000 м²")
async def warehouse_size_ge_1000(message: types.Message):
    session = user_sessions.get(message.from_user.id, {})
    session['size_choice'] = ">1000"
    user_sessions[message.from_user.id] = session
    await message.answer("Шукаємо склади — будь ласка зачекайте...")
    messages = await fetch_channel_messages_for(CHANNEL_WAREHOUSES, limit=None)
    shore = session.get('shore')
    size_choice = session.get('size_choice')
    parsed = await parse_and_filter_messages_warehouses(messages, shore, size_choice)
    if not parsed:
        await message.answer("На жаль, відповідних складів не знайдено.", reply_markup=new_search_keyboard())
        return
    user_sessions[message.from_user.id] = {'results': parsed, 'page': 0}
    await send_page(message.chat.id, message.from_user.id)

# ----------------- Pagination & Calculator -----------------
@router.callback_query(F.data == "page_next")
async def page_next_handler(callback_query: types.CallbackQuery):
    try:
        await callback_query.answer()
    except Exception:
        pass
    session = user_sessions.get(callback_query.from_user.id)
    if session and session['page'] < (len(session['results']) - 1) // PAGE_SIZE:
        session['page'] += 1
        await send_page(callback_query.message.chat.id, callback_query.from_user.id)


@router.callback_query(F.data == "page_prev")
async def page_prev_handler(callback_query: types.CallbackQuery):
    try:
        await callback_query.answer()
    except Exception:
        pass
    session = user_sessions.get(callback_query.from_user.id)
    if session and session['page'] > 0:
        session['page'] -= 1
        await send_page(callback_query.message.chat.id, callback_query.from_user.id)


@router.callback_query(F.data.startswith("calc_"))
async def calculator_handler(callback_query: types.CallbackQuery):
    try:
        await callback_query.answer()
    except Exception:
        # если запрос протух — просто игнор
        pass

    chat_id = callback_query.message.chat.id
    bot_msg_id = callback_query.message.message_id

    data = calc_store.get((chat_id, bot_msg_id))
    offer = None
    reply_kb = None
    has_photo = False
    if data:
        offer = data.get("offer")
        reply_kb = data.get("reply_markup")
        has_photo = data.get("has_photo", False)
    else:
        try:
            chan_msg_id = int(callback_query.data.split("_", 1)[1])
        except Exception:
            chan_msg_id = None
        if chan_msg_id:
            session = user_sessions.get(callback_query.from_user.id)
            if session:
                for o in session.get("results", []):
                    if o.get("msg_id") == chan_msg_id:
                        offer = o
                        reply_kb = offer_card_keyboard(offer["link"], offer["msg_id"])
                        break

    if not offer:
        await callback_query.message.answer("Помилка: не знайдено даних для калькулятора.")
        return

    monthly_payment = float(offer.get("price_total", 0.0))
    now = datetime.now()
    y, m = now.year, now.month
    days_in_month = calendar.monthrange(y, m)[1]
    days_left = days_in_month - now.day

    daily = monthly_payment / days_in_month if days_in_month else 0.0
    sum_until_month_end = daily * days_left
    guarantee = monthly_payment * 2

    if offer.get("type") == "warehouse":
        commission = monthly_payment * 0.5
    else:
        commission = 0.0

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
        f"— комісія агента: {fm(commission)}$\n"
        f"— ВСЬОГО: {fm(total)}$"
    )

    new_text = offer["text"] + calc_block

    try:
        if data and has_photo:
            await bot.edit_message_caption(
                chat_id=chat_id,
                message_id=bot_msg_id,
                caption=new_text,
                reply_markup=reply_kb
            )
        else:
            await bot.edit_message_text(
                chat_id=chat_id,
                message_id=bot_msg_id,
                text=new_text,
                reply_markup=reply_kb
            )
    except Exception as e:
        err = str(e)
        if "message is not modified" in err:
            pass
        else:
            logger.exception(f"Error editing message for calculator: {e}")

# ----------------- Channel fetching helpers -----------------
async def fetch_channel_messages(limit=None):
    await ensure_connected()
    channel = await telethon_client.get_entity(CHANNEL_OFFICES)
    msgs = await telethon_client.get_messages(channel, limit=limit)
    return [(m.message, m.id, m.entities) for m in msgs if m.message]


async def fetch_channel_messages_for(channel_username: str, limit: Optional[int] = None):
    try:
        await ensure_connected()
        channel = await telethon_client.get_entity(channel_username)
        history = []
        async for message in telethon_client.iter_messages(channel, limit=limit):
            text = message.message or ""
            if text:
                history.append((text, message.id, message.entities))
        return history
    except Exception as e:
        logger.exception(f"Error fetching messages from {channel_username}: {e}")
        return []

# ----------------- Startup -----------------
# async def run_bot():
#     await telethon_client.start()
#     await dp.start_polling(bot)


# if __name__ == "__main__":
#     try:
#         asyncio.run(run_bot())
#     except Exception as e:
#         logger.exception(f"Fatal error: {e}")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def safe_polling():
    delay = 1

    while True:
        try:
            logger.info("Starting polling...")
            await dp.start_polling(
                bot,
                polling_timeout=5,  # VERY IMPORTANT ON HEROKU
                allowed_updates=dp.resolve_used_update_types(),
                handle_signals=False
            )
        except Exception as e:
            logger.error(f"Polling failed: {e}. Retrying in {delay}s...")
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
        else:
            delay = 1   # reset delay if polling stops cleanly


async def run_bot():
    # Telethon client
    await telethon_client.start()

    # Run Telethon in background
    asyncio.create_task(telethon_client.run_until_disconnected())

    # Start Telegram bot polling
    await safe_polling()


if __name__ == "__main__":
    try:
        asyncio.run(run_bot())
    except Exception as e:
        logger.exception(f"Fatal error: {e}")

