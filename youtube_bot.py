"""
╔══════════════════════════════════════════════════════════════════╗
║         🎬 YouTube Downloader Bot — Полный код                  ║
║                                                                  ║
║  Установка зависимостей:                                         ║
║      pip install aiogram==3.13.1 yt-dlp aiohttp aiofiles        ║
║                                                                  ║
║  Запуск:  python youtube_bot.py                                  ║
╚══════════════════════════════════════════════════════════════════╝
"""

# ╔══════════════════════════════════════════════════════════════════╗
# ║                      ⚙️  НАСТРОЙКИ                             ║
# ╚══════════════════════════════════════════════════════════════════╝

BOT_TOKEN        = "8546106114:AAGpGGzszJlUIxwGbWQpzez7Vbcz883Wg14"
ADMIN_IDS        = [8535260202]
SUPPORT_USERNAME = "@famelonov"

VIP_PRICE_STARS  = 15
VIP_DAYS         = 30
REF_INVITE_COUNT = 3
REF_VIP_DAYS     = 7

MAX_FILE_SIZE_MB = 200

# Текст рекламы бота, который добавляется к подписи файла (только не-ВИП)
BOT_AD_TEXT = "\n\n🤖 <b>Скачано через @FVyoutube_bot</b>\n📥 Скачивай видео, аудио и превью с YouTube бесплатно!"

# ═══════════════════════════════════════════════════════════════════

import asyncio
import logging
import sqlite3
import os
import random
import tempfile
import shutil
from datetime import datetime, timedelta
from pathlib import Path

from aiogram import Bot, Dispatcher, F, Router
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup, KeyboardButton,
    LabeledPrice, PreCheckoutQuery,
    FSInputFile, InputMediaPhoto, InputMediaVideo,
    ReplyKeyboardRemove
)
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.deep_linking import create_start_link

try:
    import yt_dlp
except ImportError:
    print("❌ Установите yt-dlp: pip install yt-dlp")
    exit(1)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

bot = Bot(token=BOT_TOKEN)
dp  = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

TEMP_DIR = Path("temp_downloads")
TEMP_DIR.mkdir(exist_ok=True)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      🗄️  БАЗА ДАННЫХ                           ║
# ╚══════════════════════════════════════════════════════════════════╝

DB = "ytbot.db"

def get_db():
    return sqlite3.connect(DB)

def init_db():
    with get_db() as c:
        c.executescript("""
            CREATE TABLE IF NOT EXISTS users (
                user_id     INTEGER PRIMARY KEY,
                username    TEXT    DEFAULT '',
                full_name   TEXT    DEFAULT '',
                joined_at   TEXT    DEFAULT '',
                ref_by      INTEGER DEFAULT NULL,
                ref_count   INTEGER DEFAULT 0,
                ref_used    INTEGER DEFAULT 0,
                downloads   INTEGER DEFAULT 0,
                is_banned   INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS vip (
                user_id     INTEGER PRIMARY KEY,
                expires_at  TEXT    NOT NULL
            );

            CREATE TABLE IF NOT EXISTS channels (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id       TEXT,
                title            TEXT,
                link             TEXT,
                auto_delete_days INTEGER DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS broadcasts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                admin_id    INTEGER,
                sent        INTEGER DEFAULT 0,
                created_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS stars_payments (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER,
                stars       INTEGER,
                payload     TEXT,
                created_at  TEXT
            );

            CREATE TABLE IF NOT EXISTS ad_posts (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                type             TEXT    NOT NULL DEFAULT 'text',
                file_id          TEXT    DEFAULT NULL,
                caption          TEXT    DEFAULT '',
                buttons          TEXT    DEFAULT NULL,
                auto_delete_days INTEGER DEFAULT 0,
                created_at       TEXT    DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS sent_ad_messages (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                post_id     INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                message_id  INTEGER NOT NULL,
                sent_at     TEXT    DEFAULT ''
            );

            CREATE TABLE IF NOT EXISTS downloads_log (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                dl_type     TEXT    DEFAULT '',
                created_at  TEXT    DEFAULT ''
            );
        """)


# ── пользователи ─────────────────────────────────────────────────

def get_user(uid: int):
    with get_db() as c:
        return c.execute("SELECT * FROM users WHERE user_id=?", (uid,)).fetchone()

def reg_user(uid, username, full_name, ref_by=None):
    with get_db() as c:
        c.execute(
            "INSERT OR IGNORE INTO users (user_id,username,full_name,joined_at,ref_by) VALUES (?,?,?,?,?)",
            (uid, username or "", full_name or "", datetime.now().strftime("%Y-%m-%d %H:%M"), ref_by)
        )

def inc_downloads(uid, dl_type: str = ""):
    with get_db() as c:
        c.execute("UPDATE users SET downloads=downloads+1 WHERE user_id=?", (uid,))
        c.execute(
            "INSERT INTO downloads_log (user_id, dl_type, created_at) VALUES (?,?,?)",
            (uid, dl_type, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )

def inc_ref_count(uid):
    with get_db() as c:
        c.execute("UPDATE users SET ref_count=ref_count+1 WHERE user_id=?", (uid,))

def set_ref_used(uid):
    with get_db() as c:
        c.execute("UPDATE users SET ref_used=1 WHERE user_id=?", (uid,))

def ban_toggle(uid):
    with get_db() as c:
        row = c.execute("SELECT is_banned FROM users WHERE user_id=?", (uid,)).fetchone()
        if row:
            new = 0 if row[0] else 1
            c.execute("UPDATE users SET is_banned=? WHERE user_id=?", (new, uid))
            return new
    return None

def all_user_ids():
    with get_db() as c:
        return [r[0] for r in c.execute("SELECT user_id FROM users WHERE is_banned=0")]

def get_stats():
    with get_db() as c:
        total   = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        vip_cnt = c.execute("SELECT COUNT(*) FROM vip WHERE expires_at > ?",
                            (datetime.now().strftime("%Y-%m-%d %H:%M"),)).fetchone()[0]
        dls     = c.execute("SELECT SUM(downloads) FROM users").fetchone()[0] or 0
        today   = datetime.now().strftime("%Y-%m-%d")
        new     = c.execute("SELECT COUNT(*) FROM users WHERE joined_at LIKE ?", (f"{today}%",)).fetchone()[0]
    return total, vip_cnt, dls, new


# ── ВИП ──────────────────────────────────────────────────────────

def get_vip(uid: int):
    with get_db() as c:
        return c.execute("SELECT * FROM vip WHERE user_id=?", (uid,)).fetchone()

def is_vip(uid: int) -> bool:
    row = get_vip(uid)
    if not row:
        return False
    return datetime.strptime(row[1], "%Y-%m-%d %H:%M") > datetime.now()

def give_vip(uid: int, days: int):
    now = datetime.now()
    existing = get_vip(uid)
    if existing:
        current = datetime.strptime(existing[1], "%Y-%m-%d %H:%M")
        base = current if current > now else now
    else:
        base = now
    expires = (base + timedelta(days=days)).strftime("%Y-%m-%d %H:%M")
    with get_db() as c:
        c.execute("INSERT OR REPLACE INTO vip (user_id, expires_at) VALUES (?,?)", (uid, expires))
    return expires

def remove_vip(uid: int):
    with get_db() as c:
        c.execute("DELETE FROM vip WHERE user_id=?", (uid,))

def vip_expires_str(uid: int) -> str:
    row = get_vip(uid)
    if not row:
        return "нет"
    exp = datetime.strptime(row[1], "%Y-%m-%d %H:%M")
    if exp <= datetime.now():
        return "истёк"
    delta = exp - datetime.now()
    days  = delta.days
    hours = delta.seconds // 3600
    return f"{days}д {hours}ч (до {exp.strftime('%d.%m.%Y %H:%M')})"


# ── каналы ───────────────────────────────────────────────────────

def get_channels():
    with get_db() as c:
        return c.execute("SELECT * FROM channels").fetchall()

def add_channel(channel_id, title, link, auto_delete_days: int = 0):
    with get_db() as c:
        c.execute("INSERT INTO channels (channel_id,title,link,auto_delete_days) VALUES (?,?,?,?)",
                  (channel_id, title, link, auto_delete_days))

def del_channel(row_id):
    with get_db() as c:
        c.execute("DELETE FROM channels WHERE id=?", (row_id,))

async def check_subscriptions(uid: int):
    channels = get_channels()
    missing = []
    for ch in channels:
        try:
            m = await bot.get_chat_member(ch[1], uid)
            if m.status in ("left", "kicked", "banned"):
                missing.append(ch)
        except Exception:
            missing.append(ch)
    return missing


async def require_subscription(msg_or_call, uid):
    if is_vip(uid):
        return True
    missing = await check_subscriptions(uid)
    if missing:
        text = "📢 Для использования бота подпишитесь на наши каналы:"
        if isinstance(msg_or_call, Message):
            sent = await msg_or_call.answer(text, reply_markup=sub_kb(missing))
        else:
            sent = await msg_or_call.message.answer(text, reply_markup=sub_kb(missing))
        # Автоудаление сообщения о подписке (берём минимальный таймер из каналов)
        timers = [ch[4] for ch in missing if len(ch) > 4 and ch[4]]
        if timers:
            min_days = min(timers)
            if min_days > 0:
                asyncio.create_task(_auto_delete_later(uid, sent.message_id, min_days * 86400))
        return False
    return True


# ── рекламные посты ───────────────────────────────────────────────

def get_ad_posts():
    with get_db() as c:
        return c.execute("SELECT * FROM ad_posts ORDER BY id").fetchall()

def get_ad_post(post_id: int):
    with get_db() as c:
        return c.execute("SELECT * FROM ad_posts WHERE id=?", (post_id,)).fetchone()

def add_ad_post(post_type: str, file_id, caption: str, buttons, auto_delete_days: int):
    with get_db() as c:
        c.execute(
            "INSERT INTO ad_posts (type,file_id,caption,buttons,auto_delete_days,created_at) VALUES (?,?,?,?,?,?)",
            (post_type, file_id, caption, buttons, auto_delete_days,
             datetime.now().strftime("%Y-%m-%d %H:%M"))
        )

def update_ad_post(post_id: int, post_type: str, file_id, caption: str, buttons, auto_delete_days: int):
    with get_db() as c:
        c.execute(
            "UPDATE ad_posts SET type=?,file_id=?,caption=?,buttons=?,auto_delete_days=? WHERE id=?",
            (post_type, file_id, caption, buttons, auto_delete_days, post_id)
        )

def delete_ad_post(post_id: int):
    with get_db() as c:
        c.execute("DELETE FROM ad_posts WHERE id=?", (post_id,))

def get_random_ad_post():
    posts = get_ad_posts()
    if not posts:
        return None
    return random.choice(posts)


async def send_ad_post(uid: int, post: tuple):
    """
    Отправляет рекламный пост пользователю.
    post: (id, type, file_id, caption, buttons, auto_delete_days, created_at)
    """
    _, post_type, file_id, caption, buttons_raw, auto_delete_days, _ = post
    kb = parse_buttons(buttons_raw)
    try:
        if post_type == "photo" and file_id:
            sent = await bot.send_photo(uid, file_id, caption=caption or None,
                                        reply_markup=kb, parse_mode="HTML")
        elif post_type == "video" and file_id:
            sent = await bot.send_video(uid, file_id, caption=caption or None,
                                        reply_markup=kb, parse_mode="HTML")
        else:
            sent = await bot.send_message(uid, caption, reply_markup=kb, parse_mode="HTML")

        if auto_delete_days and auto_delete_days > 0:
            asyncio.create_task(_auto_delete_later(uid, sent.message_id, auto_delete_days * 86400))
        # Сохраняем id отправленного сообщения для возможности удалить из панели
        post_id = post[0]
        with get_db() as c:
            c.execute(
                "INSERT INTO sent_ad_messages (post_id,user_id,message_id,sent_at) VALUES (?,?,?,?)",
                (post_id, uid, sent.message_id, datetime.now().strftime("%Y-%m-%d %H:%M"))
            )
        return sent
    except Exception as e:
        logger.warning(f"Ad post send error to {uid}: {e}")
        return None


async def _auto_delete_later(chat_id: int, message_id: int, delay: int):
    await asyncio.sleep(delay)
    try:
        await bot.delete_message(chat_id, message_id)
    except Exception:
        pass


# ── после скачки: рекламный пост + предложение ВИП ───────────────

async def _post_download_ads(uid: int):
    """Отправляет рекламный пост и предложение ВИП. Только не-ВИП."""
    if is_vip(uid):
        return

    ad = get_random_ad_post()
    if ad:
        await send_ad_post(uid, ad)

    await bot.send_message(
        uid,
        f"💡 <i>Хотите без рекламы и без обязательных подписок?\n"
        f"Активируйте ВИП за {VIP_PRICE_STARS} ⭐ звёзд в месяц!</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="👑 Купить ВИП", callback_data="buy_vip")
        ]]),
        parse_mode="HTML"
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      📐  FSM                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

class Download(StatesGroup):
    waiting_url    = State()
    waiting_type   = State()

class AdminState(StatesGroup):
    bc_content          = State()
    bc_buttons          = State()
    bc_confirm          = State()
    give_vip_uid        = State()
    give_vip_days       = State()
    add_channel         = State()
    ban_uid             = State()
    set_vip_price       = State()
    revoke_vip_uid      = State()
    # рекламные посты
    adpost_content      = State()
    adpost_buttons      = State()
    adpost_delete       = State()
    # каналы с таймером
    add_channel_timer   = State()


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      ⌨️  КЛАВИАТУРЫ                            ║
# ╚══════════════════════════════════════════════════════════════════╝

def main_kb(uid):
    rows = [
        [KeyboardButton(text="📥 Скачать"), KeyboardButton(text="👑 ВИП подписка")],
        [KeyboardButton(text="🆘 Поддержка")],
    ]
    if uid in ADMIN_IDS:
        rows.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True)

def download_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎬 Видео",  callback_data="dl_video"),
            InlineKeyboardButton(text="🖼 Превью", callback_data="dl_thumb"),
            InlineKeyboardButton(text="🎵 Звук",   callback_data="dl_audio"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="dl_cancel")]
    ])

def sub_kb(channels):
    btns = [[InlineKeyboardButton(text=f"📢 {ch[2]}", url=ch[3])] for ch in channels]
    btns.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def vip_kb(uid=None):
    rows = []
    if uid and is_vip(uid):
        rows.append([InlineKeyboardButton(
            text="✅ ВИП уже активен",
            callback_data="vip_already_active"
        )])
    else:
        rows.append([InlineKeyboardButton(
            text=f"⭐ Купить ВИП — {VIP_PRICE_STARS} звёзд/мес",
            callback_data="buy_vip"
        )])
    rows.append([InlineKeyboardButton(text="🎁 Реферальная программа", callback_data="ref_program")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика",        callback_data="adm_stats")],
        [InlineKeyboardButton(text="👑 Выдать ВИП",        callback_data="adm_give_vip"),
         InlineKeyboardButton(text="❌ Отобрать ВИП",      callback_data="adm_revoke_vip")],
        [InlineKeyboardButton(text="📢 Рассылка",          callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="📣 Рекламные посты",   callback_data="adm_adposts")],
        [InlineKeyboardButton(text="📺 Обяз. подписки",    callback_data="adm_channels")],
        [InlineKeyboardButton(text="🚫 Бан/Разбан",        callback_data="adm_ban")],
        [InlineKeyboardButton(text="💰 Изменить цену ВИП", callback_data="adm_vip_price")],
    ])

def adm_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️ Назад", callback_data="adm_back")
    ]])

def _adpost_list_kb(posts):
    rows = [[InlineKeyboardButton(text="➕ Добавить пост", callback_data="adp_add")]]
    for p in posts:
        preview  = (p[3] or "")[:28].replace("\n", " ")
        del_ico  = f"🗑{p[5]}д" if p[5] else "♾"
        label    = f"#{p[0]} [{p[1]}] {preview}"
        rows.append([
            InlineKeyboardButton(text=f"✏️ {label}", callback_data=f"adp_edit_{p[0]}"),
            InlineKeyboardButton(text=del_ico,         callback_data=f"adp_del_{p[0]}"),
        ])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm_back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ╔══════════════════════════════════════════════════════════════════╗
# ║              🎬  АНИМАЦИЯ ЗАГРУЗКИ С ПРОГРЕССОМ                 ║
# ╚══════════════════════════════════════════════════════════════════╝

def _build_progress_bar(percent: float, width: int = 10) -> str:
    filled = int(width * percent / 100)
    bar    = "█" * filled + "░" * (width - filled)
    return f"[{bar}] {percent:.0f}%"


class ProgressTracker:
    def __init__(self):
        self.percent: float = 0.0
        self.speed:   str   = ""
        self.eta:     str   = ""

    def hook(self, d: dict):
        if d["status"] == "downloading":
            raw = d.get("_percent_str", "0%").strip().replace("%", "")
            try:
                self.percent = float(raw)
            except ValueError:
                pass
            spd = d.get("_speed_str", "").strip()
            eta = d.get("_eta_str",   "").strip()
            self.speed = spd if spd and spd != "N/A" else ""
            self.eta   = eta if eta and eta != "N/A" else ""
        elif d["status"] == "finished":
            self.percent = 100.0


async def _animate_progress(
    status_msg: Message,
    tracker: ProgressTracker,
    stop_event: asyncio.Event,
    label: str,
):
    FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]
    i = 0
    last_text = ""
    while not stop_event.is_set():
        spin  = FRAMES[i % len(FRAMES)]
        pct   = tracker.percent
        bar   = _build_progress_bar(pct)
        speed = f" • {tracker.speed}" if tracker.speed else ""
        eta   = f" • ETA {tracker.eta}" if tracker.eta else ""
        text  = (
            f"{spin} <b>{label}</b>\n\n"
            f"<code>{bar}</code>\n"
            f"<i>{pct:.0f}%{speed}{eta}</i>"
        )
        if text != last_text:
            try:
                await status_msg.edit_text(text, parse_mode="HTML")
                last_text = text
            except Exception:
                pass
        i += 1
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=1.0)
        except asyncio.TimeoutError:
            pass


async def _safe_delete(msg: Message):
    try:
        await msg.delete()
    except Exception:
        pass


# ╔══════════════════════════════════════════════════════════════════╗
# ║                   🎬  YOUTUBE ФУНКЦИИ                          ║
# ╚══════════════════════════════════════════════════════════════════╝

def is_youtube_url(url: str) -> bool:
    return any(x in url for x in ["youtube.com", "youtu.be", "yt.be"])


async def download_thumbnail(url: str, out_dir: str) -> str | None:
    try:
        ydl_opts = {
            "skip_download": True, "writethumbnail": True,
            "outtmpl": os.path.join(out_dir, "thumb"),
            "quiet": True, "no_warnings": True,
        }
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, lambda: yt_dlp.YoutubeDL(ydl_opts).download([url]))
        for ext in ["jpg", "jpeg", "png", "webp"]:
            path = os.path.join(out_dir, f"thumb.{ext}")
            if os.path.exists(path):
                return path
        for f in os.listdir(out_dir):
            if f.startswith("thumb"):
                return os.path.join(out_dir, f)
        return None
    except Exception as e:
        logger.error(f"Ошибка скачивания превью: {e}")
        return None


async def download_audio(url: str, out_dir: str, tracker: ProgressTracker) -> tuple[str | None, dict]:
    try:
        info = {}
        ydl_opts = {
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            "outtmpl": os.path.join(out_dir, "audio.%(ext)s"),
            "quiet": False, "no_warnings": False, "ignoreerrors": False,
            "progress_hooks": [tracker.hook],
        }
        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=True)
                if data: info.update(data)
        await asyncio.get_event_loop().run_in_executor(None, _dl)
        for f in os.listdir(out_dir):
            full = os.path.join(out_dir, f)
            if os.path.isfile(full):
                return full, info
        return None, info
    except Exception as e:
        logger.error(f"Ошибка скачивания аудио: {e}")
        return None, {}


async def download_video(url: str, out_dir: str, tracker: ProgressTracker) -> tuple[str | None, dict]:
    try:
        info = {}
        ydl_opts = {
            "format": "best[ext=mp4][height<=1080]/best[ext=mp4]/best",
            "outtmpl": os.path.join(out_dir, "video.%(ext)s"),
            "quiet": False, "no_warnings": False, "ignoreerrors": False,
            "progress_hooks": [tracker.hook],
        }
        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=True)
                if data: info.update(data)
        await asyncio.get_event_loop().run_in_executor(None, _dl)
        for f in os.listdir(out_dir):
            full = os.path.join(out_dir, f)
            if os.path.isfile(full):
                return full, info
        return None, info
    except Exception as e:
        logger.error(f"Ошибка скачивания видео: {e}")
        return None, {}


async def get_video_info(url: str) -> dict | None:
    try:
        result = {}
        ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
        def _info():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=False)
                result.update(data or {})
        await asyncio.get_event_loop().run_in_executor(None, _info)
        return result if result else None
    except Exception:
        return None


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      📨  ХЕНДЛЕРЫ                              ║
# ╚══════════════════════════════════════════════════════════════════╝

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    u = msg.from_user
    args = msg.text.split()
    ref_id = None

    if len(args) > 1:
        try:
            ref_id = int(args[1])
            if ref_id == u.id:
                ref_id = None
        except ValueError:
            pass

    is_new = get_user(u.id) is None
    reg_user(u.id, u.username, u.full_name, ref_id)

    if is_new and ref_id:
        referrer = get_user(ref_id)
        if referrer and not referrer[8]:
            inc_ref_count(ref_id)
            name = f"@{u.username}" if u.username else u.full_name
            try:
                await bot.send_message(
                    ref_id,
                    f"🎉 Вы пригласили <b>{name}</b>!\n\n"
                    f"👥 Ваших рефералов: <b>{referrer[5] + 1}</b> / {REF_INVITE_COUNT}\n"
                    f"{'🎁 Ещё ' + str(REF_INVITE_COUNT - referrer[5] - 1) + ' и получите ВИП!' if referrer[5] + 1 < REF_INVITE_COUNT else '✅ Заберите свой ВИП в разделе «ВИП подписка» > «Реферальная программа»'}",
                    parse_mode="HTML"
                )
            except Exception:
                pass

    vip_status = "👑 ВИП активен" if is_vip(u.id) else "👤 Обычный пользователь"
    await msg.answer(
        f"👋 Добро пожаловать в <b>YouTube Downloader</b>!\n\n"
        f"🎬 Скачивай видео, превью и музыку с YouTube!\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 Статус: {vip_status}\n\n"
        f"Выберите действие:",
        reply_markup=main_kb(u.id),
        parse_mode="HTML"
    )


@router.callback_query(F.data == "check_sub")
async def check_sub_cb(call: CallbackQuery):
    missing = await check_subscriptions(call.from_user.id)
    if missing:
        await call.answer("❌ Вы ещё не подписались на все каналы!", show_alert=True)
    else:
        await call.message.delete()
        await call.message.answer(
            "✅ Спасибо за подписку! Теперь можете пользоваться ботом.",
            reply_markup=main_kb(call.from_user.id)
        )


@router.message(F.text == "📥 Скачать")
async def download_start(msg: Message, state: FSMContext):
    if not await require_subscription(msg, msg.from_user.id):
        return
    await state.set_state(Download.waiting_url)
    await msg.answer("🔗 Отправьте ссылку на YouTube видео:", reply_markup=main_kb(msg.from_user.id))


@router.message(Download.waiting_url)
async def download_got_url(msg: Message, state: FSMContext):
    if msg.text in ("📥 Скачать", "👑 ВИП подписка", "🆘 Поддержка", "⚙️ Админ панель"):
        await state.clear()
        if msg.text == "👑 ВИП подписка":   return await vip_menu(msg)
        if msg.text == "🆘 Поддержка":       return await support(msg)
        if msg.text == "⚙️ Админ панель":    return await admin_panel(msg, state)
        return await download_start(msg, state)

    url = msg.text.strip()
    if not is_youtube_url(url):
        return await msg.answer(
            "❌ Это не ссылка YouTube!\n"
            "Отправьте ссылку вида:\n"
            "<code>https://youtube.com/watch?v=...</code>\n"
            "<code>https://youtu.be/...</code>",
            parse_mode="HTML"
        )

    info_msg = await msg.answer("⏳ Получаю информацию о видео...")
    info = await get_video_info(url)

    if not info:
        await _safe_delete(info_msg)
        return await msg.answer(
            "❌ Не удалось получить информацию.\nПроверьте ссылку или попробуйте позже.",
            reply_markup=main_kb(msg.from_user.id)
        )

    await _safe_delete(info_msg)
    title    = info.get("title", "Без названия")
    duration = info.get("duration", 0)
    mins, secs = divmod(duration, 60)

    await state.update_data(url=url, title=title)
    await state.set_state(Download.waiting_type)
    type_msg = await msg.answer(
        f"✅ Видео найдено!\n\n"
        f"📹 <b>{title}</b>\n"
        f"⏱ Длительность: {mins}:{secs:02d}\n\n"
        f"Что хотите скачать?",
        reply_markup=download_type_kb(),
        parse_mode="HTML"
    )
    await state.update_data(type_msg_id=type_msg.message_id)


@router.callback_query(F.data == "dl_cancel")
async def dl_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.delete()
    await call.message.answer("Отменено.", reply_markup=main_kb(call.from_user.id))


@router.callback_query(F.data.in_({"dl_video", "dl_thumb", "dl_audio"}))
async def download_execute(call: CallbackQuery, state: FSMContext):
    data    = await state.get_data()
    url     = data.get("url")
    title   = data.get("title", "Видео")
    dl_type = call.data

    if not url:
        await call.answer("❌ Ссылка потеряна, начните снова", show_alert=True)
        await state.clear()
        return

    await state.clear()
    await _safe_delete(call.message)

    uid         = call.from_user.id
    user_is_vip = is_vip(uid)
    tracker     = ProgressTracker()

    LABELS = {"dl_thumb": "Скачиваю превью", "dl_audio": "Скачиваю аудио", "dl_video": "Скачиваю видео"}
    label      = LABELS[dl_type]
    status_msg = await bot.send_message(
        uid, f"⠋ <b>{label}</b>\n\n<code>[░░░░░░░░░░] 0%</code>", parse_mode="HTML"
    )

    stop_anim = asyncio.Event()
    anim_task = asyncio.create_task(_animate_progress(status_msg, tracker, stop_anim, label))

    tmp = tempfile.mkdtemp(dir=TEMP_DIR)

    # Рекламная подпись только для не-ВИП
    ad_suffix = "" if user_is_vip else BOT_AD_TEXT

    try:
        if dl_type == "dl_thumb":
            path = await download_thumbnail(url, tmp)
            stop_anim.set(); await anim_task
            if not path:
                await status_msg.edit_text("❌ Не удалось скачать превью."); return
            await _safe_delete(status_msg)
            await bot.send_photo(uid, FSInputFile(path),
                                 caption=f"🖼 <b>Превью</b>\n📹 {title}{ad_suffix}",
                                 parse_mode="HTML")

        elif dl_type == "dl_audio":
            path, info = await download_audio(url, tmp, tracker)
            stop_anim.set(); await anim_task
            if not path:
                await status_msg.edit_text("❌ Не удалось скачать аудио."); return
            size_mb = os.path.getsize(path) / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                await status_msg.edit_text(
                    f"❌ <b>Аудио слишком большое</b> ({size_mb:.1f} МБ).\n\n"
                    f"Telegram разрешает боту отправлять максимум <b>{MAX_FILE_SIZE_MB} МБ</b>.\n\n"
                    f"💡 Попробуйте более короткое видео.",
                    parse_mode="HTML"
                )
                return
            await status_msg.edit_text("📤 <b>Отправляю аудио...</b>", parse_mode="HTML")
            duration = info.get("duration", 0)
            await bot.send_audio(uid, FSInputFile(path, filename=os.path.basename(path)),
                                 title=title, duration=int(duration) if duration else None,
                                 caption=f"🎵 <b>{title}</b>{ad_suffix}", parse_mode="HTML")
            await _safe_delete(status_msg)

        elif dl_type == "dl_video":
            path, info = await download_video(url, tmp, tracker)
            stop_anim.set(); await anim_task
            if not path:
                await status_msg.edit_text("❌ Не удалось скачать видео."); return
            size_mb = os.path.getsize(path) / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                await status_msg.edit_text(
                    f"❌ <b>Видео слишком большое</b> ({size_mb:.1f} МБ).\n\n"
                    f"Telegram разрешает боту отправлять максимум <b>{MAX_FILE_SIZE_MB} МБ</b>.\n\n"
                    f"💡 Попробуйте скачать только <b>🎵 аудио</b> — оно обычно намного меньше.",
                    parse_mode="HTML"
                )
                return
            await status_msg.edit_text("📤 <b>Отправляю видео...</b>", parse_mode="HTML")
            duration = info.get("duration", 0)
            await bot.send_video(uid, FSInputFile(path),
                                 duration=int(duration) if duration else None,
                                 caption=f"🎬 <b>{title}</b>{ad_suffix}", parse_mode="HTML")
            await _safe_delete(status_msg)

        inc_downloads(uid, dl_type)
        await _post_download_ads(uid)

    except Exception as e:
        stop_anim.set()
        try: await anim_task
        except Exception: pass
        logger.error(f"Ошибка загрузки: {e}")
        err_str = str(e)
        if "Request Entity Too Large" in err_str or "413" in err_str:
            user_msg = (
                "❌ <b>Файл слишком большой для Telegram.</b>\n\n"
                "Telegram позволяет боту отправлять файлы не более <b>50 МБ</b>.\n\n"
                "💡 Попробуйте скачать <b>аудио</b> — оно обычно намного меньше."
            )
        else:
            user_msg = (
                f"❌ Произошла ошибка при загрузке.\n"
                f"Попробуйте позже или другую ссылку.\n\n"
                f"<code>{err_str[:200]}</code>"
            )
        try:
            await status_msg.edit_text(user_msg, parse_mode="HTML")
        except Exception: pass
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ─── 👑 ВИП подписка ──────────────────────────────────────────────

@router.message(F.text == "👑 ВИП подписка")
async def vip_menu(msg: Message):
    uid        = msg.from_user.id
    vip_active = is_vip(uid)
    expires    = vip_expires_str(uid)
    user       = get_user(uid)
    ref_count  = user[5] if user else 0
    ref_used   = user[6] if user else 0

    status_line = (
        f"✅ Статус: <b>ВИП активен</b>\n⏳ Истекает: {expires}"
        if vip_active else "❌ Статус: <b>Не активен</b>"
    )
    ref_line = (
        "✅ Реферальная акция использована" if ref_used
        else f"👥 Ваши рефералы: <b>{ref_count}/{REF_INVITE_COUNT}</b>"
    )

    await msg.answer(
        f"👑 <b>ВИП подписка</b>\n\n"
        f"{status_line}\n\n"
        f"<b>Что даёт ВИП:</b>\n"
        f"• 🚫 Никакой рекламы в подписях\n"
        f"• 📣 Никаких рекламных постов после скачки\n"
        f"• 📢 Никаких обязательных подписок\n"
        f"• 🔕 Никаких рассылок\n\n"
        f"💰 Цена: <b>{VIP_PRICE_STARS} ⭐ звёзд / месяц</b>\n\n"
        f"🎁 <b>Бесплатный ВИП:</b>\n"
        f"Пригласи {REF_INVITE_COUNT} друзей → {REF_VIP_DAYS} дней ВИП бесплатно\n"
        f"{ref_line}",
        reply_markup=vip_kb(uid),
        parse_mode="HTML"
    )


@router.callback_query(F.data == "vip_already_active")
async def vip_already_active_cb(call: CallbackQuery):
    expires = vip_expires_str(call.from_user.id)
    await call.answer(f"✅ У вас уже активен ВИП!\nДействует: {expires}", show_alert=True)


@router.callback_query(F.data == "buy_vip")
async def buy_vip_cb(call: CallbackQuery):
    uid = call.from_user.id
    if is_vip(uid):
        expires = vip_expires_str(uid)
        await call.answer(f"✅ У вас уже активен ВИП!\nДействует: {expires}", show_alert=True)
        return
    prices = [LabeledPrice(label="ВИП подписка на месяц", amount=VIP_PRICE_STARS)]
    await bot.send_invoice(
        chat_id=uid, title="👑 ВИП подписка",
        description=f"ВИП на {VIP_DAYS} дней — без рекламы и обязательных подписок",
        payload=f"vip_{uid}_{VIP_DAYS}", currency="XTR", prices=prices,
    )
    await call.answer()


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def payment_done(msg: Message):
    uid = msg.from_user.id
    with get_db() as c:
        c.execute(
            "INSERT INTO stars_payments (user_id,stars,payload,created_at) VALUES (?,?,?,?)",
            (uid, msg.successful_payment.total_amount,
             msg.successful_payment.invoice_payload, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )
    expires = give_vip(uid, VIP_DAYS)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    await msg.answer(
        f"🎉 <b>ВИП активирован!</b>\n\n⏳ Действует до: <b>{exp_str}</b>\n\nНаслаждайтесь без рекламы! 🚀",
        reply_markup=main_kb(uid), parse_mode="HTML"
    )


@router.callback_query(F.data == "ref_program")
async def ref_program(call: CallbackQuery):
    uid  = call.from_user.id
    user = get_user(uid)
    ref_count = user[5] if user else 0
    ref_used  = user[6] if user else 0
    link = await create_start_link(bot, str(uid), encode=False)

    if ref_used:
        text = f"🎁 <b>Реферальная программа</b>\n\n✅ Вы уже использовали эту акцию.\n\n🔗 Ваша ссылка:\n<code>{link}</code>"
    else:
        need = REF_INVITE_COUNT - ref_count
        text = (
            f"🎁 <b>Реферальная программа</b>\n\n"
            f"👥 Приглашено: <b>{ref_count}/{REF_INVITE_COUNT}</b>\n"
            f"{'🟢 Осталось: ' + str(need) + ' чел.' if need > 0 else '✅ Цель достигнута! Заберите ВИП!'}\n\n"
            f"📌 Пригласите {REF_INVITE_COUNT} друзей → получите ВИП на {REF_VIP_DAYS} дней!\n"
            f"⚠️ Акция работает только 1 раз\n\n"
            f"🔗 Ваша ссылка:\n<code>{link}</code>"
        )

    btns = [[InlineKeyboardButton(text="📤 Поделиться",
             url=f"https://t.me/share/url?url={link}&text=Качай+видео+с+YouTube+в+Telegram!")]]
    if not ref_used and ref_count >= REF_INVITE_COUNT:
        btns.insert(0, [InlineKeyboardButton(text="🎁 Забрать ВИП!", callback_data="claim_ref_vip")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=btns), parse_mode="HTML")


@router.callback_query(F.data == "claim_ref_vip")
async def claim_ref_vip(call: CallbackQuery):
    uid  = call.from_user.id
    user = get_user(uid)
    if not user:               return await call.answer("Ошибка", show_alert=True)
    if user[6]:                return await call.answer("❌ Вы уже использовали эту акцию!", show_alert=True)
    if user[5] < REF_INVITE_COUNT:
        return await call.answer(f"❌ Нужно ещё {REF_INVITE_COUNT - user[5]} рефералов!", show_alert=True)
    if is_vip(uid):
        expires = vip_expires_str(uid)
        return await call.answer(
            f"✅ У вас уже активен ВИП!\n"
            f"Действует: {expires}\n\n"
            f"Приз можно получить только когда ВИП не активен.",
            show_alert=True
        )

    set_ref_used(uid)
    expires = give_vip(uid, REF_VIP_DAYS)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    await call.answer("🎉 ВИП активирован!", show_alert=True)
    await call.message.edit_text(f"🎉 <b>ВИП получен за рефералов!</b>\n\n⏳ Действует до: <b>{exp_str}</b>", parse_mode="HTML")


@router.message(F.text == "🆘 Поддержка")
async def support(msg: Message):
    await msg.answer(
        f"🆘 <b>Поддержка</b>\n\nПо всем вопросам:\n👨‍💼 {SUPPORT_USERNAME}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✉️ Написать", url=f"https://t.me/{SUPPORT_USERNAME.lstrip('@')}")
        ]]),
        parse_mode="HTML"
    )


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      ⚙️  АДМИН ПАНЕЛЬ                         ║
# ╚══════════════════════════════════════════════════════════════════╝

@router.message(F.text == "⚙️ Админ панель")
async def admin_panel(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    total, vip_cnt, dls, new = get_stats()
    await msg.answer(
        f"⚙️ <b>Админ панель</b>\n\n"
        f"👥 Пользователей: {total}  |  Новых сегодня: {new}\n"
        f"👑 ВИП активных: {vip_cnt}\n📥 Всего скачиваний: {dls}",
        reply_markup=admin_kb(), parse_mode="HTML"
    )


@router.callback_query(F.data == "adm_back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.clear()
    total, vip_cnt, dls, new = get_stats()
    await call.message.edit_text(
        f"⚙️ <b>Админ панель</b>\n\n"
        f"👥 Пользователей: {total}  |  Новых сегодня: {new}\n"
        f"👑 ВИП активных: {vip_cnt}\n📥 Всего скачиваний: {dls}",
        reply_markup=admin_kb(), parse_mode="HTML"
    )


@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    with get_db() as c:
        total    = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        banned   = c.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]
        vip_cnt  = c.execute("SELECT COUNT(*) FROM vip WHERE expires_at > ?", (datetime.now().strftime("%Y-%m-%d %H:%M"),)).fetchone()[0]
        # Скачивания всех пользователей из лога
        dls_all  = c.execute("SELECT COUNT(*) FROM downloads_log").fetchone()[0] or 0
        dls_vid  = c.execute("SELECT COUNT(*) FROM downloads_log WHERE dl_type='dl_video'").fetchone()[0] or 0
        dls_aud  = c.execute("SELECT COUNT(*) FROM downloads_log WHERE dl_type='dl_audio'").fetchone()[0] or 0
        dls_thm  = c.execute("SELECT COUNT(*) FROM downloads_log WHERE dl_type='dl_thumb'").fetchone()[0] or 0
        # Старые записи без лога (в users.downloads)
        dls_users = c.execute("SELECT SUM(downloads) FROM users").fetchone()[0] or 0
        stars    = c.execute("SELECT SUM(stars) FROM stars_payments").fetchone()[0] or 0
        today    = datetime.now().strftime("%Y-%m-%d")
        new      = c.execute("SELECT COUNT(*) FROM users WHERE joined_at LIKE ?", (f"{today}%",)).fetchone()[0]
        dls_today = c.execute("SELECT COUNT(*) FROM downloads_log WHERE created_at LIKE ?", (f"{today}%",)).fetchone()[0] or 0
        chans    = c.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
        ad_cnt   = c.execute("SELECT COUNT(*) FROM ad_posts").fetchone()[0]
        uniq_dls = c.execute("SELECT COUNT(DISTINCT user_id) FROM downloads_log").fetchone()[0] or 0
    await call.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Всего: {total}  |  Сегодня: +{new}\n"
        f"🚫 Заблокировано: {banned}\n"
        f"👑 ВИП активных: {vip_cnt}\n\n"
        f"📥 <b>Скачивания (все пользователи):</b>\n"
        f"  Всего: {dls_users} | Сегодня: {dls_today}\n"
        f"  🎬 Видео: {dls_vid}  🎵 Аудио: {dls_aud}  🖼 Превью: {dls_thm}\n"
        f"  👤 Уникальных юзеров качали: {uniq_dls}\n\n"
        f"⭐ Заработано звёзд: {stars}\n"
        f"📺 Обяз. каналов: {chans}\n"
        f"📣 Рекламных постов: {ad_cnt}",
        reply_markup=adm_back_kb(), parse_mode="HTML"
    )


@router.callback_query(F.data == "adm_give_vip")
async def adm_give_vip_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.give_vip_uid)
    await call.message.edit_text("👑 <b>Выдать ВИП</b>\n\nВведите ID пользователя:", reply_markup=adm_back_kb(), parse_mode="HTML")


@router.message(AdminState.give_vip_uid)
async def adm_give_vip_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❌ Введите числовой ID!")
    user = get_user(uid)
    if not user: return await msg.answer("❌ Пользователь не найден!")
    await state.update_data(vip_target=uid)
    await state.set_state(AdminState.give_vip_days)
    await msg.answer(f"👤 {user[2]} (ID: {uid})\n⏳ ВИП: {vip_expires_str(uid)}\n\nВведите кол-во дней:")


@router.message(AdminState.give_vip_days)
async def adm_give_vip_days(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try:
        days = int(msg.text.strip())
        if days <= 0: raise ValueError
    except ValueError: return await msg.answer("❌ Введите положительное число дней!")
    data = await state.get_data()
    uid = data["vip_target"]
    expires = give_vip(uid, days)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    await state.clear()
    try: await bot.send_message(uid, f"👑 Вам выдан ВИП!\n⏳ До: <b>{exp_str}</b>", parse_mode="HTML")
    except: pass
    await msg.answer(f"✅ ВИП выдан {uid}\n⏳ До: {exp_str}", reply_markup=main_kb(msg.from_user.id))


@router.callback_query(F.data == "adm_revoke_vip")
async def adm_revoke_vip_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.revoke_vip_uid)
    await call.message.edit_text("❌ <b>Отобрать ВИП</b>\n\nВведите ID пользователя:", reply_markup=adm_back_kb(), parse_mode="HTML")


@router.message(AdminState.revoke_vip_uid)
async def adm_revoke_vip_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❌ Введите числовой ID!")
    user = get_user(uid)
    if not user: return await msg.answer("❌ Пользователь не найден!")
    if not is_vip(uid):
        await state.clear()
        return await msg.answer(f"⚠️ У пользователя <b>{uid}</b> нет активного ВИП.", reply_markup=main_kb(msg.from_user.id), parse_mode="HTML")
    remove_vip(uid)
    await state.clear()
    try: await bot.send_message(uid, "❌ <b>Ваш ВИП был отозван администратором.</b>", parse_mode="HTML")
    except: pass
    await msg.answer(f"✅ ВИП отобран у <b>{uid}</b>\n👤 {user[2]}", reply_markup=main_kb(msg.from_user.id), parse_mode="HTML")


@router.callback_query(F.data == "adm_vip_price")
async def adm_vip_price(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.set_vip_price)
    await call.message.edit_text(f"💰 <b>Изменить цену ВИП</b>\n\nТекущая: <b>{VIP_PRICE_STARS} ⭐</b>\n\nНовая цена:", reply_markup=adm_back_kb(), parse_mode="HTML")


@router.message(AdminState.set_vip_price)
async def adm_set_vip_price(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try:
        new_price = int(msg.text.strip())
        if new_price <= 0: raise ValueError
    except ValueError: return await msg.answer("❌ Введите положительное целое число!")
    global VIP_PRICE_STARS
    VIP_PRICE_STARS = new_price
    await state.clear()
    await msg.answer(f"✅ Цена изменена на <b>{new_price} ⭐</b>", reply_markup=main_kb(msg.from_user.id), parse_mode="HTML")


# ── Рассылка ─────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_broadcast")
async def adm_bc_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.bc_content)
    await call.message.edit_text(
        "📢 <b>Рассылка — шаг 1/2</b>\n\nОтправьте сообщение.\nМожно: текст, фото, видео.\n\n⚠️ ВИП пользователи <b>не получат</b>.",
        reply_markup=adm_back_kb(), parse_mode="HTML"
    )


@router.message(AdminState.bc_content)
async def adm_bc_content(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.photo:    bc_type, file_id, caption = "photo", msg.photo[-1].file_id, msg.caption or ""
    elif msg.video:  bc_type, file_id, caption = "video", msg.video.file_id,     msg.caption or ""
    elif msg.text:   bc_type, file_id, caption = "text",  None,                  msg.text
    else:            return await msg.answer("❌ Поддерживаются: текст, фото, видео.")
    await state.update_data(bc_type=bc_type, bc_file_id=file_id, bc_caption=caption)
    await state.set_state(AdminState.bc_buttons)
    await msg.answer(
        "📢 <b>Рассылка — шаг 2/2</b>\n\nДобавьте кнопки (необязательно).\n"
        "Формат: <code>Текст|https://ссылка</code>\n\nКаждая кнопка на новой строке.\nИли <b>нет</b>.",
        reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="нет")]], resize_keyboard=True),
        parse_mode="HTML"
    )


@router.message(AdminState.bc_buttons)
async def adm_bc_buttons(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    buttons_raw = None if msg.text.strip().lower() in ("нет", "/skip", "skip") else msg.text
    await state.update_data(bc_buttons=buttons_raw)
    data      = await state.get_data()
    non_vip   = [u for u in all_user_ids() if not is_vip(u)]
    await state.set_state(AdminState.bc_confirm)
    await msg.answer(
        f"📢 <b>Предпросмотр</b>\n\n📝 Тип: {data['bc_type']}\n🔘 Кнопки: {'есть' if buttons_raw else 'нет'}\n👥 Получателей: {len(non_vip)}\n\nНачать рассылку?",
        reply_markup=ReplyKeyboardRemove(), parse_mode="HTML"
    )
    await msg.answer("Подтвердите:", reply_markup=InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Начать", callback_data="bc_go")],
        [InlineKeyboardButton(text="❌ Отмена",  callback_data="bc_cancel")],
    ]))


def parse_buttons(raw: str | None) -> InlineKeyboardMarkup | None:
    if not raw: return None
    rows = []
    for line in raw.strip().splitlines():
        if "|" in line:
            label, url = line.split("|", 1)
            rows.append([InlineKeyboardButton(text=label.strip(), url=url.strip())])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


@router.callback_query(F.data == "bc_go")
async def bc_go(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    data = await state.get_data()
    await state.clear()
    bc_type = data.get("bc_type"); file_id = data.get("bc_file_id")
    caption = data.get("bc_caption", ""); kb = parse_buttons(data.get("bc_buttons"))
    all_users = [u for u in all_user_ids() if not is_vip(u)]
    await call.message.edit_text(f"⏳ Рассылаю {len(all_users)} пользователям...")
    ok, fail = 0, 0
    for uid in all_users:
        try:
            if bc_type == "photo":   await bot.send_photo(uid, file_id, caption=caption, reply_markup=kb, parse_mode="HTML")
            elif bc_type == "video": await bot.send_video(uid, file_id, caption=caption, reply_markup=kb, parse_mode="HTML")
            else:                    await bot.send_message(uid, caption, reply_markup=kb, parse_mode="HTML")
            ok += 1
        except Exception: fail += 1
        await asyncio.sleep(0.04)
    await call.message.edit_text(f"✅ Рассылка завершена!\n📨 {ok}/{len(all_users)}  ❌ {fail}", reply_markup=adm_back_kb())
    await call.message.answer("Меню:", reply_markup=main_kb(call.from_user.id))


@router.callback_query(F.data == "bc_cancel")
async def bc_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ Рассылка отменена.", reply_markup=admin_kb())


# ── Обязательные каналы ───────────────────────────────────────────

@router.callback_query(F.data == "adm_channels")
async def adm_channels(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    chs  = get_channels()
    text = "📺 <b>Обязательные подписки</b>\n\n"
    if chs:
        lines = []
        for ch in chs:
            timer_str = f" 🗑{ch[4]}д" if len(ch) > 4 and ch[4] else " ♾"
            lines.append(f"• {ch[2]}  ({ch[1]}){timer_str}")
        text += "\n".join(lines)
    else:
        text += "Каналы не добавлены."
    rows = [[InlineKeyboardButton(text="➕ Добавить", callback_data="ch_add")]]
    for ch in chs:
        rows.append([InlineKeyboardButton(text=f"🗑 {ch[2]}", callback_data=f"ch_del_{ch[0]}")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm_back")])
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@router.callback_query(F.data == "ch_add")
async def ch_add_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.add_channel)
    await call.message.answer(
        "➕ <b>Добавить канал — шаг 1/2</b>\n\n"
        "Введите данные канала:\n"
        "<code>@channel_id|Название|https://t.me/channel</code>\n\n"
        "⚠️ Бот должен быть администратором в канале!",
        parse_mode="HTML"
    )


@router.message(AdminState.add_channel)
async def ch_add_handler(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    parts = msg.text.split("|")
    if len(parts) != 3:
        return await msg.answer("❌ Формат: @channel|Название|https://ссылка")
    await state.update_data(
        ch_id=parts[0].strip(),
        ch_title=parts[1].strip(),
        ch_link=parts[2].strip()
    )
    await state.set_state(AdminState.add_channel_timer)
    await msg.answer(
        "➕ <b>Добавить канал — шаг 2/2</b>\n\n"
        "Через сколько <b>дней</b> автоматически удалять сообщение\n"
        "«подпишитесь на каналы» у пользователя?\n\n"
        "• <code>0</code> — не удалять\n"
        "• <code>1</code> — через 1 день\n"
        "• <code>3</code> — через 3 дня\n"
        "• <code>7</code> — через 7 дней",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="0"), KeyboardButton(text="1"),
                       KeyboardButton(text="3"), KeyboardButton(text="7")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )


@router.message(AdminState.add_channel_timer)
async def ch_add_timer_handler(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try:
        days = int(msg.text.strip())
        if days < 0: raise ValueError
    except ValueError:
        return await msg.answer("❌ Введите 0 или количество дней (целое ≥ 0).")
    data = await state.get_data()
    add_channel(data["ch_id"], data["ch_title"], data["ch_link"], days)
    await state.clear()
    timer_text = f"🗑 Автоудаление через {days} дн." if days > 0 else "♾ Без автоудаления"
    await msg.answer(
        f"✅ Канал <b>{data['ch_title']}</b> добавлен!\n{timer_text}",
        reply_markup=main_kb(msg.from_user.id),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("ch_del_"))
async def ch_del(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    del_channel(int(call.data.split("_")[2]))
    await call.answer("✅ Канал удалён!")
    await adm_channels(call)


@router.callback_query(F.data == "adm_ban")
async def adm_ban_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.ban_uid)
    await call.message.edit_text("🚫 <b>Бан/Разбан</b>\n\nВведите ID:", reply_markup=adm_back_kb(), parse_mode="HTML")


@router.message(AdminState.ban_uid)
async def adm_ban_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    try: uid = int(msg.text.strip())
    except ValueError: return await msg.answer("❌ Введите числовой ID!")
    new_status = ban_toggle(uid)
    await state.clear()
    if new_status == 1:
        action = "🚫 Заблокирован"
        try: await bot.send_message(uid, "🚫 Ваш аккаунт заблокирован.")
        except: pass
    elif new_status == 0:
        action = "✅ Разблокирован"
        try: await bot.send_message(uid, "✅ Ваш аккаунт разблокирован.")
        except: pass
    else: action = "❌ Пользователь не найден"
    await msg.answer(f"{action}: {uid}", reply_markup=main_kb(msg.from_user.id))


# ╔══════════════════════════════════════════════════════════════════╗
# ║                   📣  РЕКЛАМНЫЕ ПОСТЫ (ADMIN)                  ║
# ╚══════════════════════════════════════════════════════════════════╝

@router.callback_query(F.data == "adm_adposts")
async def adm_adposts(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.clear()
    posts = get_ad_posts()
    text = (
        f"📣 <b>Рекламные посты</b>\n\n"
        f"Всего: <b>{len(posts)}</b> шт.\n"
        f"После каждой скачки случайный пост отправляется не-ВИП пользователям.\n\n"
        f"🗑<i>N</i>д — автоудаление через N дней\n"
        f"♾ — не удаляется"
    )
    await call.message.edit_text(text, reply_markup=_adpost_list_kb(posts), parse_mode="HTML")


# ── Добавить / редактировать — шаг 1: контент ────────────────────

@router.callback_query(F.data == "adp_add")
async def adp_add_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    await state.set_state(AdminState.adpost_content)
    await state.update_data(adp_mode="add", adp_edit_id=None)
    await call.message.answer(
        "📣 <b>Новый рекламный пост — шаг 1/3</b>\n\n"
        "Отправьте содержимое поста:\n"
        "текст, фото с подписью или видео с подписью.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True
        ),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("adp_edit_"))
async def adp_edit_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS: return
    post_id = int(call.data.split("_")[2])
    post    = get_ad_post(post_id)
    if not post:
        await call.answer("❌ Пост не найден!", show_alert=True); return

    _, post_type, file_id, caption, buttons, auto_del, _ = post
    del_text  = f"{auto_del} дн." if auto_del else "нет"
    btns_text = buttons if buttons else "нет"

    await call.message.answer(
        f"✏️ <b>Редактировать пост #{post_id}</b>\n\n"
        f"Тип: {post_type}\n"
        f"Подпись:\n<code>{(caption or '')[:300]}</code>\n\n"
        f"Кнопки:\n<code>{btns_text[:200]}</code>\n\n"
        f"Автоудаление: {del_text}\n\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"Отправьте новое содержимое поста:",
        parse_mode="HTML",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="❌ Отмена")]], resize_keyboard=True
        )
    )
    await state.set_state(AdminState.adpost_content)
    await state.update_data(adp_mode="edit", adp_edit_id=post_id)


@router.message(AdminState.adpost_content)
async def adp_got_content(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=main_kb(msg.from_user.id))
        return

    if msg.photo:    post_type, file_id, caption = "photo", msg.photo[-1].file_id, msg.caption or ""
    elif msg.video:  post_type, file_id, caption = "video", msg.video.file_id,     msg.caption or ""
    elif msg.text:   post_type, file_id, caption = "text",  None,                  msg.text
    else:            return await msg.answer("❌ Поддерживаются: текст, фото, видео.")

    await state.update_data(adp_type=post_type, adp_file_id=file_id, adp_caption=caption)
    await state.set_state(AdminState.adpost_buttons)
    await msg.answer(
        "📣 <b>Шаг 2/3 — Кнопки</b>\n\n"
        "Добавьте inline-кнопки (необязательно).\n"
        "Каждая кнопка на новой строке:\n"
        "<code>Текст кнопки|https://ссылка.com</code>\n\n"
        "Или напишите <b>нет</b>.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="нет")], [KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )


@router.message(AdminState.adpost_buttons)
async def adp_got_buttons(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=main_kb(msg.from_user.id))
        return

    buttons_raw = None if msg.text.strip().lower() in ("нет", "no") else msg.text
    await state.update_data(adp_buttons=buttons_raw)
    await state.set_state(AdminState.adpost_delete)
    await msg.answer(
        "📣 <b>Шаг 3/3 — Автоудаление</b>\n\n"
        "Через сколько <b>дней</b> удалять пост у пользователя?\n\n"
        "• <code>0</code> — не удалять\n"
        "• <code>1</code> — через 1 день\n"
        "• <code>3</code> — через 3 дня\n"
        "• <code>7</code> — через 7 дней\n"
        "• <code>30</code> — через 30 дней",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="0"), KeyboardButton(text="1"),
                       KeyboardButton(text="3"), KeyboardButton(text="7")],
                      [KeyboardButton(text="❌ Отмена")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )


@router.message(AdminState.adpost_delete)
async def adp_got_delete(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    if msg.text == "❌ Отмена":
        await state.clear()
        await msg.answer("Отменено.", reply_markup=main_kb(msg.from_user.id))
        return

    try:
        auto_del = int(msg.text.strip())
        if auto_del < 0: raise ValueError
    except ValueError:
        return await msg.answer("❌ Введите 0 или количество дней (целое ≥ 0).")

    data      = await state.get_data()
    mode      = data.get("adp_mode", "add")
    edit_id   = data.get("adp_edit_id")
    post_type = data["adp_type"]
    file_id   = data.get("adp_file_id")
    caption   = data["adp_caption"]
    buttons   = data.get("adp_buttons")
    await state.clear()

    if mode == "edit" and edit_id:
        update_ad_post(edit_id, post_type, file_id, caption, buttons, auto_del)
        action_text = f"✅ Рекламный пост #{edit_id} обновлён!"
    else:
        add_ad_post(post_type, file_id, caption, buttons, auto_del)
        action_text = "✅ Рекламный пост добавлен!"

    del_text = f"🗑 Автоудаление: через {auto_del} дн." if auto_del > 0 else "🗑 Автоудаление: отключено"
    await msg.answer(
        f"{action_text}\n\n"
        f"📝 Тип: {post_type}\n"
        f"🔘 Кнопки: {'есть' if buttons else 'нет'}\n"
        f"{del_text}",
        reply_markup=main_kb(msg.from_user.id),
        parse_mode="HTML"
    )


@router.callback_query(F.data.startswith("adp_del_"))
async def adp_delete(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    post_id = int(call.data.split("_")[2])
    # Спрашиваем — удалить только из базы или ещё и у всех пользователей
    with get_db() as c:
        sent_cnt = c.execute("SELECT COUNT(*) FROM sent_ad_messages WHERE post_id=?", (post_id,)).fetchone()[0]
    await call.message.answer(
        f"🗑 <b>Удалить рекламный пост #{post_id}?</b>\n\n"
        f"Этот пост был отправлен <b>{sent_cnt}</b> пользователям.\n\n"
        f"Выберите действие:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑 Удалить из базы", callback_data=f"adp_deldb_{post_id}")],
            [InlineKeyboardButton(text="🗑💬 Удалить + отозвать у юзеров", callback_data=f"adp_delall_{post_id}")],
            [InlineKeyboardButton(text="❌ Отмена", callback_data="adm_adposts")],
        ]),
        parse_mode="HTML"
    )
    await call.answer()


@router.callback_query(F.data.startswith("adp_deldb_"))
async def adp_deldb(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    post_id = int(call.data.split("_")[2])
    with get_db() as c:
        c.execute("DELETE FROM sent_ad_messages WHERE post_id=?", (post_id,))
    delete_ad_post(post_id)
    await call.answer(f"✅ Пост #{post_id} удалён из базы!")
    posts = get_ad_posts()
    text  = f"📣 <b>Рекламные посты</b>\n\nВсего: <b>{len(posts)}</b> шт."
    try:
        await call.message.edit_text(text, reply_markup=_adpost_list_kb(posts), parse_mode="HTML")
    except Exception:
        pass


@router.callback_query(F.data.startswith("adp_delall_"))
async def adp_delall(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS: return
    post_id = int(call.data.split("_")[2])
    # Собираем все отправленные сообщения
    with get_db() as c:
        rows = c.execute(
            "SELECT user_id, message_id FROM sent_ad_messages WHERE post_id=?", (post_id,)
        ).fetchall()
    await call.answer("⏳ Удаляю у пользователей...")
    ok, fail = 0, 0
    for user_id, message_id in rows:
        try:
            await bot.delete_message(user_id, message_id)
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.03)
    # Удаляем из БД
    with get_db() as c:
        c.execute("DELETE FROM sent_ad_messages WHERE post_id=?", (post_id,))
    delete_ad_post(post_id)
    posts = get_ad_posts()
    text  = (
        f"✅ Пост #{post_id} удалён!\n"
        f"💬 Удалено у пользователей: {ok}  ❌ Ошибок: {fail}\n\n"
        f"📣 <b>Рекламные посты</b>\n\nВсего: <b>{len(posts)}</b> шт."
    )
    try:
        await call.message.edit_text(text, reply_markup=_adpost_list_kb(posts), parse_mode="HTML")
    except Exception:
        pass


# ─── Фоновая задача — снятие истёкших ВИП ────────────────────────

async def vip_cleanup_loop():
    while True:
        try:
            with get_db() as c:
                c.execute("DELETE FROM vip WHERE expires_at <= ?", (datetime.now().strftime("%Y-%m-%d %H:%M"),))
        except Exception as e:
            logger.error(f"VIP cleanup error: {e}")
        await asyncio.sleep(3600)


# ╔══════════════════════════════════════════════════════════════════╗
# ║                         🚀  ЗАПУСК                             ║
# ╚══════════════════════════════════════════════════════════════════╝

async def main():
    init_db()
    logger.info("✅ Бот запущен!")
    asyncio.create_task(vip_cleanup_loop())
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
