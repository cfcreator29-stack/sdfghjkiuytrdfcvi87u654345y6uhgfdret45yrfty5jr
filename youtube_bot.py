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

MAX_FILE_SIZE_MB = 50

# ═══════════════════════════════════════════════════════════════════


import asyncio
import logging
import sqlite3
import os
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
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                channel_id  TEXT,
                title       TEXT,
                link        TEXT
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

def inc_downloads(uid):
    with get_db() as c:
        c.execute("UPDATE users SET downloads=downloads+1 WHERE user_id=?", (uid,))

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
    """Выдать или продлить VIP"""
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
    """Отобрать ВИП у пользователя"""
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

def add_channel(channel_id, title, link):
    with get_db() as c:
        c.execute("INSERT INTO channels (channel_id,title,link) VALUES (?,?,?)",
                  (channel_id, title, link))

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


# ╔══════════════════════════════════════════════════════════════════╗
# ║                      📐  FSM                                    ║
# ╚══════════════════════════════════════════════════════════════════╝

class Download(StatesGroup):
    waiting_url    = State()
    waiting_type   = State()

class AdminState(StatesGroup):
    bc_content      = State()
    bc_buttons      = State()
    bc_confirm      = State()
    give_vip_uid    = State()
    give_vip_days   = State()
    add_channel     = State()
    ban_uid         = State()
    set_vip_price   = State()
    revoke_vip_uid  = State()   # ← отобрать ВИП


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

def cancel_kb():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def download_type_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🎬 Видео",    callback_data="dl_video"),
            InlineKeyboardButton(text="🖼 Превью",   callback_data="dl_thumb"),
            InlineKeyboardButton(text="🎵 Звук",     callback_data="dl_audio"),
        ],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="dl_cancel")]
    ])

def sub_kb(channels):
    btns = [[InlineKeyboardButton(text=f"📢 {ch[2]}", url=ch[3])] for ch in channels]
    btns.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=btns)

def vip_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"⭐ Купить ВИП — {VIP_PRICE_STARS} звёзд/мес",
            callback_data="buy_vip"
        )],
        [InlineKeyboardButton(text="🎁 Реферальная программа", callback_data="ref_program")],
    ])

def admin_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика",         callback_data="adm_stats")],
        [InlineKeyboardButton(text="👑 Выдать ВИП",         callback_data="adm_give_vip"),
         InlineKeyboardButton(text="❌ Отобрать ВИП",       callback_data="adm_revoke_vip")],
        [InlineKeyboardButton(text="📢 Рассылка",           callback_data="adm_broadcast")],
        [InlineKeyboardButton(text="📺 Обяз. подписки",     callback_data="adm_channels")],
        [InlineKeyboardButton(text="🚫 Бан/Разбан",         callback_data="adm_ban")],
        [InlineKeyboardButton(text="💰 Изменить цену ВИП",  callback_data="adm_vip_price")],
    ])


# ╔══════════════════════════════════════════════════════════════════╗
# ║                   🎬  YOUTUBE ФУНКЦИИ                          ║
# ╚══════════════════════════════════════════════════════════════════╝

def is_youtube_url(url: str) -> bool:
    return any(x in url for x in ["youtube.com", "youtu.be", "yt.be"])

async def download_thumbnail(url: str, out_dir: str) -> str | None:
    try:
        ydl_opts = {
            "skip_download": True,
            "writethumbnail": True,
            "outtmpl": os.path.join(out_dir, "thumb"),
            "quiet": True,
            "no_warnings": True,
        }
        loop = asyncio.get_event_loop()
        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                ydl.download([url])
        await loop.run_in_executor(None, _dl)

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

async def download_audio(url: str, out_dir: str) -> tuple[str | None, dict]:
    try:
        ydl_opts = {
            "format": "bestaudio[ext=m4a]/bestaudio[ext=webm]/bestaudio/best",
            "outtmpl": os.path.join(out_dir, "audio.%(ext)s"),
            "quiet": False,
            "no_warnings": False,
            "ignoreerrors": False,
        }
        info = {}
        loop = asyncio.get_event_loop()

        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=True)
                if data:
                    info.update(data)
        await loop.run_in_executor(None, _dl)

        for f in os.listdir(out_dir):
            full = os.path.join(out_dir, f)
            if os.path.isfile(full):
                return full, info
        return None, info
    except Exception as e:
        logger.error(f"Ошибка скачивания аудио: {e}")
        return None, {}

async def download_video(url: str, out_dir: str) -> tuple[str | None, dict]:
    try:
        ydl_opts = {
            "format": "best[ext=mp4][height<=720]/best[ext=mp4]/best",
            "outtmpl": os.path.join(out_dir, "video.%(ext)s"),
            "quiet": False,
            "no_warnings": False,
            "ignoreerrors": False,
        }
        info = {}
        loop = asyncio.get_event_loop()

        def _dl():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=True)
                if data:
                    info.update(data)
        await loop.run_in_executor(None, _dl)

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
        ydl_opts = {"quiet": True, "no_warnings": True, "skip_download": True}
        loop = asyncio.get_event_loop()
        result = {}
        def _info():
            with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                data = ydl.extract_info(url, download=False)
                result.update(data or {})
        await loop.run_in_executor(None, _info)
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


async def require_subscription(msg_or_call, uid):
    if is_vip(uid):
        return True
    missing = await check_subscriptions(uid)
    if missing:
        text = "📢 Для использования бота подпишитесь на наши каналы:"
        if isinstance(msg_or_call, Message):
            await msg_or_call.answer(text, reply_markup=sub_kb(missing))
        else:
            await msg_or_call.message.answer(text, reply_markup=sub_kb(missing))
        return False
    return True


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
    await msg.answer(
        "🔗 Отправьте ссылку на YouTube видео:",
        reply_markup=main_kb(msg.from_user.id)
    )


@router.message(Download.waiting_url)
async def download_got_url(msg: Message, state: FSMContext):
    if msg.text in ("📥 Скачать", "👑 ВИП подписка", "🆘 Поддержка", "⚙️ Админ панель"):
        await state.clear()
        if msg.text == "👑 ВИП подписка":
            return await vip_menu(msg)
        if msg.text == "🆘 Поддержка":
            return await support(msg)
        if msg.text == "⚙️ Админ панель":
            return await admin_panel(msg, state)
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
        await info_msg.delete()
        return await msg.answer(
            "❌ Не удалось получить информацию.\n"
            "Проверьте ссылку или попробуйте позже.",
            reply_markup=main_kb(msg.from_user.id)
        )

    await info_msg.delete()
    title    = info.get("title", "Без названия")
    duration = info.get("duration", 0)
    mins     = duration // 60
    secs     = duration % 60

    await state.update_data(url=url, title=title)
    await state.set_state(Download.waiting_type)
    await msg.answer(
        f"✅ Видео найдено!\n\n"
        f"📹 <b>{title}</b>\n"
        f"⏱ Длительность: {mins}:{secs:02d}\n\n"
        f"Что хотите скачать?",
        reply_markup=download_type_kb(),
        parse_mode="HTML"
    )


@router.callback_query(F.data == "dl_cancel")
async def dl_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.delete()
    await call.message.answer("Отменено.", reply_markup=main_kb(call.from_user.id))


@router.callback_query(F.data.in_({"dl_video", "dl_thumb", "dl_audio"}))
async def download_execute(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    url     = data.get("url")
    title   = data.get("title", "Видео")
    dl_type = call.data

    if not url:
        await call.answer("❌ Ссылка потеряна, начните снова", show_alert=True)
        await state.clear()
        return

    await state.clear()
    await call.message.delete()

    uid    = call.from_user.id
    status = await bot.send_message(uid, "⏳ <b>Загружаю, ожидайте...</b>", parse_mode="HTML")
    tmp    = tempfile.mkdtemp(dir=TEMP_DIR)

    try:
        if dl_type == "dl_thumb":
            await status.edit_text("🖼 Скачиваю превью...")
            path = await download_thumbnail(url, tmp)
            if not path:
                return await status.edit_text("❌ Не удалось скачать превью. Попробуйте другое видео.")

            await status.edit_text("📤 Отправляю...")
            await bot.send_photo(
                uid,
                FSInputFile(path),
                caption=f"🖼 <b>Превью</b>\n📹 {title}",
                parse_mode="HTML"
            )

        elif dl_type == "dl_audio":
            await status.edit_text("🎵 Скачиваю аудио...")
            path, info = await download_audio(url, tmp)
            if not path:
                return await status.edit_text(
                    "❌ Не удалось скачать аудио.\n"
                    "Попробуйте другое видео или другой формат."
                )

            size_mb = os.path.getsize(path) / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                return await status.edit_text(
                    f"❌ Файл слишком большой ({size_mb:.1f} МБ).\n"
                    f"Максимум: {MAX_FILE_SIZE_MB} МБ"
                )

            await status.edit_text("📤 Отправляю аудио...")
            duration = info.get("duration", 0)
            fname    = os.path.basename(path)
            await bot.send_audio(
                uid,
                FSInputFile(path, filename=fname),
                title=title,
                duration=int(duration) if duration else None,
                caption=f"🎵 <b>{title}</b>",
                parse_mode="HTML"
            )

        elif dl_type == "dl_video":
            await status.edit_text("🎬 Скачиваю видео...")
            path, info = await download_video(url, tmp)
            if not path:
                return await status.edit_text(
                    "❌ Не удалось скачать видео.\n"
                    "Попробуйте другое видео или скачайте только звук."
                )

            size_mb = os.path.getsize(path) / (1024 * 1024)
            if size_mb > MAX_FILE_SIZE_MB:
                return await status.edit_text(
                    f"❌ Видео слишком большое ({size_mb:.1f} МБ).\n"
                    f"Максимум: {MAX_FILE_SIZE_MB} МБ.\n\n"
                    f"💡 Попробуйте скачать только аудио."
                )

            await status.edit_text("📤 Отправляю видео...")
            duration = info.get("duration", 0)
            await bot.send_video(
                uid,
                FSInputFile(path),
                duration=int(duration) if duration else None,
                caption=f"🎬 <b>{title}</b>",
                parse_mode="HTML"
            )

        await status.delete()
        inc_downloads(uid)

        if not is_vip(uid):
            await bot.send_message(
                uid,
                f"💡 <i>Хотите без рекламы и без обязательных подписок?\n"
                f"Активируйте ВИП за {VIP_PRICE_STARS} ⭐ звёзд в месяц!</i>",
                reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text="👑 Купить ВИП", callback_data="buy_vip")
                ]]),
                parse_mode="HTML"
            )

    except Exception as e:
        logger.error(f"Ошибка загрузки: {e}")
        try:
            await status.edit_text(
                f"❌ Произошла ошибка при загрузке.\n"
                f"Попробуйте позже или другую ссылку.\n\n"
                f"<code>{str(e)[:200]}</code>",
                parse_mode="HTML"
            )
        except Exception:
            pass
    finally:
        shutil.rmtree(tmp, ignore_errors=True)


# ─── 👑 ВИП подписка ──────────────────────────────────────────────

@router.message(F.text == "👑 ВИП подписка")
async def vip_menu(msg: Message):
    uid = msg.from_user.id
    vip_active = is_vip(uid)
    expires    = vip_expires_str(uid)

    user = get_user(uid)
    ref_count = user[5] if user else 0
    ref_used  = user[6] if user else 0

    status_line = (
        f"✅ Статус: <b>ВИП активен</b>\n"
        f"⏳ Истекает: {expires}"
        if vip_active else
        "❌ Статус: <b>Не активен</b>"
    )

    ref_line = (
        f"✅ Реферальная акция использована"
        if ref_used else
        f"👥 Ваши рефералы: <b>{ref_count}/{REF_INVITE_COUNT}</b>"
    )

    await msg.answer(
        f"👑 <b>ВИП подписка</b>\n\n"
        f"{status_line}\n\n"
        f"<b>Что даёт ВИП:</b>\n"
        f"• 🚫 Никакой рекламы\n"
        f"• 📢 Никаких обязательных подписок\n"
        f"• 🔕 Никаких рассылок\n\n"
        f"💰 Цена: <b>{VIP_PRICE_STARS} ⭐ звёзд / месяц</b>\n\n"
        f"🎁 <b>Бесплатный ВИП:</b>\n"
        f"Пригласи {REF_INVITE_COUNT} друзей → {REF_VIP_DAYS} дней ВИП бесплатно\n"
        f"{ref_line}",
        reply_markup=vip_kb(),
        parse_mode="HTML"
    )


# ─── Оплата звёздами ──────────────────────────────────────────────

@router.callback_query(F.data == "buy_vip")
async def buy_vip_cb(call: CallbackQuery):
    uid = call.from_user.id
    prices = [LabeledPrice(label="ВИП подписка на месяц", amount=VIP_PRICE_STARS)]
    await bot.send_invoice(
        chat_id=uid,
        title="👑 ВИП подписка",
        description=f"ВИП на {VIP_DAYS} дней — без рекламы и обязательных подписок",
        payload=f"vip_{uid}_{VIP_DAYS}",
        currency="XTR",
        prices=prices,
    )
    await call.answer()


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery):
    await query.answer(ok=True)


@router.message(F.successful_payment)
async def payment_done(msg: Message):
    uid = msg.from_user.id
    payload = msg.successful_payment.invoice_payload

    with get_db() as c:
        c.execute(
            "INSERT INTO stars_payments (user_id,stars,payload,created_at) VALUES (?,?,?,?)",
            (uid, msg.successful_payment.total_amount,
             payload, datetime.now().strftime("%Y-%m-%d %H:%M"))
        )

    expires = give_vip(uid, VIP_DAYS)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    await msg.answer(
        f"🎉 <b>ВИП активирован!</b>\n\n"
        f"⏳ Действует до: <b>{exp_str}</b>\n\n"
        f"Наслаждайтесь без рекламы! 🚀",
        reply_markup=main_kb(uid),
        parse_mode="HTML"
    )


# ─── Реферальная программа ────────────────────────────────────────

@router.callback_query(F.data == "ref_program")
async def ref_program(call: CallbackQuery):
    uid  = call.from_user.id
    user = get_user(uid)
    ref_count = user[5] if user else 0
    ref_used  = user[6] if user else 0

    link = await create_start_link(bot, str(uid), encode=False)

    if ref_used:
        text = (
            f"🎁 <b>Реферальная программа</b>\n\n"
            f"✅ Вы уже использовали эту акцию.\n\n"
            f"🔗 Ваша реферальная ссылка:\n<code>{link}</code>"
        )
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

    btns = [[InlineKeyboardButton(
        text="📤 Поделиться",
        url=f"https://t.me/share/url?url={link}&text=Качай+видео+с+YouTube+в+Telegram!"
    )]]
    if not ref_used and ref_count >= REF_INVITE_COUNT:
        btns.insert(0, [InlineKeyboardButton(text="🎁 Забрать ВИП!", callback_data="claim_ref_vip")])

    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=btns),
        parse_mode="HTML"
    )


@router.callback_query(F.data == "claim_ref_vip")
async def claim_ref_vip(call: CallbackQuery):
    uid  = call.from_user.id
    user = get_user(uid)

    if not user:
        return await call.answer("Ошибка", show_alert=True)
    if user[6]:
        return await call.answer("❌ Вы уже использовали эту акцию!", show_alert=True)
    if user[5] < REF_INVITE_COUNT:
        return await call.answer(
            f"❌ Нужно ещё {REF_INVITE_COUNT - user[5]} рефералов!", show_alert=True
        )

    set_ref_used(uid)
    expires = give_vip(uid, REF_VIP_DAYS)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")

    await call.answer("🎉 ВИП активирован!", show_alert=True)
    await call.message.edit_text(
        f"🎉 <b>ВИП получен за рефералов!</b>\n\n"
        f"⏳ Действует до: <b>{exp_str}</b>",
        parse_mode="HTML"
    )


# ─── 🆘 Поддержка ─────────────────────────────────────────────────

@router.message(F.text == "🆘 Поддержка")
async def support(msg: Message):
    await msg.answer(
        f"🆘 <b>Поддержка</b>\n\n"
        f"По всем вопросам обращайтесь к администратору:\n"
        f"👨‍💼 {SUPPORT_USERNAME}",
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
    if msg.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    total, vip_cnt, dls, new = get_stats()
    await msg.answer(
        f"⚙️ <b>Админ панель</b>\n\n"
        f"👥 Пользователей: {total}  |  Новых сегодня: {new}\n"
        f"👑 ВИП активных: {vip_cnt}\n"
        f"📥 Всего скачиваний: {dls}",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )


def adm_back_kb():
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="◀️ Назад", callback_data="adm_back")
    ]])


@router.callback_query(F.data == "adm_back")
async def adm_back(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.clear()
    total, vip_cnt, dls, new = get_stats()
    await call.message.edit_text(
        f"⚙️ <b>Админ панель</b>\n\n"
        f"👥 Пользователей: {total}  |  Новых сегодня: {new}\n"
        f"👑 ВИП активных: {vip_cnt}\n"
        f"📥 Всего скачиваний: {dls}",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )


# ── Статистика ────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_stats")
async def adm_stats(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return
    with get_db() as c:
        total   = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        banned  = c.execute("SELECT COUNT(*) FROM users WHERE is_banned=1").fetchone()[0]
        vip_cnt = c.execute("SELECT COUNT(*) FROM vip WHERE expires_at > ?",
                            (datetime.now().strftime("%Y-%m-%d %H:%M"),)).fetchone()[0]
        dls     = c.execute("SELECT SUM(downloads) FROM users").fetchone()[0] or 0
        stars   = c.execute("SELECT SUM(stars) FROM stars_payments").fetchone()[0] or 0
        today   = datetime.now().strftime("%Y-%m-%d")
        new     = c.execute("SELECT COUNT(*) FROM users WHERE joined_at LIKE ?",
                            (f"{today}%",)).fetchone()[0]
        chans   = c.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    await call.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Всего: {total}  |  Сегодня: +{new}\n"
        f"🚫 Заблокировано: {banned}\n"
        f"👑 ВИП активных: {vip_cnt}\n"
        f"📥 Скачиваний: {dls}\n"
        f"⭐ Заработано звёзд: {stars}\n"
        f"📺 Обяз. каналов: {chans}",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


# ── Выдать ВИП ────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_give_vip")
async def adm_give_vip_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.give_vip_uid)
    await call.message.edit_text(
        "👑 <b>Выдать ВИП</b>\n\n"
        "Введите ID пользователя:",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


@router.message(AdminState.give_vip_uid)
async def adm_give_vip_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❌ Введите числовой ID!")

    user = get_user(uid)
    if not user:
        return await msg.answer("❌ Пользователь не найден!")

    await state.update_data(vip_target=uid)
    await state.set_state(AdminState.give_vip_days)
    await msg.answer(
        f"👤 Пользователь: {user[2]} (ID: {uid})\n"
        f"⏳ Текущий ВИП: {vip_expires_str(uid)}\n\n"
        f"Введите количество дней ВИП:"
    )


@router.message(AdminState.give_vip_days)
async def adm_give_vip_days(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    try:
        days = int(msg.text.strip())
        if days <= 0:
            raise ValueError
    except ValueError:
        return await msg.answer("❌ Введите положительное число дней!")

    data = await state.get_data()
    uid  = data["vip_target"]
    expires = give_vip(uid, days)
    exp_str = datetime.strptime(expires, "%Y-%m-%d %H:%M").strftime("%d.%m.%Y %H:%M")
    await state.clear()

    try:
        await bot.send_message(uid,
            f"👑 Вам выдан ВИП!\n⏳ Действует до: <b>{exp_str}</b>",
            parse_mode="HTML")
    except Exception:
        pass

    await msg.answer(
        f"✅ ВИП выдан пользователю {uid}\n"
        f"⏳ До: {exp_str}",
        reply_markup=main_kb(msg.from_user.id)
    )


# ── Отобрать ВИП ─────────────────────────────────────────────────

@router.callback_query(F.data == "adm_revoke_vip")
async def adm_revoke_vip_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.revoke_vip_uid)
    await call.message.edit_text(
        "❌ <b>Отобрать ВИП</b>\n\n"
        "Введите ID пользователя:",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


@router.message(AdminState.revoke_vip_uid)
async def adm_revoke_vip_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❌ Введите числовой ID!")

    user = get_user(uid)
    if not user:
        return await msg.answer("❌ Пользователь не найден!")

    if not is_vip(uid):
        await state.clear()
        return await msg.answer(
            f"⚠️ У пользователя <b>{uid}</b> нет активного ВИП.",
            reply_markup=main_kb(msg.from_user.id),
            parse_mode="HTML"
        )

    remove_vip(uid)
    await state.clear()

    try:
        await bot.send_message(
            uid,
            "❌ <b>Ваш ВИП был отозван администратором.</b>",
            parse_mode="HTML"
        )
    except Exception:
        pass

    await msg.answer(
        f"✅ ВИП успешно отобран у пользователя <b>{uid}</b>\n"
        f"👤 Имя: {user[2]}",
        reply_markup=main_kb(msg.from_user.id),
        parse_mode="HTML"
    )


# ── Цена ВИП ─────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_vip_price")
async def adm_vip_price(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.set_vip_price)
    await call.message.edit_text(
        f"💰 <b>Изменить цену ВИП</b>\n\n"
        f"Текущая цена: <b>{VIP_PRICE_STARS} ⭐</b>\n\n"
        f"Отправьте новую цену:",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


@router.message(AdminState.set_vip_price)
async def adm_set_vip_price(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    try:
        new_price = int(msg.text.strip())
        if new_price <= 0:
            raise ValueError
    except ValueError:
        return await msg.answer("❌ Введите положительное целое число!")

    global VIP_PRICE_STARS
    VIP_PRICE_STARS = new_price
    await state.clear()
    await msg.answer(
        f"✅ Цена ВИП изменена на <b>{new_price} ⭐</b>",
        reply_markup=main_kb(msg.from_user.id),
        parse_mode="HTML"
    )


# ── Рассылка ─────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_broadcast")
async def adm_bc_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.bc_content)
    await call.message.edit_text(
        "📢 <b>Рассылка — шаг 1/2</b>\n\n"
        "Отправьте сообщение для рассылки.\n"
        "Можно: текст, фото с подписью, видео с подписью.\n\n"
        "⚠️ ВИП пользователи рассылку <b>не получат</b>.",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


@router.message(AdminState.bc_content)
async def adm_bc_content(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    if msg.photo:
        bc_type = "photo"
        file_id = msg.photo[-1].file_id
        caption = msg.caption or ""
    elif msg.video:
        bc_type = "video"
        file_id = msg.video.file_id
        caption = msg.caption or ""
    elif msg.text:
        bc_type = "text"
        file_id = None
        caption = msg.text
    else:
        return await msg.answer("❌ Поддерживаются: текст, фото, видео.")

    await state.update_data(bc_type=bc_type, bc_file_id=file_id, bc_caption=caption)
    await state.set_state(AdminState.bc_buttons)

    await msg.answer(
        "📢 <b>Рассылка — шаг 2/2</b>\n\n"
        "Добавьте inline-кнопки (необязательно).\n"
        "Формат:\n<code>Текст|https://ссылка.com</code>\n\n"
        "Каждая кнопка на новой строке.\n"
        "Или напишите <b>нет</b> чтобы без кнопок.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="нет")]],
            resize_keyboard=True
        ),
        parse_mode="HTML"
    )


@router.message(AdminState.bc_buttons)
async def adm_bc_buttons(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return

    buttons_raw = None if msg.text.strip().lower() in ("нет", "/skip", "skip") else msg.text
    await state.update_data(bc_buttons=buttons_raw)
    data = await state.get_data()

    all_users = all_user_ids()
    non_vip   = [u for u in all_users if not is_vip(u)]

    await state.set_state(AdminState.bc_confirm)
    await msg.answer(
        f"📢 <b>Предпросмотр рассылки</b>\n\n"
        f"📝 Тип: {data['bc_type']}\n"
        f"🔘 Кнопки: {'есть' if buttons_raw else 'нет'}\n"
        f"👥 Получателей: {len(non_vip)} (ВИП не получат)\n\n"
        f"Начать рассылку?",
        reply_markup=ReplyKeyboardRemove(),
        parse_mode="HTML"
    )
    await msg.answer(
        "Подтвердите:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Начать", callback_data="bc_go")],
            [InlineKeyboardButton(text="❌ Отмена",  callback_data="bc_cancel")],
        ])
    )


def parse_buttons(raw: str | None) -> InlineKeyboardMarkup | None:
    if not raw:
        return None
    rows = []
    for line in raw.strip().splitlines():
        if "|" in line:
            label, url = line.split("|", 1)
            rows.append([InlineKeyboardButton(text=label.strip(), url=url.strip())])
    return InlineKeyboardMarkup(inline_keyboard=rows) if rows else None


@router.callback_query(F.data == "bc_go")
async def bc_go(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    data = await state.get_data()
    await state.clear()

    bc_type   = data.get("bc_type")
    file_id   = data.get("bc_file_id")
    caption   = data.get("bc_caption", "")
    kb        = parse_buttons(data.get("bc_buttons"))
    all_users = [u for u in all_user_ids() if not is_vip(u)]

    await call.message.edit_text(f"⏳ Рассылаю {len(all_users)} пользователям...")

    ok, fail = 0, 0
    for uid in all_users:
        try:
            if bc_type == "photo":
                await bot.send_photo(uid, file_id, caption=caption, reply_markup=kb, parse_mode="HTML")
            elif bc_type == "video":
                await bot.send_video(uid, file_id, caption=caption, reply_markup=kb, parse_mode="HTML")
            else:
                await bot.send_message(uid, caption, reply_markup=kb, parse_mode="HTML")
            ok += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.04)

    await call.message.edit_text(
        f"✅ Рассылка завершена!\n📨 Доставлено: {ok}/{len(all_users)}  ❌ Ошибок: {fail}",
        reply_markup=adm_back_kb()
    )
    await call.message.answer("Меню:", reply_markup=main_kb(call.from_user.id))


@router.callback_query(F.data == "bc_cancel")
async def bc_cancel(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("❌ Рассылка отменена.", reply_markup=admin_kb())


# ── Обязательные каналы ───────────────────────────────────────────

@router.callback_query(F.data == "adm_channels")
async def adm_channels(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return
    chs  = get_channels()
    text = "📺 <b>Обязательные подписки</b>\n\n"
    text += "\n".join(f"• {ch[2]}  ({ch[1]})" for ch in chs) if chs else "Каналы не добавлены."

    rows = [[InlineKeyboardButton(text="➕ Добавить", callback_data="ch_add")]]
    for ch in chs:
        rows.append([InlineKeyboardButton(text=f"🗑 {ch[2]}", callback_data=f"ch_del_{ch[0]}")])
    rows.append([InlineKeyboardButton(text="◀️ Назад", callback_data="adm_back")])

    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=rows), parse_mode="HTML")


@router.callback_query(F.data == "ch_add")
async def ch_add_prompt(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.add_channel)
    await call.message.answer(
        "➕ Введите данные канала:\n"
        "<code>@channel_id|Название|https://t.me/channel</code>\n\n"
        "⚠️ Бот должен быть администратором в канале!",
        parse_mode="HTML"
    )


@router.message(AdminState.add_channel)
async def ch_add_handler(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    parts = msg.text.split("|")
    if len(parts) != 3:
        return await msg.answer("❌ Формат: @channel|Название|https://ссылка")
    add_channel(parts[0].strip(), parts[1].strip(), parts[2].strip())
    await state.clear()
    await msg.answer("✅ Канал добавлен!", reply_markup=main_kb(msg.from_user.id))


@router.callback_query(F.data.startswith("ch_del_"))
async def ch_del(call: CallbackQuery):
    if call.from_user.id not in ADMIN_IDS:
        return
    del_channel(int(call.data.split("_")[2]))
    await call.answer("✅ Канал удалён!")
    await adm_channels(call)


# ── Бан / Разбан ─────────────────────────────────────────────────

@router.callback_query(F.data == "adm_ban")
async def adm_ban_start(call: CallbackQuery, state: FSMContext):
    if call.from_user.id not in ADMIN_IDS:
        return
    await state.set_state(AdminState.ban_uid)
    await call.message.edit_text(
        "🚫 <b>Бан/Разбан</b>\n\nВведите ID пользователя:",
        reply_markup=adm_back_kb(),
        parse_mode="HTML"
    )


@router.message(AdminState.ban_uid)
async def adm_ban_uid(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS:
        return
    try:
        uid = int(msg.text.strip())
    except ValueError:
        return await msg.answer("❌ Введите числовой ID!")

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
    else:
        action = "❌ Пользователь не найден"

    await msg.answer(f"{action}: {uid}", reply_markup=main_kb(msg.from_user.id))


# ─── Фоновая задача — снятие истёкших ВИП ────────────────────────

async def vip_cleanup_loop():
    while True:
        try:
            with get_db() as c:
                c.execute(
                    "DELETE FROM vip WHERE expires_at <= ?",
                    (datetime.now().strftime("%Y-%m-%d %H:%M"),)
                )
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
