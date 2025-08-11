# main.py
import os
import asyncio
import aiohttp
from datetime import datetime
from aiohttp import web, ClientSession
from aiogram import Bot, Dispatcher, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State

from config import BOT_TOKEN
from db import (
    init_db,
    register_user,
    get_user_profile,
    increment_message_count,
    set_user_rank,
    get_user_id_by_username,
    award_achievement,
    get_user_achievements,
    set_user_books,
    get_user_activity_stats,
    get_all_users,
)
from achievements_loader import load_achievements_from_excel

# === привязанный чат обсуждений канала ===
DISCUSSION_CHAT_ID = int(os.getenv("DISCUSSION_CHAT_ID")) if os.getenv("DISCUSSION_CHAT_ID") else None

# === вебхук ===
WEBHOOK_HOST = os.getenv("WEBHOOK_HOST")  # например: https://achievebot.onrender.com
WEBHOOK_PATH = "/webhook"
WEBHOOK_URL = f"{WEBHOOK_HOST}{WEBHOOK_PATH}" if WEBHOOK_HOST else None

# 🔒 твой админ id
ADMIN_IDS = [6382960258]

bot_username = None
achievements_by_code = {}     # code -> {title, description, category}
achievements_by_category = {} # category -> list[{code,title,description}]

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ====== Новый код для автопинга ======
async def keep_alive_task():
    """Постоянно пингует /healthz, чтобы Render не усыплял контейнер."""
    if not WEBHOOK_HOST:
        print("[KEEPALIVE] WEBHOOK_HOST не задан, автопинг отключен.")
        return
    url = f"{WEBHOOK_HOST}/healthz"
    async with ClientSession() as session:
        while True:
            try:
                async with session.get(url) as resp:
                    text = await resp.text()
                    print(f"[KEEPALIVE] {resp.status} {text}")
            except Exception as e:
                print(f"[KEEPALIVE] Ошибка пинга: {e}")
            await asyncio.sleep(300)  # каждые 5 минут

# --- ХЕЛПЕР: определяем, является ли сообщение комментарием к посту канала
def is_channel_comment(msg: Message) -> bool:
    # учитываем только сообщения именно в связанном чате (если задан)
    if DISCUSSION_CHAT_ID and msg.chat.id != DISCUSSION_CHAT_ID:
        return False

    # Обычная привязанная группа без тем:
    # комментарий = ответ на авто-перенос поста из канала
    rt = getattr(msg, "reply_to_message", None)
    if rt and getattr(rt, "is_automatic_forward", False):
        sender_chat = getattr(rt, "sender_chat", None)
        if sender_chat and getattr(sender_chat, "type", None) == "channel":
            return True

    # На будущее: если включишь топики (форум), это тоже будет комментом
    if getattr(msg, "is_topic_message", False) or getattr(msg, "message_thread_id", None):
        return True

    return False

# ======================
# Общие утилиты
# ======================
def is_command(cmd_name: str, message: Message) -> bool:
    if not message.text:
        return False
    command_part = message.text.split()[0]
    if "@" in command_part:
        base, mention = command_part.split("@", 1)
        return base == cmd_name and mention.lower() == (bot_username or "").lower()
    return command_part == cmd_name

def get_next_rank_progress(messages: int, comments: int) -> str:
    steps = []

    # Комментарии
    comment_ranks = [
        (5, "💡 Рядовой комментатор"),
        (15, "🧐 Младший комментатор"),
        (30, "🎯 Комментатор"),
        (100, "👨‍🏫 Старший комментатор"),
        (300, "🧠 Капитан-комментатор"),
        (400, "🎖 Майор-комментатор"),
        (500, "🎖 Полковник-комментатор"),
        (1000, "🫅 Верховный комментатор")
    ]
    for required, title in comment_ranks:
        if comments < required:
            steps.append(f"💬 Ещё {required - comments} комментариев до {title}")
            break

    # Сообщения
    message_ranks = [
        (100, "🗨 Болтун"),
        (300, "📣 Голос канала"),
        (1000, "🔥 Легенда чата"),
        (3000, "🌪 Стихийное бедствие")
    ]
    for required, title in message_ranks:
        if messages < required:
            steps.append(f"📨 Ещё {required - messages} сообщений до {title}")
            break

    # Комбинированные
    combined_ranks = [
        ((300, 50), "🌟 Активист"),
        ((2000, 1000), "🛡 Ветеран"),
        ((5000, 2000), "🧭 Бог FicBen")
    ]
    for (msg_req, com_req), title in combined_ranks:
        if messages < msg_req or comments < com_req:
            msg_left = max(0, msg_req - messages)
            com_left = max(0, com_req - comments)
            steps.append(f"🥇 До {title}: {msg_left} сообщений и {com_left} комментариев")
            break

    return "\n".join(steps)

# ======================
# Команды пользователя
# ======================
@dp.message(lambda msg: msg.text and msg.text.startswith("/start"))
async def handle_start(message: Message):
    if not is_command("/start", message):
        return
    await register_user(message.from_user.id, message.from_user.username or "NoUsername")
    await message.answer("🎉 Добро пожаловать! Я — бот званий и ачивок.\nИспользуй /profile чтобы увидеть свою карточку.")

@dp.message(lambda msg: msg.text and msg.text.startswith("/profile"))
async def handle_profile(message: Message):
    if not is_command("/profile", message):
        return

    user_id = message.from_user.id
    data = await get_user_profile(user_id)

    if not data:
        await message.reply("Вы ещё не зарегистрированы. Напишите мне /start.")
        return

    username, joined_at, messages, fanfics, comments, rank_messages, rank_comments, rank_combined = data

    profile_text = f"""
📇 <b>Профиль</b>: @{username or '—'}
🗓 С нами с: {joined_at}
🎖 Звание (сообщения): {rank_messages}
🎖 Звание (комментарии): {rank_comments}
🎖 Комбинированное звание: {rank_combined}
💬 Комментариев: {comments}
📚 Книг: {fanfics}
📈 Сообщений: {messages}
""".strip()

    achievements = await get_user_achievements(user_id)
    if achievements:
        lines = []
        for code in achievements:
            if code in achievements_by_code:
                a = achievements_by_code[code]
                lines.append(f"• {a['title']} — {a['description']}")
            else:
                lines.append(f"• {code}")
        profile_text += "\n\n🏆 Ачивки:\n" + "\n".join(lines)

    progress = get_next_rank_progress(messages, comments)
    if progress:
        profile_text += "\n\n📊 <b>Прогресс:</b>\n" + progress

    await message.reply(profile_text)

@dp.message(lambda msg: msg.text and msg.text.startswith("/stats"))
async def handle_stats(message: Message):
    if not is_command("/stats", message):
        return

    user_id = message.from_user.id
    stats = await get_user_activity_stats(user_id)

    if not stats:
        await message.reply("Нет данных об активности за последние дни.")
        return

    lines = ["📊 <b>Активность за последние 7 дней:</b>"]
    for date, messages, comments in stats:
        lines.append(f"📅 {date}: 💬 Сообщений — {messages} | 🗨 Комментариев — {comments}")

    await message.reply("\n".join(lines))

@dp.message(lambda msg: msg.text and msg.text.startswith("/id"))
async def handle_id(message: Message):
    if not is_command("/id", message):
        return
    await message.reply(f"🆔 Твой user_id: <code>{message.from_user.id}</code>")

@dp.message(lambda msg: msg.text and msg.text.startswith("/whereami"))
async def handle_whereami(message: Message):
    await message.reply(
        "🔍 Где я:\n"
        f"chat.id: <code>{message.chat.id}</code>\n"
        f"chat.type: <b>{message.chat.type}</b>\n"
        f"thread: <code>{getattr(message, 'message_thread_id', None)}</code>\n"
        f"is_automatic_forward: <code>{getattr(message, 'is_automatic_forward', False)}</code>"
    )

@dp.message(lambda msg: msg.text and msg.text.startswith("/help"))
async def handle_help(message: Message):
    if not is_command("/help", message):
        return

    help_text = """
<b>📘 Доступные команды:</b>

/start — зарегистрироваться в системе
/profile — посмотреть свой профиль
/id — узнать свой user_id
/achievements — список всех доступных ачивок
/ranks — показать систему званий
/stats — посмотреть активность за 7 дней
/whereami — показать chat.id (для настройки обсуждений)
/help — показать это сообщение

<b>🔧 Админ-команды:</b>
/admin — панель с кнопками (книги/ачивки)
""".strip()

    await message.reply(help_text)

@dp.message(lambda msg: msg.text and msg.text.startswith("/ranks"))
async def handle_ranks(message: Message):
    if not is_command("/ranks", message):
        return

    text = """
<b>🎖 Система званий</b>

<b>📨 За сообщения:</b>
🗨 Болтун — 100+
📣 Голос канала — 300+
🔥 Легенда чата — 1000+
🌪 Стихийное бедствие — 3000+

<b>💬 За комментарии:</b>
💡 Рядовой комментатор — 5+
🧐 Младший комментатор — 15+
🎯 Комментатор — 30+
👨‍🏫 Старший комментатор — 100+
🧠 Капитан-комментатор — 300+
🎖 Майор-комментатор — 400+
🎖 Полковник-комментатор — 500+
🫅 Верховный комментатор — 1000+

<b>🌟 Комбинированные:</b>
🌟 Активист — 300+ сообщений и 50+ комментариев  
🛡 Ветеран — 2000+ сообщений и 1000+ комментариев  
🧭 Бог FicBen — 5000+ сообщений и 2000+ комментариев
""".strip()

    await message.reply(text)

@dp.message(lambda msg: msg.text and msg.text.startswith("/achievements"))
async def handle_all_achievements(message: Message):
    if not is_command("/achievements", message):
        return

    if not achievements_by_code:
        await message.reply("❌ Список ачивок не загружен.")
        return

    grouped = {}
    for code, ach in achievements_by_code.items():
        grouped.setdefault(ach['category'], []).append(ach)

    text = "<b>🏆 Все доступные ачивки:</b>\n"
    for category, items in grouped.items():
        text += f"\n<b>{category}</b>\n"
        for ach in items:
            text += f"• <b>{ach['title']}</b> — {ach['description']}\n"

    await message.reply(text.strip())

@dp.message(lambda msg: msg.text and msg.text.startswith("/about"))
async def handle_about(message: Message):
    about_text = """
👋 <b>Привет! Я — бот FicBen Studio для отображения достижений и активности</b>

📊 Я считаю:
• Кол-во сообщений, комментариев и книг
• Ваш прогресс и звание
• Активность по дням
• Ачивки за разные достижения

🏆 У меня есть крутая система рангов и ачивок. Просто общайся — и прогрессируй!

📚 Команда для начала: /start  
📝 Посмотреть профиль: /profile  
🎖 Открыть список ачивок: /achievements  
📈 Проверить активность: /stats  
⚙️ Помощь: /help  

👨‍💻 Автор: @real_qewbytini
    """.strip()

    await message.reply(about_text)

# ======================
# Админка с кнопками
# ======================
class SetBooksFSM(StatesGroup):
    waiting_for_user = State()
    waiting_for_amount = State()

class GiveAchievementFSM(StatesGroup):
    waiting_for_user = State()
    waiting_for_category = State()
    waiting_for_pick = State()  # ожидание выбора конкретной ачивки кнопкой

def admin_root_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📚 Установить книги", callback_data="admin:set_books")],
        [InlineKeyboardButton(text="🏆 Выдать ачивку", callback_data="admin:give_achieve")],
    ])

def make_users_keyboard(users, page: int, per_page: int, mode: str) -> InlineKeyboardMarkup:
    total = len(users)
    start = page * per_page
    end = start + per_page
    chunk = users[start:end]

    rows = []
    for user_id, username in chunk:
        label = f"@{username}" if username else f"ID: {user_id}"
        rows.append([InlineKeyboardButton(text=label, callback_data=f"{mode}:select:{user_id}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"{mode}:page:{page-1}"))
    if end < total:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"{mode}:page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="↩️ Назад", callback_data="admin:back")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def make_categories_keyboard(categories: list[str], page: int = 0) -> InlineKeyboardMarkup:
    per_page = 6
    start = page * per_page
    end = start + per_page
    chunk = categories[start:end]

    rows = []
    row = []
    for i, cat in enumerate(chunk, start=start):
        row.append(InlineKeyboardButton(text=cat, callback_data=f"ach:cat:{i}"))
        if len(row) == 3:
            rows.append(row)
            row = []
    if row:
        rows.append(row)

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"ach:cat_page:{page-1}"))
    if end < len(categories):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"ach:cat_page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="↩️ Назад к пользователям", callback_data="ach:back_to_users")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

def make_achievements_keyboard(items: list[dict], page: int = 0, per_page: int = 10) -> InlineKeyboardMarkup:
    start = page * per_page
    end = start + per_page
    chunk = items[start:end]

    rows = []
    for a in chunk:
        title = a.get("title") or a.get("code")
        code = a.get("code")
        rows.append([InlineKeyboardButton(text=f"🏆 {title}", callback_data=f"ach:pick:{code}")])

    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"ach:items_page:{page-1}"))
    if end < len(items):
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"ach:items_page:{page+1}"))
    if nav:
        rows.append(nav)

    rows.append([InlineKeyboardButton(text="↩️ К категориям", callback_data="ach:back_to_categories")])
    return InlineKeyboardMarkup(inline_keyboard=rows)

@dp.message(lambda msg: msg.text and msg.text.startswith("/admin"))
async def handle_admin_panel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.reply("🚫 У тебя нет прав для этой команды.")
        return
    await message.reply("<b>🔧 Админ-панель:</b>\nВыбери действие:", reply_markup=admin_root_kb())

@dp.callback_query(F.data == "admin:back")
async def admin_back(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 Нет доступа", show_alert=True)
        return
    await state.clear()
    await callback.message.edit_text("<b>🔧 Админ-панель:</b>\nВыбери действие:", reply_markup=admin_root_kb())
    await callback.answer()

class _CtxKeys:
    USERS = "all_users"
    PAGE = "page"
    SELECTED_USER = "selected_user_id"
    CATEGORIES = "categories"
    SELECTED_CATEGORY = "selected_category"
    ITEMS_PAGE = "items_page"
    CATS_PAGE = "cats_page"

@dp.callback_query(F.data == "admin:set_books")
async def admin_set_books_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 Нет доступа", show_alert=True)
        return
    users = await get_all_users()
    await state.set_state(SetBooksFSM.waiting_for_user)
    await state.update_data({_CtxKeys.USERS: users, _CtxKeys.PAGE: 0})
    kb = make_users_keyboard(users, page=0, per_page=10, mode="books")
    await callback.message.edit_text("Выбери пользователя для установки количества книг:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^books:page:\d+$"))
async def books_page_nav(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    users = data.get(_CtxKeys.USERS, [])
    page = int(callback.data.split(":")[2])
    await state.update_data({_CtxKeys.PAGE: page})
    kb = make_users_keyboard(users, page=page, per_page=10, mode="books")
    await callback.message.edit_text("Выбери пользователя для установки количества книг:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^books:select:\d+$"))
async def books_select_user(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split(":")[2])
    await state.update_data({_CtxKeys.SELECTED_USER: user_id})
    await state.set_state(SetBooksFSM.waiting_for_amount)
    await callback.message.edit_text(f"Введи количество книг для пользователя <code>{user_id}</code>:")
    await callback.answer()

@dp.message(SetBooksFSM.waiting_for_amount)
async def set_books_amount(message: Message, state: FSMContext):
    if not (message.text and message.text.isdigit()):
        await message.reply("❗ Введи целое число (0, 1, 2, ...)")
        return
    data = await state.get_data()
    user_id = data.get(_CtxKeys.SELECTED_USER)
    await set_user_books(user_id, int(message.text))
    await message.reply(f"✅ Количество книг у <code>{user_id}</code> установлено на <b>{message.text}</b>.")
    await state.clear()

@dp.callback_query(F.data == "admin:give_achieve")
async def admin_give_achieve_start(callback: CallbackQuery, state: FSMContext):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("🚫 Нет доступа", show_alert=True)
        return
    users = await get_all_users()
    await state.set_state(GiveAchievementFSM.waiting_for_user)
    await state.update_data({_CtxKeys.USERS: users, _CtxKeys.PAGE: 0})
    kb = make_users_keyboard(users, page=0, per_page=10, mode="ach")
    await callback.message.edit_text("Выбери пользователя для выдачи ачивки:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:page:\d+$"))
async def ach_users_page(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    users = data.get(_CtxKeys.USERS, [])
    page = int(callback.data.split(":")[2])
    await state.update_data({_CtxKeys.PAGE: page})
    kb = make_users_keyboard(users, page=page, per_page=10, mode="ach")
    await callback.message.edit_text("Выбери пользователя для выдачи ачивки:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:select:\d+$"))
async def ach_select_user(callback: CallbackQuery, state: FSMContext):
    user_id = int(callback.data.split(":")[2])
    await state.update_data({_CtxKeys.SELECTED_USER: user_id})

    categories = list(achievements_by_category.keys())
    await state.set_state(GiveAchievementFSM.waiting_for_category)
    await state.update_data({_CtxKeys.CATEGORIES: categories, _CtxKeys.CATS_PAGE: 0})
    kb = make_categories_keyboard(categories, page=0)
    await callback.message.edit_text(
        f"Кому выдаём: <code>{user_id}</code>\nВыбери категорию ачивок:",
        reply_markup=kb
    )
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:cat_page:\d+$"))
async def ach_categories_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split(":")[2])
    data = await state.get_data()
    categories = data.get(_CtxKeys.CATEGORIES, [])
    await state.update_data({_CtxKeys.CATS_PAGE: page})
    kb = make_categories_keyboard(categories, page=page)
    await callback.message.edit_text("Выбери категорию ачивок:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "ach:back_to_users")
async def ach_back_to_users(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    users = data.get(_CtxKeys.USERS, [])
    page = data.get(_CtxKeys.PAGE, 0)
    await state.set_state(GiveAchievementFSM.waiting_for_user)
    kb = make_users_keyboard(users, page=page, per_page=10, mode="ach")
    await callback.message.edit_text("Выбери пользователя для выдачи ачивки:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:cat:\d+$"))
async def ach_pick_category(callback: CallbackQuery, state: FSMContext):
    idx = int(callback.data.split(":")[2])
    data = await state.get_data()
    categories = data.get(_CtxKeys.CATEGORIES, [])
    if idx < 0 or idx >= len(categories):
        await callback.answer("Некорректная категория", show_alert=True)
        return
    category = categories[idx]
    await state.update_data({_CtxKeys.SELECTED_CATEGORY: category, _CtxKeys.ITEMS_PAGE: 0})

    items = achievements_by_category.get(category, [])
    kb = make_achievements_keyboard(items, page=0, per_page=10)
    await state.set_state(GiveAchievementFSM.waiting_for_pick)
    await callback.message.edit_text(f"Категория: <b>{category}</b>\nВыбери ачивку:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data == "ach:back_to_categories")
async def ach_back_to_categories(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    categories = data.get(_CtxKeys.CATEGORIES, [])
    page = data.get(_CtxKeys.CATS_PAGE, 0)
    kb = make_categories_keyboard(categories, page=page)
    await state.set_state(GiveAchievementFSM.waiting_for_category)
    await callback.message.edit_text("Выбери категорию ачивок:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:items_page:\d+$"))
async def ach_items_page(callback: CallbackQuery, state: FSMContext):
    page = int(callback.data.split(":")[2])
    data = await state.get_data()
    category = data.get(_CtxKeys.SELECTED_CATEGORY)
    items = achievements_by_category.get(category, [])
    await state.update_data({_CtxKeys.ITEMS_PAGE: page})
    kb = make_achievements_keyboard(items, page=page, per_page=10)
    await callback.message.edit_text(f"Категория: <b>{category}</b>\nВыбери ачивку:", reply_markup=kb)
    await callback.answer()

@dp.callback_query(F.data.regexp(r"^ach:pick:.+"))
async def ach_pick_one(callback: CallbackQuery, state: FSMContext):
    code = callback.data.split(":", 2)[2]
    data = await state.get_data()
    user_id = data.get(_CtxKeys.SELECTED_USER)
    await award_achievement(user_id, code)
    title = achievements_by_code.get(code, {}).get("title", code)
    await callback.message.edit_text(f"🏆 Ачивка <b>{title}</b> (<code>{code}</code>) выдана пользователю <code>{user_id}</code>.")
    await state.clear()
    await callback.answer()

# ======================
# Подсчёт сообщений + авто-поздравления
# ======================
@dp.message()
async def handle_all_messages(message: Message):
    if message.from_user.is_bot:
        return

    # считаем «комментом» только то, что реально комментарий к посту канала
    is_comment = is_channel_comment(message)

    changes = await increment_message_count(
        user_id=message.from_user.id,
        is_comment=is_comment
    )

    # Поздравляем только при реальной смене ранга (db.update_user_rank вернёт только изменения)
    if isinstance(changes, dict) and changes:
        lines = ["🎉 <b>Поздравляем с новым званием!</b>"]
        if "messages" in changes:
            lines.append(f"📨 За сообщения: {changes['messages']}")
        if "comments" in changes:
            lines.append(f"💬 За комментарии: {changes['comments']}")
        if "combined" in changes:
            lines.append(f"🌟 Комбинированное: {changes['combined']}")
        await message.reply("\n".join(lines))

# ======================
# AIOHTTP СЕРВЕР (WEBHOOK)
# ======================
async def on_startup(app):
    global bot_username, achievements_by_code, achievements_by_category

    await init_db()
    me = await bot.get_me()
    bot_username = me.username
    print(f"✅ Запуск бота: @{bot_username}")

    # Загружаем ачивки из Excel
    achievements_list = await load_achievements_from_excel()
    achievements_by_code = {a["code"]: a for a in achievements_list}
    achievements_by_category = {}
    for a in achievements_list:
        cat = a.get("category") or "Прочее"
        achievements_by_category.setdefault(cat, []).append(a)
    for cat in achievements_by_category:
        achievements_by_category[cat].sort(key=lambda x: (x.get("title") or x.get("code") or "").lower())

    # Устанавливаем вебхук
    if not WEBHOOK_URL:
        raise RuntimeError("WEBHOOK_HOST не задан. Укажи переменную окружения WEBHOOK_HOST, например https://your-bot.onrender.com")
    await bot.set_webhook(WEBHOOK_URL)
    print(f"🌍 Вебхук установлен: {WEBHOOK_URL}")
    
    asyncio.create_task(keep_alive_task())

async def on_shutdown(app):
    await bot.delete_webhook()
    print("🧹 Вебхук удалён")

async def handle_webhook(request):
    update = await request.json()
    await dp.feed_webhook_update(bot, update)
    return web.Response()

async def handle_health(request):
    return web.Response(text="ok")

def run():
    app = web.Application()
    app.router.add_post(WEBHOOK_PATH, handle_webhook)
    app.router.add_get("/healthz", handle_health)  # health-check для Render
    app.on_startup.append(on_startup)
    app.on_shutdown.append(on_shutdown)
    web.run_app(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))

if __name__ == "__main__":
    run()
    


