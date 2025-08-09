# db.py
import aiosqlite
from config import DB_PATH


async def _ensure_users_columns(conn):
    """
    Идемпотентная миграция: гарантируем, что в таблице users есть
    rank_messages, rank_comments, rank_combined.
    """
    cols = {}
    async with conn.execute("PRAGMA table_info(users)") as cur:
        async for cid, name, ctype, notnull, dflt, pk in cur:
            cols[name] = True

    # Добавляем недостающие колонки. DEFAULT нужен, чтобы старые строки получили значение сразу.
    if "rank_messages" not in cols:
        await conn.execute("ALTER TABLE users ADD COLUMN rank_messages TEXT DEFAULT ''")
    if "rank_comments" not in cols:
        await conn.execute("ALTER TABLE users ADD COLUMN rank_comments TEXT DEFAULT ''")
    if "rank_combined" not in cols:
        await conn.execute("ALTER TABLE users ADD COLUMN rank_combined TEXT DEFAULT ''")


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        # Основная таблица пользователей (старая база может быть без новых полей — ниже миграция это починит)
        await db.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id   INTEGER PRIMARY KEY,
            username  TEXT,
            joined_at TEXT,
            messages  INTEGER DEFAULT 0,
            fanfics   INTEGER DEFAULT 0,
            comments  INTEGER DEFAULT 0
        )
        ''')

        # Таблица достижений
        await db.execute('''
        CREATE TABLE IF NOT EXISTS achievements (
            user_id INTEGER,
            code    TEXT,
            PRIMARY KEY (user_id, code)
        )
        ''')

        # Таблица активности по дням
        await db.execute('''
        CREATE TABLE IF NOT EXISTS activity (
            user_id  INTEGER,
            date     TEXT,
            messages INTEGER DEFAULT 0,
            comments INTEGER DEFAULT 0,
            PRIMARY KEY (user_id, date)
        )
        ''')

        # 🔧 Миграция недостающих колонок users
        await _ensure_users_columns(db)

        await db.commit()


# ========== Пользователи ==========
async def register_user(user_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, joined_at) VALUES (?, ?, datetime('now'))",
            (user_id, username)
        )
        await db.commit()


async def get_user_profile(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT username, joined_at, messages, fanfics, comments,
                   rank_messages, rank_comments, rank_combined
            FROM users
            WHERE user_id = ?
        ''', (user_id,)) as cursor:
            return await cursor.fetchone()


async def update_user_rank(user_id: int):
    """
    Пересчитать 3 типа званий и вернуть словарь изменений:
    { "messages": "...", "comments": "...", "combined": "..." } — только те, что изменились.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT messages, comments, rank_messages, rank_comments, rank_combined
            FROM users WHERE user_id = ?
        ''', (user_id,)) as cursor:
            row = await cursor.fetchone()
            if not row:
                return {}

            messages, comments, old_r_msg, old_r_com, old_r_comb = row

    # 📨 Звание за сообщения
    if messages >= 3000:
        r_msg = "🌪 Стихийное бедствие"
    elif messages >= 1000:
        r_msg = "🔥 Легенда чата"
    elif messages >= 300:
        r_msg = "📣 Голос канала"
    elif messages >= 100:
        r_msg = "🗨 Болтун"
    else:
        r_msg = "🐣 Новичок"

    # 💬 Звание за комментарии
    if comments >= 1000:
        r_com = "🫅 Верховный комментатор"
    elif comments >= 500:
        r_com = "🎖 Полковник-комментатор"
    elif comments >= 400:
        r_com = "🎖 Майор-комментатор"
    elif comments >= 300:
        r_com = "🧠 Капитан-комментатор"
    elif comments >= 100:
        r_com = "👨‍🏫 Старший комментатор"
    elif comments >= 30:
        r_com = "🎯 Комментатор"
    elif comments >= 15:
        r_com = "🧐 Младший комментатор"
    elif comments >= 5:
        r_com = "💡 Рядовой комментатор"
    else:
        r_com = "❌ Нет звания"

    # 🌟 Комбинированное
    if messages >= 5000 and comments >= 2000:
        r_comb = "🧭 Бог FicBen"
    elif messages >= 2000 and comments >= 1000:
        r_comb = "🛡 Ветеран"
    elif messages >= 300 and comments >= 50:
        r_comb = "🌟 Активист"
    else:
        r_comb = "—"

    changes = {}
    if r_msg != (old_r_msg or ""):
        changes["messages"] = r_msg
    if r_com != (old_r_com or ""):
        changes["comments"] = r_com
    if r_comb != (old_r_comb or ""):
        changes["combined"] = r_comb

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            UPDATE users
            SET rank_messages = ?, rank_comments = ?, rank_combined = ?
            WHERE user_id = ?
        ''', (r_msg, r_com, r_comb, user_id))
        await db.commit()

    return changes


async def increment_message_count(user_id: int, is_comment: bool = False):
    """
    +1 сообщение; если это ответ на сообщение — считаем как комментарий.
    Возвращает словарь изменений рангов (см. update_user_rank()).
    """
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE users SET messages = messages + 1 WHERE user_id = ?",
            (user_id,)
        )
        if is_comment:
            await db.execute(
                "UPDATE users SET comments = comments + 1 WHERE user_id = ?",
                (user_id,)
            )
        await db.commit()

    changes = await update_user_rank(user_id)

    # ⏱ Запись в таблицу activity
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('''
            INSERT INTO activity (user_id, date, messages, comments)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(user_id, date) DO UPDATE SET
                messages = messages + ?,
                comments = comments + ?
        ''', (
            user_id, today,
            1, int(is_comment),
            1, int(is_comment)
        ))
        await db.commit()

    return changes


async def set_user_rank(user_id: int, rank_type: str, rank_value: str):
    """rank_type: 'messages', 'comments', 'combined'"""
    field = f"rank_{rank_type}"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE users SET {field} = ? WHERE user_id = ?", (rank_value, user_id))
        await db.commit()


async def get_user_id_by_username(username: str):
    username = username.lstrip("@").lower()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_id FROM users WHERE LOWER(username) = ?", (username,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row else None


async def get_all_users():
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, username FROM users ORDER BY username") as cursor:
            return await cursor.fetchall()


# ========== Ачивки ==========
async def has_achievement(user_id: int, code: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT 1 FROM achievements WHERE user_id = ? AND code = ?",
            (user_id, code)
        ) as cursor:
            return await cursor.fetchone() is not None


async def award_achievement(user_id: int, code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO achievements (user_id, code) VALUES (?, ?)",
            (user_id, code)
        )
        await db.commit()


async def get_user_achievements(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT code FROM achievements WHERE user_id = ?",
            (user_id,)
        ) as cursor:
            return [row[0] async for row in cursor]


# ========== Книги и активность ==========
async def set_user_books(user_id: int, books_count: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE users SET fanfics = ? WHERE user_id = ?", (books_count, user_id))
        await db.commit()


async def get_user_activity_stats(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute('''
            SELECT date, messages, comments
            FROM activity
            WHERE user_id = ?
            ORDER BY date DESC
            LIMIT 7
        ''', (user_id,)) as cursor:
            return await cursor.fetchall()
