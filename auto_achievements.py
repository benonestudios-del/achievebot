# auto_achievements.py

from achievements import award_achievement
from db import get_user_profile

# 👇 Проверка: опубликовал хотя бы один фанфик
async def check_first_chapter(user_id, message=None):
    profile = await get_user_profile(user_id)
    if profile and profile[4] >= 1:  # fanfics count
        await award_achievement(user_id, "first_chapter", message)

# 👇 Проверка: оставил 50+ комментариев
async def check_comment_master(user_id, message=None):
    profile = await get_user_profile(user_id)
    if profile and profile[5] >= 50:  # comments count
        await award_achievement(user_id, "comment_master", message)

# 👇 Проверка: написал 100+ сообщений
async def check_message_master(user_id, message=None):
    profile = await get_user_profile(user_id)
    if profile and profile[2] >= 100:  # messages count
        await award_achievement(user_id, "message_master", message)

# 🧠 Словарь соответствий: achievement_id → проверяющая функция
_AUTO_ACHIEVEMENTS = {
    "first_chapter": check_first_chapter,
    "comment_master": check_comment_master,
    "message_master": check_message_master
}

# 🔁 Основной обработчик автоачивок (вызывается из main.py)
async def run_auto_achievements(user_id, message=None):
    for check_fn in _AUTO_ACHIEVEMENTS.values():
        await check_fn(user_id, message)
