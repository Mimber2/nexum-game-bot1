import asyncio
import time
import random
import sqlite3
from datetime import date, timedelta
import re

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.types import (
    Message, InlineKeyboardButton, InlineKeyboardMarkup,
    CallbackQuery, User, LabeledPrice, PreCheckoutQuery,
    ChatMemberAdministrator, ChatMemberOwner
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.exceptions import TelegramBadRequest

# ─────────────────────────────────────────────────────────────
#                     КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────────────────────

BOT_TOKEN = "8763557889:AAH1WdZnzYELr-4OCFJbe0-CWyUSpW952TM"
OWNER_ID = 5093616942
OWNER_USERNAME = "@kilka38548"
MOD_LOG_CHAT_ID = -1001234567890  # канал логов (или None)

REFERRAL_BONUSES = {1: 500, 2: 250, 3: 100}
STARS_TO_GEMS_RATE = 100
PREMIUM_ENERGY_BONUS = 20
PREMIUM_DAILY_BONUS_MIN = 50
PREMIUM_DAILY_BONUS_MAX = 120
PREMIUM_WIN_MULTIPLIER = 1.15

LEVEL_REQUIREMENTS = {
    1: 0, 2: 5, 3: 15, 4: 30, 5: 60,
    6: 100, 7: 150, 8: 250, 9: 400, 10: 600,
}

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher(storage=MemoryStorage())
router = Router()

BOT_ID = None
DB_FILE = "duelbot.db"

conn = sqlite3.connect(DB_FILE, check_same_thread=False)
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    coins INTEGER DEFAULT 150,
    gems INTEGER DEFAULT 0,
    energy INTEGER DEFAULT 100,
    level INTEGER DEFAULT 1,
    last_active REAL DEFAULT 0,
    last_daily REAL DEFAULT 0,
    won_bets INTEGER DEFAULT 0,
    lost_bets INTEGER DEFAULT 0,
    last_bet_result TEXT DEFAULT 'ещё не играл',
    double_daily_until REAL DEFAULT 0,
    lucky_charm_until REAL DEFAULT 0,
    safe_all_until REAL DEFAULT 0,
    vip_until REAL DEFAULT 0,
    today_wins INTEGER DEFAULT 0,
    last_reset_date TEXT DEFAULT '2025-01-01',
    referrer_id INTEGER DEFAULT NULL,
    level1_count INTEGER DEFAULT 0,
    level2_count INTEGER DEFAULT 0,
    level3_count INTEGER DEFAULT 0,
    referral_bonus_given INTEGER DEFAULT 0,
    banned INTEGER DEFAULT 0,
    is_premium INTEGER DEFAULT 0,
    total_won_coins INTEGER DEFAULT 0,
    total_lost_coins INTEGER DEFAULT 0
)
""")

cursor.execute("PRAGMA table_info(users)")
columns = [col[1] for col in cursor.fetchall()]
for col in ['is_premium', 'total_won_coins', 'total_lost_coins']:
    if col not in columns:
        cursor.execute(f"ALTER TABLE users ADD COLUMN {col} INTEGER DEFAULT 0")
conn.commit()

duels = {}

# ─── FSM для админки ─────────────────────────────────────────────────────
class AdminEditStates(StatesGroup):
    waiting_for_amount = State()
    waiting_for_reason = State()

# ─── Вспомогательные функции ─────────────────────────────────────────────

async def init_bot_id():
    global BOT_ID
    me = await bot.get_me()
    BOT_ID = me.id
    print(f"Nexum Game ⚔️✨ запущен | BOT_ID: {BOT_ID} | {date.today()}")

dp.startup.register(init_bot_id)

def is_bot(uid: int) -> bool:
    return BOT_ID is not None and uid == BOT_ID

async def get_user(uid: int, tg_user: User = None) -> dict:
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (uid,))
    row = cursor.fetchone()
    
    if row:
        d = dict(row)
        if tg_user:
            changed = False
            if tg_user.username and d.get("username") != tg_user.username:
                d["username"] = tg_user.username
                changed = True
            if tg_user.first_name and d.get("first_name") != tg_user.first_name:
                d["first_name"] = tg_user.first_name
                changed = True
            if tg_user.is_premium != bool(d.get("is_premium", 0)):
                d["is_premium"] = 1 if tg_user.is_premium else 0
                changed = True
            if changed:
                cursor.execute("UPDATE users SET username=?, first_name=?, is_premium=? WHERE user_id=?",
                               (d["username"], d["first_name"], d["is_premium"], uid))
                conn.commit()
        return d
    
    default = {
        "user_id": uid,
        "username": tg_user.username if tg_user else None,
        "first_name": tg_user.first_name if tg_user else "Unknown",
        "coins": 150,
        "gems": 0,
        "energy": 100,
        "level": 1,
        "last_active": time.time(),
        "last_daily": 0,
        "won_bets": 0,
        "lost_bets": 0,
        "last_bet_result": "ещё не играл",
        "today_wins": 0,
        "referrer_id": None,
        "level1_count": 0,
        "level2_count": 0,
        "level3_count": 0,
        "referral_bonus_given": 0,
        "banned": 0,
        "is_premium": 1 if (tg_user and tg_user.is_premium) else 0,
        "total_won_coins": 0,
        "total_lost_coins": 0,
    }
    cols = ", ".join(default.keys())
    vals = tuple(default.values())
    cursor.execute(f"INSERT INTO users ({cols}) VALUES ({','.join('?'*len(default))})", vals)
    conn.commit()
    return default

def save_user(d: dict):
    uid = d["user_id"]
    fields = {k: v for k, v in d.items() if k != "user_id"}
    set_str = ", ".join(f"{k}=?" for k in fields)
    cursor.execute(f"UPDATE users SET {set_str} WHERE user_id=?", (*fields.values(), uid))
    conn.commit()

def format_name(u) -> str:
    if isinstance(u, User):
        username = u.username
        first_name = u.first_name
        uid = u.id
    else:
        username = u.get("username")
        first_name = u.get("first_name")
        uid = u["user_id"]
    return f"@{username}" if username else (first_name or f"ID {uid}")

def reset_daily_if_needed():
    today = date.today().isoformat()
    cursor.execute("UPDATE users SET today_wins=0, last_reset_date=? WHERE last_reset_date != ?", (today, today))
    conn.commit()

def parse_duration(duration_str: str) -> int or None:
    if not duration_str:
        return None
    multipliers = {'d': 86400, 'h': 3600, 'm': 60, 's': 1}
    total = 0
    for match in re.findall(r"(\d+)([dhms])", duration_str):
        total += int(match[0]) * multipliers.get(match[1], 1)
    return total if total > 0 else None

async def is_group_admin(chat_id: int, user_id: int) -> bool:
    try:
        member = await bot.get_chat_member(chat_id, user_id)
        return isinstance(member, (ChatMemberAdministrator, ChatMemberOwner))
    except:
        return False

async def safe_send_message(user_id: int, text: str, reply_markup=None):
    try:
        await bot.send_message(user_id, text, reply_markup=reply_markup, parse_mode="HTML")
        return True
    except TelegramBadRequest as e:
        if "chat not found" in str(e).lower():
            print(f"Не удалось отправить {user_id}: chat not found")
            return False
        raise
    except Exception as e:
        print(f"Ошибка отправки {user_id}: {e}")
        return False

# ─── Проверка и повышение уровня ────────────────────────────────────────

async def check_and_level_up(user: dict):
    current_level = user["level"]
    wins = user["won_bets"]

    next_level = current_level + 1
    required = LEVEL_REQUIREMENTS.get(next_level)

    if required is None or wins < required:
        return False, None

    user["level"] = next_level

    reward_coins = next_level * 200
    reward_energy = min(100 - user["energy"], 30 + next_level * 5)

    user["coins"] += reward_coins
    user["energy"] += reward_energy

    save_user(user)

    msg = f"🎉 <b>Новый уровень!</b> Вы достигли <b>{next_level}</b> уровня!\n" \
          f"+{reward_coins:,} монет 💰\n" \
          f"+{reward_energy} энергии ⚡"

    return True, msg

# ─── ПРОФИЛЬ ─────────────────────────────────────────────────────────────

async def show_profile(event):
    is_message = isinstance(event, Message)
    msg = event if is_message else event.message
    uid = msg.from_user.id

    u = await get_user(uid, msg.from_user)

    reset_daily_if_needed()
    cursor.execute("SELECT COUNT(*) + 1 FROM users WHERE coins > ?", (u["coins"],))
    rank = cursor.fetchone()[0]

    total_games = u["won_bets"] + u["lost_bets"]
    win_rate = round((u["won_bets"] / total_games * 100), 1) if total_games > 0 else 0.0
    net_profit = u.get("total_won_coins", 0) - u.get("total_lost_coins", 0)

    current_level = u["level"]
    next_req = LEVEL_REQUIREMENTS.get(current_level + 1, "Макс.")
    wins_to_next = max(0, next_req - u["won_bets"]) if isinstance(next_req, int) else "Макс."

    premium_tag = " 👑" if u.get("is_premium", 0) == 1 else ""

    text = f"""✨ <b>{format_name(u)}{premium_tag}</b> ✨

💰 <b>{u['coins']:,}</b> монет   💎 <b>{u.get('gems', 0):,}</b> гемов
⚡ <b>{u['energy']}/100</b> энергии   🏆 Уровень <b>{current_level}</b>

📊 Статистика:
• Всего игр: <b>{total_games}</b>
• Побед: <b>{u['won_bets']}</b> ({win_rate}%)
• Поражений: <b>{u['lost_bets']}</b>
• Чистая прибыль: <b>{net_profit:+,} монет</b>
  ├ Выиграно: <b>{u.get('total_won_coins', 0):,}</b>
  └ Проиграно: <b>{u.get('total_lost_coins', 0):,}</b>
• Сегодня побед: <b>{u['today_wins']}</b>
• До уровня {current_level + 1}: <b>{wins_to_next}</b> побед
• Последняя: {u['last_bet_result']}

🌟 Место в топе: <b>#{rank}</b>

👥 Рефералы:
• 1 ур: <b>{u.get('level1_count', 0)}</b> (+500)
• 2 ур: <b>{u.get('level2_count', 0)}</b> (+250)
• 3 ур: <b>{u.get('level3_count', 0)}</b> (+100)
"""

    builder = InlineKeyboardBuilder()
    builder.button(text="🎁 Daily", callback_data="daily")
    builder.button(text="🛍️ Магазин", callback_data="shop")
    builder.button(text="💎 Магазин гемов", callback_data="gemshop")

    builder.button(text="⭐ Пополнить", callback_data="donate_info")
    builder.button(text="🏆 Топы", callback_data="top_menu")
    builder.button(text="📜 Инструкция", callback_data="instruction")
    builder.button(text="ℹ️ О боте", callback_data="about_bot")

    bot_me = await bot.get_me()
    ref_url = f"https://t.me/{bot_me.username}?start=ref_menu"
    builder.button(text="🔗 Реф. ссылка", url=ref_url)

    builder.button(text="📣 Channel", url="https://t.me/nexum_sh0p")
    builder.button(text="🆘 Support", url="https://t.me/kilka38548")

    builder.button(text="🔄 Обновить", callback_data="refresh_profile")

    if uid == OWNER_ID:
        builder.button(text="🔧 Админ-панель", callback_data="admin_menu")

    builder.adjust(3, 3, 2, 1)

    try:
        if is_message:
            await msg.answer(text, reply_markup=builder.as_markup())
        else:
            await msg.edit_text(text, reply_markup=builder.as_markup())
    except Exception as e:
        print(f"Ошибка в show_profile: {e}")
        await msg.answer(text, reply_markup=builder.as_markup())

# ─── СТАРТ / ПРОФИЛЬ ─────────────────────────────────────────────────────

@router.message(Command("start", "status", "профиль"))
async def cmd_start_status(m: Message):
    args = m.text.split(maxsplit=1)
    param = args[1] if len(args) > 1 else None

    referrer_id = None
    if param and param.startswith("ref_"):
        try:
            ref_candidate = int(param[4:])
            if ref_candidate != m.from_user.id and ref_candidate > 0:
                referrer_id = ref_candidate
        except:
            pass

    u = await get_user(m.from_user.id, m.from_user)

    if referrer_id and u.get("referrer_id") is None and u.get("referral_bonus_given", 0) == 0:
        u["referrer_id"] = referrer_id
        u["referral_bonus_given"] = 1
        save_user(u)

        current = referrer_id
        level = 1
        while current and level <= 3:
            ref_u = await get_user(current)
            if not ref_u: break
            bonus = REFERRAL_BONUSES.get(level, 0)
            if bonus == 0: break

            ref_u["coins"] += bonus
            count_key = f"level{level}_count"
            ref_u[count_key] = ref_u.get(count_key, 0) + 1
            save_user(ref_u)

            await safe_send_message(current, f"+{bonus} монет (реферал {level} уровня)")

            current = ref_u.get("referrer_id")
            level += 1

    if m.from_user.is_premium:
        old_energy = u["energy"]
        u["energy"] = min(100, u["energy"] + PREMIUM_ENERGY_BONUS)
        save_user(u)
        if u["energy"] > old_energy:
            await m.answer(f"👑 Premium: +{PREMIUM_ENERGY_BONUS} энергии (теперь {u['energy']}/100)")

    if param == "ref_menu" and m.chat.type == "private":
        bot_me = await bot.get_me()
        ref_link = f"https://t.me/{bot_me.username}?start=ref_{m.from_user.id}"

        text = f"""<b>Ваша реф. ссылка Nexum Game ⚔️✨</b>

<code>{ref_link}</code>

Приглашайте друзей — бонусы по 3 уровням:
• 1 ур: +500 монет
• 2 ур: +250 монет
• 3 ур: +100 монет

Сейчас у вас:
• 1 ур: {u.get('level1_count', 0)}
• 2 ур: {u.get('level2_count', 0)}
• 3 ур: {u.get('level3_count', 0)}
"""

        builder = InlineKeyboardBuilder()
        builder.button(text="Скопировать ссылку", url=ref_link)
        builder.button(text="← В профиль", callback_data="refresh_profile")

        await m.answer(text, reply_markup=builder.as_markup())
        return

    await show_profile(m)

@router.callback_query(F.data == "refresh_profile")
async def refresh_profile(c: CallbackQuery):
    await show_profile(c)
    await c.answer("Профиль обновлён ✓")

# ─── ДУЭЛИ ───────────────────────────────────────────────────────────────

@router.message(Command("duel"))
async def cmd_duel(m: Message):
    args = m.text.split()[1:]

    if len(args) == 0:
        await m.answer("Укажи сумму и @username или ответь реплаем\nПример: /duel 100 @friend")
        return

    bet_str = args[0].lower()
    opponent_username = None

    if len(args) > 1 and args[1].startswith("@"):
        opponent_username = args[1][1:]

    if bet_str == "all":
        bet = "all"
    else:
        try:
            bet = int(bet_str)
            if bet < 10:
                await m.answer("Минимум 10 монет.")
                return
        except:
            await m.answer("Формат: /duel [сумма или all] [@username]")
            return

    opponent = None
    opponent_user = None

    if m.reply_to_message and not m.reply_to_message.from_user.is_bot:
        opponent = m.reply_to_message.from_user
        opponent_user = await get_user(opponent.id, opponent)

    elif opponent_username:
        cursor.execute("SELECT user_id FROM users WHERE username = ? COLLATE NOCASE", (opponent_username,))
        row = cursor.fetchone()
        if row:
            opponent_id = row[0]
            opponent_user = await get_user(opponent_id)
            try:
                chat = await bot.get_chat(opponent_id)
                opponent = User(id=opponent_id, is_bot=False, first_name=chat.first_name or "Unknown", username=chat.username)
            except:
                opponent = User(id=opponent_id, is_bot=False, first_name="Unknown")
        else:
            await m.answer(f"@{opponent_username} не найден в базе. Пользователь должен сначала написать боту /start.")
            return

    else:
        await m.answer("Ответь реплаем на сообщение друга или укажи @username (пользователь должен быть в базе бота).")
        return

    if opponent.id == m.from_user.id:
        await m.answer("Нельзя вызвать самого себя.")
        return

    if is_bot(opponent.id):
        await m.answer("Нельзя вызвать бота.")
        return

    creator = await get_user(m.from_user.id, m.from_user)

    if creator["energy"] < 20:
        await m.answer("Нужна энергия ≥ 20.")
        return

    duel_id = f"{creator['user_id']}_{opponent_user['user_id']}_{int(time.time())}"

    duels[duel_id] = {
        "creator_id": creator["user_id"],
        "opponent_id": opponent_user["user_id"],
        "creator_name": format_name(creator),
        "opponent_name": format_name(opponent_user),
        "bet": bet,
        "rounds_done": 0,
        "creator_score": 0,
        "opponent_score": 0,
        "current_round_moves": {},
        "status": "waiting",
        "created": time.time(),
        "chat_id": m.chat.id
    }

    builder = InlineKeyboardBuilder()
    builder.button(text="Принять ⚔️", callback_data=f"accept_{duel_id}")
    builder.button(text="Отклонить ❌", callback_data=f"decline_{duel_id}")
    builder.button(text="Отменить вызов", callback_data=f"cancel_duel_{duel_id}")
    builder.adjust(2)

    await safe_send_message(
        creator["user_id"],
        f"Вы вызвали <b>{format_name(opponent)}</b> на дуэль (3 раунда)!\nСтавка: {'all-in' if bet == 'all' else bet} монет\nОжидаем ответа...",
        reply_markup=builder.as_markup()
    )

    sent = await safe_send_message(
        opponent_user["user_id"],
        f"<b>{format_name(m.from_user)}</b> вызывает вас на дуэль (3 раунда)!\nСтавка: {'all-in' if bet == 'all' else bet} монет\nУ вас 120 секунд!",
        reply_markup=builder.as_markup()
    )

    if not sent:
        await m.answer(
            f"Вызов отправлен, но {format_name(opponent)} скорее всего не начинал диалог с ботом.\n"
            f"Ему придёт уведомление только если он уже писал боту раньше."
        )
    else:
        await m.answer(f"Вызов отправлен {format_name(opponent)} в личку! Ожидаем ответа...")

@router.callback_query(F.data.startswith("cancel_duel_"))
async def cancel_duel(c: CallbackQuery):
    duel_id = c.data[12:]
    if duel_id not in duels:
        await c.answer("Дуэль уже недоступна", show_alert=True)
        return

    duel = duels[duel_id]

    if c.from_user.id != duel["creator_id"]:
        await c.answer("Отменить может только тот, кто отправил вызов", show_alert=True)
        return

    del duels[duel_id]

    await safe_send_message(duel["creator_id"], "Вы отменили вызов на дуэль.")
    await safe_send_message(duel["opponent_id"], f"{format_name(c.from_user)} отменил вызов на дуэль.")

    await c.message.edit_text("Вызов отменён.")
    await c.answer("Вызов отменён")

@router.callback_query(F.data.startswith("accept_"))
async def accept_duel(c: CallbackQuery):
    duel_id = c.data[7:]
    if duel_id not in duels:
        await c.answer("Дуэль уже недоступна", show_alert=True)
        return

    duel = duels[duel_id]

    if c.from_user.id != duel["opponent_id"]:
        await c.answer("Это не ваш вызов", show_alert=True)
        return

    duel["status"] = "active"

    creator = await get_user(duel["creator_id"])
    opponent = await get_user(duel["opponent_id"])

    creator["energy"] = max(0, creator["energy"] - 10)
    opponent["energy"] = max(0, opponent["energy"] - 10)
    save_user(creator)
    save_user(opponent)

    builder = InlineKeyboardBuilder()
    builder.button(text="✊ Камень", callback_data=f"move_{duel_id}_rock")
    builder.button(text="✌️ Ножницы", callback_data=f"move_{duel_id}_scissors")
    builder.button(text="✋ Бумага", callback_data=f"move_{duel_id}_paper")
    builder.adjust(3)

    await safe_send_message(
        creator["user_id"],
        f"⚔️ <b>Дуэль (3 раунда) началась!</b>\nПротивник принял вызов!\nСтавка: {'all-in' if duel['bet'] == 'all' else duel['bet']} монет\n\nРаунд 1 — твой ход:",
        reply_markup=builder.as_markup()
    )

    await safe_send_message(
        opponent["user_id"],
        f"⚔️ <b>Дуэль (3 раунда) началась!</b>\nВы приняли вызов!\nСтавка: {'all-in' if duel['bet'] == 'all' else duel['bet']} монет\n\nРаунд 1 — твой ход:",
        reply_markup=builder.as_markup()
    )

    await c.message.edit_text("Вы приняли дуэль! Дуэль началась.")
    await c.answer("Дуэль принята!")

@router.callback_query(F.data.startswith("move_"))
async def make_move(c: CallbackQuery):
    parts = c.data.split("_")
    if len(parts) != 5:
        await c.answer("Ошибка кнопки", show_alert=True)
        return

    duel_id = f"{parts[1]}_{parts[2]}_{parts[3]}"
    move = parts[4]

    if duel_id not in duels:
        await c.answer("Дуэль завершена", show_alert=True)
        return

    duel = duels[duel_id]
    uid = c.from_user.id

    round_num = duel["rounds_done"] + 1

    if uid == duel["creator_id"]:
        if "creator" in duel["current_round_moves"]:
            await c.answer("Вы уже сделали ход в этом раунде", show_alert=True)
            return
        duel["current_round_moves"]["creator"] = move
    elif uid == duel["opponent_id"]:
        if "opponent" in duel["current_round_moves"]:
            await c.answer("Вы уже сделали ход в этом раунде", show_alert=True)
            return
        duel["current_round_moves"]["opponent"] = move
    else:
        await c.answer("Вы не участник этой дуэли", show_alert=True)
        return

    await c.answer(f"Вы выбрали: {move.capitalize()} (раунд {round_num})")

    if len(duel["current_round_moves"]) == 2:
        cm = duel["current_round_moves"]["creator"]
        om = duel["current_round_moves"]["opponent"]

        if cm == om:
            round_result = "🤝 Ничья в раунде!"
        elif (cm == "rock" and om == "scissors") or \
             (cm == "scissors" and om == "paper") or \
             (cm == "paper" and om == "rock"):
            duel["creator_score"] += 1
            round_result = f"🏆 Раунд выиграл {duel['creator_name']}!"
        else:
            duel["opponent_score"] += 1
            round_result = f"🏆 Раунд выиграл {duel['opponent_name']}!"

        animation_frames = [
            f"Раунд {round_num} начинается... ✊✌️✋",
            f"Игрок 1 выбрал: {cm.capitalize()}",
            f"Игрок 2 выбрал: {om.capitalize()}",
            "Столкновение! 💥",
            round_result,
            f"Счёт: {duel['creator_score']} : {duel['opponent_score']}"
        ]

        for user_id in [duel["creator_id"], duel["opponent_id"]]:
            try:
                msg = await bot.send_message(user_id, "⚔️ Готовимся к результату раунда...")
                for frame in animation_frames:
                    await asyncio.sleep(1.2)
                    try:
                        await msg.edit_text(frame)
                    except:
                        pass
                await msg.edit_text(f"Раунд {round_num} завершён!\n{round_result}\nСчёт: {duel['creator_score']} : {duel['opponent_score']}")
            except:
                pass

        duel["rounds_done"] += 1
        duel["current_round_moves"] = {}

        if duel["rounds_done"] == 3:
            if duel["creator_score"] > duel["opponent_score"]:
                winner = "creator"
                final_result = f"🏆 Победил {duel['creator_name']} ({duel['creator_score']}:{duel['opponent_score']})!"
            elif duel["opponent_score"] > duel["creator_score"]:
                winner = "opponent"
                final_result = f"🏆 Победил {duel['opponent_name']} ({duel['creator_score']}:{duel['opponent_score']})!"
            else:
                winner = None
                final_result = f"🤝 Ничья! ({duel['creator_score']}:{duel['opponent_score']})"

            final_frames = [
                "⚔️ Финал дуэли...",
                "Подсчёт очков...",
                final_result
            ]

            for user_id in [duel["creator_id"], duel["opponent_id"]]:
                try:
                    msg = await bot.send_message(user_id, "⚔️ Финальный подсчёт...")
                    for frame in final_frames:
                        await asyncio.sleep(1.5)
                        await msg.edit_text(frame)
                except:
                    pass

            await finish_multi_round_duel(duel_id, duel, winner)
            return

        builder = InlineKeyboardBuilder()
        builder.button(text="✊ Камень", callback_data=f"move_{duel_id}_rock")
        builder.button(text="✌️ Ножницы", callback_data=f"move_{duel_id}_scissors")
        builder.button(text="✋ Бумага", callback_data=f"move_{duel_id}_paper")
        builder.adjust(3)

        await safe_send_message(
            duel["creator_id"],
            f"Раунд {duel['rounds_done']+1} — твой ход!",
            reply_markup=builder.as_markup()
        )

        await safe_send_message(
            duel["opponent_id"],
            f"Раунд {duel['rounds_done']+1} — твой ход!",
            reply_markup=builder.as_markup()
        )

async def finish_multi_round_duel(duel_id, duel, winner):
    creator = await get_user(duel["creator_id"])
    opponent = await get_user(duel["opponent_id"])

    bet = duel["bet"]
    bet_amount = bet if isinstance(bet, int) else creator["coins"]

    if winner == "creator":
        creator["coins"] += bet_amount
        opponent["coins"] -= bet_amount
        creator["won_bets"] += 1
        opponent["lost_bets"] += 1
    elif winner == "opponent":
        opponent["coins"] += bet_amount
        creator["coins"] -= bet_amount
        opponent["won_bets"] += 1
        creator["lost_bets"] += 1

    save_user(creator)
    save_user(opponent)

    result_text = f"Дуэль (3 раунда) завершена!\nСчёт: {duel['creator_score']} : {duel['opponent_score']}\n"
    if winner == "creator":
        result_text += f"🏆 Победил {duel['creator_name']}!\n+{bet_amount} монет"
    elif winner == "opponent":
        result_text += f"🏆 Победил {duel['opponent_name']}!\n+{bet_amount} монет"
    else:
        result_text += "🤝 Ничья! Ставка возвращена."

    await safe_send_message(creator["user_id"], result_text)
    await safe_send_message(opponent["user_id"], result_text)

    del duels[duel_id]

# ─── ПОПОЛНЕНИЕ ЗА STARS ─────────────────────────────────────────────────

@router.callback_query(F.data.in_({"buy5", "buy10", "buy15"}))
async def buy_stars_callback(c: CallbackQuery):
    amount = int(c.data.replace("buy", ""))
    prices = [LabeledPrice(label="Пополнение гемов", amount=amount)]

    await bot.send_invoice(
        chat_id=c.from_user.id,
        title="Пополнение гемов 💎",
        description=f"{amount} ⭐ → {amount * STARS_TO_GEMS_RATE:,} гемов",
        payload=f"gem_topup_{amount}_{c.from_user.id}",
        provider_token="",
        currency="XTR",
        prices=prices,
        need_name=False,
        need_phone_number=False,
        need_email=False,
        need_shipping_address=False,
        is_flexible=False
    )
    await c.answer(f"Открываем оплату {amount} ⭐...", show_alert=False)

@router.pre_checkout_query()
async def pre_checkout_handler(pre_checkout: PreCheckoutQuery):
    await bot.answer_pre_checkout_query(pre_checkout.id, ok=True)

@router.message(F.successful_payment)
async def handle_successful_payment(m: Message):
    stars_received = m.successful_payment.total_amount
    user_id = m.from_user.id
    u = await get_user(user_id, m.from_user)

    gems_earned = stars_received * STARS_TO_GEMS_RATE

    u["gems"] += gems_earned
    save_user(u)

    await m.answer(
        f"🌟 <b>Спасибо за поддержку!</b>\n\n"
        f"Получено <b>{stars_received}</b> ⭐\n"
        f"Начислено <b>+{gems_earned:,}</b> гемов 💎\n\n"
        f"Твой баланс гемов: <b>{u['gems']:,}</b>\n\n"
        f"Используй их в магазине за гемы!"
    )

    if MOD_LOG_CHAT_ID:
        await safe_send_message(
            MOD_LOG_CHAT_ID,
            f"💰 Донат от {format_name(m.from_user)}: "
            f"{stars_received} ⭐ → +{gems_earned:,} гемов"
        )

@router.callback_query(F.data == "donate_info")
async def donate_info(c: CallbackQuery):
    text = f"""💎 <b>Пополнение гемов за Stars</b>

Курс: 1 ⭐ = {STARS_TO_GEMS_RATE} гемов 💎

Выберите сумму:"""

    builder = InlineKeyboardBuilder()
    builder.button(text="5 ⭐ → 500 гемов", callback_data="buy5")
    builder.button(text="10 ⭐ → 1000 гемов", callback_data="buy10")
    builder.button(text="15 ⭐ → 1500 гемов", callback_data="buy15")
    builder.button(text="← Назад в профиль", callback_data="refresh_profile")
    builder.adjust(1, 1, 1, 1)

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

# ─── DAILY ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "daily")
@router.message(Command("daily"))
async def cmd_daily(event):
    is_cb = isinstance(event, CallbackQuery)
    m = event.message if is_cb else event

    uid = m.from_user.id
    u = await get_user(uid, m.from_user)

    reset_daily_if_needed()

    now = time.time()
    if now - u["last_daily"] < 86400:
        rem = 86400 - (now - u["last_daily"])
        h = int(rem // 3600)
        mi = int((rem % 3600) // 60)
        await m.answer(f"⏳ Следующая ежедневка через ~{h}ч {mi}мин")
        if is_cb:
            await event.answer()
        return

    base = random.randint(50, 150)
    mult = 2 if now < u.get("double_daily_until", 0) else 1
    reward = base * mult

    u["coins"] += reward
    u["last_daily"] = now
    if mult == 2:
        u["double_daily_until"] = 0

    text = f"""🌅 <b>Ежедневная награда!</b>

+<b>{reward}</b> монет 💰"""

    if m.from_user.is_premium:
        bonus = random.randint(PREMIUM_DAILY_BONUS_MIN, PREMIUM_DAILY_BONUS_MAX)
        u["coins"] += bonus
        text += f"\n👑 Premium бонус: +{bonus}"

    text += "\n\nУдачи в дуэлях! ⚔️"

    save_user(u)
    await m.answer(text)

    if is_cb:
        await event.answer()
        await show_profile(event)

# ─── МАГАЗИН ЗА ГЕМЫ ────────────────────────────────────────────────────

GEM_SHOP_ITEMS = [
    {"id": "energy_50", "name": "+50 энергии ⚡", "gems": 300, "desc": "Восстановление энергии"},
    {"id": "coins_5000", "name": "+5 000 монет 💰", "gems": 400, "desc": "Пополнение монет"},
    {"id": "double_daily", "name": "×2 Daily ×1 💰", "gems": 800, "desc": "Следующая ежедневка удвоится"},
]

@router.callback_query(F.data == "gemshop")
async def show_gem_shop(c: CallbackQuery):
    uid = c.from_user.id
    u = await get_user(uid)

    builder = InlineKeyboardBuilder()
    for item in GEM_SHOP_ITEMS:
        builder.button(text=f"{item['name']} — {item['gems']:,} 💎", callback_data=f"buy_gem_{item['id']}")
    builder.button(text="← Назад", callback_data="refresh_profile")
    builder.adjust(1)

    text = f"""💎 <b>Магазин за гемы</b>

Твой баланс: <b>{u.get('gems', 0):,}</b> гемов

""" + "\n".join(f"• <b>{i['name']}</b> — {i['gems']:,} гемов\n  {i['desc']}\n" for i in GEM_SHOP_ITEMS)

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("buy_gem_"))
async def buy_gem_item(c: CallbackQuery):
    item_id = c.data[8:]
    item = next((x for x in GEM_SHOP_ITEMS if x["id"] == item_id), None)
    if not item:
        await c.answer("Товар не найден", show_alert=True)
        return

    uid = c.from_user.id
    u = await get_user(uid)

    if u.get('gems', 0) < item["gems"]:
        await c.answer(f"Недостаточно гемов! Нужно {item['gems']:,}", show_alert=True)
        return

    u["gems"] -= item["gems"]

    msg = ""
    if item["id"] == "energy_50":
        u["energy"] = min(100, u["energy"] + 50)
        msg = f"+50 энергии ⚡ (теперь {u['energy']}/100)"
    elif item["id"] == "coins_5000":
        u["coins"] += 5000
        msg = "+5 000 монет 💰"
    elif item["id"] == "double_daily":
        u["double_daily_until"] = time.time() + 86400
        msg = "Следующая ежедневка ×2 💰"

    save_user(u)

    await c.answer(f"Куплено!\n{msg}", show_alert=True)
    await show_gem_shop(c)

# ─── МАГАЗИН ЗА МОНЕТЫ ──────────────────────────────────────────────────

SHOP_ITEMS = [
    {"id": "small_energy", "name": "Малый энергетик ⚡", "price": 40, "desc": "+20 энергии"},
    {"id": "energy", "name": "Энергетик ⚡", "price": 80, "desc": "+40 энергии"},
    {"id": "elixir_energy", "name": "Эликсир энергии 💉", "price": 200, "desc": "+60 энергии"},
    {"id": "lucky", "name": "Талисман 🍀", "price": 250, "desc": "+20% шанс (24ч)"},
    {"id": "lucky_ticket", "name": "Счастливый билет 🎟️", "price": 600, "desc": "+30% шанс на 3 дуэли"},
    {"id": "safe", "name": "Защита all-in 🛡️", "price": 150, "desc": "all-in оставит 50 монет (24ч)"},
    {"id": "luck_shield", "name": "Щит удачи 🛡️✨", "price": 800, "desc": "Защита all-in 1 раз (оставит 100)"},
    {"id": "double", "name": "×2 daily 💰", "price": 400, "desc": "Следующий daily ×2 (24ч)"},
    {"id": "coin_rain", "name": "Монетный дождь 🌧️💰", "price": 1200, "desc": "Удвоение следующей daily"},
]

@router.callback_query(F.data == "shop")
async def show_shop(c: CallbackQuery):
    uid = c.from_user.id
    u = await get_user(uid)

    builder = InlineKeyboardBuilder()
    for item in SHOP_ITEMS:
        builder.button(text=f"{item['name']} · {item['price']:,} 💰", callback_data=f"buy_{item['id']}")
    builder.button(text="← Назад", callback_data="refresh_profile")
    builder.adjust(1)

    text = f"""🛒 <b>Магазин за монеты</b>

""" + "\n".join(f"• <b>{i['name']}</b> — {i['price']:,} монет\n  {i['desc']}\n" for i in SHOP_ITEMS) + f"""
Ваши монеты: <b>{u['coins']:,}</b>"""

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("buy_"))
async def buy_item(c: CallbackQuery):
    item_id = c.data[4:]
    item = next((x for x in SHOP_ITEMS if x["id"] == item_id), None)
    if not item:
        await c.answer("Товар не найден", show_alert=True)
        return

    uid = c.from_user.id
    u = await get_user(uid)

    if u["coins"] < item["price"]:
        await c.answer(f"Недостаточно монет! Нужно {item['price']:,}", show_alert=True)
        return

    u["coins"] -= item["price"]
    now = time.time()
    duration_24h = 86400
    duration_72h = 86400 * 3

    msg = ""

    if item["id"] == "small_energy":
        u["energy"] = min(100, u["energy"] + 20)
        msg = f"+20 энергии ⚡"
    elif item["id"] == "energy":
        u["energy"] = min(100, u["energy"] + 40)
        msg = f"+40 энергии ⚡"
    elif item["id"] == "elixir_energy":
        u["energy"] = min(100, u["energy"] + 60)
        msg = f"+60 энергии 💉"
    elif item["id"] == "lucky":
        u["lucky_charm_until"] = now + duration_24h
        msg = "Талисман +20% на 24ч 🍀"
    elif item["id"] == "lucky_ticket":
        u["lucky_charm_until"] = now + duration_72h
        msg = "Счастливый билет +30% на 3 дуэли 🎟️"
    elif item["id"] == "safe":
        u["safe_all_until"] = now + duration_24h
        msg = "Защита all-in 🛡️"
    elif item["id"] == "luck_shield":
        u["luck_shield"] = u.get("luck_shield", 0) + 1
        msg = "Щит удачи +1 🛡️✨"
    elif item["id"] == "double":
        u["double_daily_until"] = now + duration_24h
        msg = "×2 daily активирован 💰"
    elif item["id"] == "coin_rain":
        u["double_daily_until"] = now + duration_72h
        msg = "Монетный дождь — daily ×2 🌧️💰"

    save_user(u)
    await c.answer(f"Куплено!\n{msg}", show_alert=True)
    await show_shop(c)

# ─── ТОПЫ ───────────────────────────────────────────────────────────────

@router.callback_query(F.data == "top_menu")
async def top_menu(c: CallbackQuery):
    builder = InlineKeyboardBuilder()
    builder.button(text="Глобальный топ по монетам 🏆", callback_data="show_top")
    builder.button(text="Ежедневный топ 🔥", callback_data="show_dailytop")
    builder.button(text="← Назад", callback_data="refresh_profile")
    builder.adjust(1)

    await c.message.edit_text("Выберите лидерборд Nexum Game ⚔️✨:", reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data == "show_top")
async def show_global_top(c: CallbackQuery):
    cursor.execute("SELECT user_id, coins, username, first_name FROM users ORDER BY coins DESC LIMIT 10")
    rows = cursor.fetchall()

    if not rows:
        text = "🏆 Глобальный топ пока пуст... стань первым!"
    else:
        lines = ["<b>🏆 Глобальный топ-10 по монетам</b>\n"]
        medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]

        for i, r in enumerate(rows):
            name = f"@{r['username']}" if r['username'] else (r['first_name'] or f"ID {r['user_id']}")
            lines.append(f"{medals[i]} {name} — <b>{r['coins']:,}</b> 💰")

        text = "\n".join(lines)

    builder = InlineKeyboardBuilder()
    builder.button(text="Обновить 🔄", callback_data="show_top")
    builder.button(text="← Назад", callback_data="top_menu")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data == "show_dailytop")
async def show_dailytop(c: CallbackQuery):
    reset_daily_if_needed()
    today = date.today().isoformat()

    cursor.execute("""
        SELECT user_id, today_wins, username, first_name
        FROM users WHERE last_reset_date = ?
        ORDER BY today_wins DESC LIMIT 10
    """, (today,))
    rows = cursor.fetchall()

    if not rows:
        text = f"<b>🔥 Ежедневный топ — {today}</b>\n\nПока никто не побеждал!"
    else:
        lines = [f"<b>🔥 Ежедневный топ — {today}</b>\n"]
        medals = ["🥇", "🥈", "🥉"] + [f"{i}." for i in range(4, 11)]

        for i, r in enumerate(rows):
            name = f"@{r['username']}" if r['username'] else (r['first_name'] or f"ID {r['user_id']}")
            lines.append(f"{medals[i]} {name} — <b>{r['today_wins']}</b> побед")

        text = "\n".join(lines)

    builder = InlineKeyboardBuilder()
    builder.button(text="Обновить 🔄", callback_data="show_dailytop")
    builder.button(text="← Назад", callback_data="top_menu")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

# ─── TOP / ТОПЫ (быстрый) ───────────────────────────────────────────────

@router.message(Command("top", "tops", "топ", "лидерборд"))
async def cmd_top(m: Message):
    # Глобальный топ-5 по монетам
    cursor.execute("SELECT user_id, coins, username, first_name FROM users ORDER BY coins DESC LIMIT 5")
    global_top = cursor.fetchall()

    # Ежедневный топ-5 по победам
    today = date.today().isoformat()
    cursor.execute("""
        SELECT user_id, today_wins, username, first_name
        FROM users WHERE last_reset_date = ?
        ORDER BY today_wins DESC LIMIT 5
    """, (today,))
    daily_top = cursor.fetchall()

    text = "<b>🏆 Быстрый топ Nexum Game</b>\n\n"

    text += "<b>🌍 Глобальный топ (монеты)</b>\n"
    if not global_top:
        text += "Топ пуст... стань первым! 🔥\n"
    else:
        for i, row in enumerate(global_top, 1):
            name = f"@{row['username']}" if row['username'] else (row['first_name'] or f"ID {row['user_id']}")
            medal = ["🥇", "🥈", "🥉", "4.", "5."][i-1]
            text += f"{medal} {name} — <b>{row['coins']:,}</b> 💰\n"

    text += "\n<b>🔥 Сегодняшний топ (победы)</b>\n"
    if not daily_top:
        text += "Сегодня никто не побеждал... начни! ⚔️\n"
    else:
        for i, row in enumerate(daily_top, 1):
            name = f"@{row['username']}" if row['username'] else (row['first_name'] or f"ID {row['user_id']}")
            medal = ["🥇", "🥈", "🥉", "4.", "5."][i-1]
            text += f"{medal} {name} — <b>{row['today_wins']}</b> побед\n"

    text += "\nПолные топы — кнопка «🏆 Топы» в профиле (/start)"

    builder = InlineKeyboardBuilder()
    builder.button(text="🏆 Полные топы", callback_data="top_menu")
    builder.button(text="👤 В профиль", callback_data="refresh_profile")

    await m.answer(text, reply_markup=builder.as_markup())

# ─── HELP / ПОМОЩЬ ───────────────────────────────────────────────────────

@router.message(Command("help", "помощь", "h"))
async def cmd_help(m: Message):
    text = f"""<b>⚔️ Nexum Game — Помощь</b>

Основные команды:
/start — профиль
/daily — ежедневка
/duel [сумма/all] — дуэль (3 раунда)
/top или /топ — быстрый топ-5
/help — эта справка

В группах:
/ban /kick /mute [время] /unban /unmute /warn

Выбери раздел ниже ↓"""

    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="👤 Профиль", callback_data="refresh_profile"),
        InlineKeyboardButton(text="⚔️ Дуэли", callback_data="help_duels")
    )
    builder.row(
        InlineKeyboardButton(text="🛒 Магазины", callback_data="help_shops"),
        InlineKeyboardButton(text="🏆 Топы", callback_data="help_tops")
    )
    builder.row(
        InlineKeyboardButton(text="💎 Пополнение", callback_data="help_donate"),
        InlineKeyboardButton(text="👥 Рефералы", callback_data="help_referrals")
    )
    builder.row(
        InlineKeyboardButton(text="🔙 В профиль", callback_data="refresh_profile")
    )

    await m.answer(text, reply_markup=builder.as_markup())

@router.callback_query(F.data.startswith("help_"))
async def help_sections(c: CallbackQuery):
    data = c.data

    texts = {
        "help_duels": """<b>⚔️ Дуэли</b>
• /duel 500 или /duel all
• Реплай или @username
• 3 раунда с анимацией
• Победитель забирает ставку
• Отмена вызова — кнопка""",

        "help_shops": """<b>🛒 Магазины</b>
• За монеты: энергия, талисманы, защита all-in, ×2 daily
• За гемы: +энергия, +монеты, ×2 daily
Гемы — через «Пополнить»""",

        "help_tops": """<b>🏆 Топы</b>
• Глобальный — по монетам
• Ежедневный — по победам за день

Смотри, на каком месте! 🌟""",

        "help_donate": """<b>💎 Пополнение</b>
• Кнопка «⭐ Пополнить»
• 5/10/15 ⭐ → гемы (1⭐ = 100 гемов)
• Трать в «Магазине гемов»""",

        "help_referrals": """<b>👥 Рефералы</b>
• Приглашай по ссылке
• Бонусы 3 уровня:
  1 ур → +500
  2 ур → +250
  3 ур → +100

Ссылка — в профиле"""
    }

    text = texts.get(data, "Раздел не найден 😅")

    builder = InlineKeyboardBuilder()
    builder.button(text="🔙 Назад в помощь", callback_data="help")
    builder.button(text="👤 В профиль", callback_data="refresh_profile")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

# ─── О БОТЕ ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "about_bot")
async def show_about_bot(c: CallbackQuery):
    text = f"""<b>ℹ️ О боте Nexum Game ⚔️✨</b>

• Дуэли 1 на 1 (3 раунда с анимацией)
• Ставки монетами
• Ежедневные награды + бонусы Premium
• Магазины бустов
• Реферальная система (3 уровня)
• Глобальные и ежедневные топы
• Система уровней за победы
• Бусты, защита all-in, талисманы и т.д.

<b>Версия:</b> 2.6
<b>Создатель:</b> {OWNER_USERNAME}

Наслаждайся и побеждай! ⚔️🔥✨"""

    builder = InlineKeyboardBuilder()
    builder.button(text="← Назад", callback_data="refresh_profile")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

# ─── ИНСТРУКЦИЯ ──────────────────────────────────────────────────────────

@router.callback_query(F.data == "instruction")
async def show_instruction(c: CallbackQuery):
    text = f"""<b>📖 Инструкция Nexum Game ⚔️✨</b>

/start — профиль
/daily — ежедневка
/duel [сумма/all] — дуэль (3 раунда)
  В ЛС: реплай или @username (если писал боту)
  В группе: реплай или @username
/top — быстрый топ-5
Пополнить гемы — кнопка «Пополнить»
Магазины — кнопки «Магазин» и «Магазин гемов»
Топы — кнопка «Топы»
Рефералка — кнопка «Реф. ссылка»

Приятной игры! ⚔️"""

    builder = InlineKeyboardBuilder()
    builder.button(text="← Назад", callback_data="refresh_profile")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await c.answer()

# ─── АДМИН-ПАНЕЛЬ ───────────────────────────────────────────────────────

@router.callback_query(F.data == "admin_menu")
async def admin_menu(c: CallbackQuery):
    if c.from_user.id != OWNER_ID:
        await c.answer("Доступ запрещён", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    builder.button(text="➕ Добавить монеты 💰", callback_data="admin_add_coins")
    builder.button(text="➖ Отнять монеты 💰", callback_data="admin_remove_coins")
    builder.button(text="➕ Добавить гемы 💎", callback_data="admin_add_gems")
    builder.button(text="➖ Отнять гемы 💎", callback_data="admin_remove_gems")
    builder.button(text="➕ Добавить энергию ⚡", callback_data="admin_add_energy")
    builder.button(text="➖ Отнять энергию ⚡", callback_data="admin_remove_energy")
    builder.button(text="← Назад в профиль", callback_data="refresh_profile")
    builder.adjust(2)

    await c.message.edit_text("🔧 <b>Админ-панель Nexum Game ⚔️✨</b>\nВыберите действие:", reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data.in_({"admin_add_coins", "admin_remove_coins", "admin_add_gems", "admin_remove_gems", "admin_add_energy", "admin_remove_energy"}))
async def admin_edit_start(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != OWNER_ID:
        await c.answer("Нет доступа", show_alert=True)
        return

    action = c.data.replace("admin_", "")
    await state.update_data(edit_action=action)

    cursor.execute("SELECT user_id, username, first_name, coins, gems, energy FROM users ORDER BY last_active DESC LIMIT 30")
    users = cursor.fetchall()

    builder = InlineKeyboardBuilder()
    for user in users:
        name = f"@{user['username']}" if user['username'] else (user['first_name'] or f"ID {user['user_id']}")
        builder.button(text=name, callback_data=f"admin_select_{user['user_id']}")
    builder.button(text="← Назад", callback_data="admin_menu")
    builder.adjust(2)

    await c.message.edit_text(f"Выберите пользователя для {action.replace('_', ' ')}:", reply_markup=builder.as_markup())
    await c.answer()

@router.callback_query(F.data.startswith("admin_select_"))
async def admin_select(c: CallbackQuery, state: FSMContext):
    if c.from_user.id != OWNER_ID:
        return

    uid = int(c.data.split("_")[-1])
    user = await get_user(uid)
    if not user:
        await c.answer("Пользователь не найден", show_alert=True)
        return

    name = format_name(user)
    data = await state.get_data()
    action = data.get("edit_action", "")

    text = f"""Выбран: <b>{name}</b> (ID: {uid})

Текущие значения:
• Монеты: {user['coins']:,}
• Гемы: {user.get('gems', 0):,}
• Энергия: {user['energy']}/100
• Уровень: {user['level']}

Введите сумму (положительное — добавить, отрицательное — отнять):

/cancel — отмена"""

    builder = InlineKeyboardBuilder()
    builder.button(text="Отмена", callback_data="admin_menu")

    await c.message.edit_text(text, reply_markup=builder.as_markup())
    await state.update_data(target_uid=uid, target_name=name)
    await state.set_state(AdminEditStates.waiting_for_amount)
    await c.answer()

@router.message(AdminEditStates.waiting_for_amount)
async def admin_amount(m: Message, state: FSMContext):
    if m.from_user.id != OWNER_ID:
        return

    text = m.text.strip()
    if not text.lstrip("-").isdigit():
        await m.answer("Введите число (можно с минусом)")
        return

    amount = int(text)
    data = await state.get_data()
    uid = data.get("target_uid")
    action = data.get("edit_action")

    u = await get_user(uid)
    if not u:
        await m.answer("Пользователь не найден")
        await state.clear()
        return

    await state.update_data(amount=amount)

    text = f"""Сумма: {amount:+,}

Введите причину изменения (обязательно):

/cancel — отмена"""

    builder = InlineKeyboardBuilder()
    builder.button(text="Отмена", callback_data="admin_menu")

    await m.answer(text, reply_markup=builder.as_markup())
    await state.set_state(AdminEditStates.waiting_for_reason)

@router.message(AdminEditStates.waiting_for_reason)
async def admin_reason(m: Message, state: FSMContext):
    if m.from_user.id != OWNER_ID:
        return

    reason = m.text.strip()
    if not reason or reason.lower() == "/cancel":
        await m.answer("Отменено")
        await state.clear()
        return

    data = await state.get_data()
    uid = data.get("target_uid")
    amount = data.get("amount")
    action = data.get("edit_action")
    name = data.get("target_name", f"ID {uid}")

    u = await get_user(uid)

    field = ""
    old = 0
    new = 0

    if "coins" in action:
        field = "монет"
        old = u["coins"]
        new = max(0, old + amount) if amount < 0 else old + amount
        u["coins"] = new
    elif "gems" in action:
        field = "гемов"
        old = u.get("gems", 0)
        new = max(0, old + amount) if amount < 0 else old + amount
        u["gems"] = new
    elif "energy" in action:
        field = "энергии"
        old = u["energy"]
        new = max(0, min(100, old + amount))
        u["energy"] = new

    save_user(u)

    log_text = f"Админ-изменение: {name} | {action} | {old:,} → {new:,} {field} | Причина: {reason}"
    if MOD_LOG_CHAT_ID:
        await safe_send_message(MOD_LOG_CHAT_ID, log_text)

    await m.answer(f"Изменено! {field.capitalize()}: {old:,} → {new:,}\nПричина: {reason}")
    await state.clear()

@router.message(Command("cancel"), AdminEditStates)
async def admin_cancel(m: Message, state: FSMContext):
    if m.from_user.id != OWNER_ID:
        return
    await state.clear()
    await m.answer("Отменено")
    await admin_menu(m)

# ─── МОДЕРАЦИЯ ──────────────────────────────────────────────────────────

@router.message(F.reply_to_message, Command(commands=["ban", "kick", "mute", "unban", "unmute", "warn"], prefix="/!"))
async def group_moderation(m: Message):
    if m.chat.type not in ("group", "supergroup"):
        await m.answer("Эти команды работают только в группах и супергруппах.")
        return

    if not await is_group_admin(m.chat.id, m.from_user.id):
        await m.answer("Только администраторы группы могут использовать эту команду.")
        return

    try:
        bot_member = await bot.get_chat_member(m.chat.id, BOT_ID)
        if not isinstance(bot_member, (ChatMemberAdministrator, ChatMemberOwner)):
            await m.answer("Я не администратор в этой группе. Дайте мне права.")
            return
    except Exception as e:
        await m.answer("Не удалось проверить мои права в группе.")
        return

    target = m.reply_to_message.from_user
    if not target or target.id == BOT_ID or target.id == m.from_user.id:
        await m.answer("Нельзя применить команду к себе или к боту.")
        return

    command = m.text.split()[0].lstrip("!/").lower()
    args = m.text.split(maxsplit=2)
    reason = " ".join(args[2:]) if len(args) > 2 else "без причины"

    duration = None
    if command == "mute":
        duration_str = args[1] if len(args) > 1 else ""
        duration = parse_duration(duration_str)
        if duration is None:
            await m.answer("Формат времени для /mute: 1h 30m 2d 3600s\nПримеры: 1d, 2h30m, 3600")
            return

    try:
        if command in ("ban", "kick"):
            until_date = None if command == "kick" else 0
            await bot.ban_chat_member(m.chat.id, target.id, until_date=until_date)
            action_text = "забанил" if command == "ban" else "кикнул"

        elif command == "mute":
            until_date = int(time.time()) + duration
            await bot.restrict_chat_member(
                m.chat.id,
                target.id,
                permissions={"can_send_messages": False},
                until_date=until_date
            )
            action_text = f"замьютил на {timedelta(seconds=duration)}"

        elif command == "unban":
            await bot.unban_chat_member(m.chat.id, target.id, only_if_banned=True)
            action_text = "разбанил"

        elif command == "unmute":
            await bot.restrict_chat_member(
                m.chat.id,
                target.id,
                permissions={"can_send_messages": True}
            )
            action_text = "размьютил"

        elif command == "warn":
            action_text = "предупредил"

        await m.answer(
            f"{action_text.capitalize()} {format_name(target)}\n"
            f"Админ: {format_name(m.from_user)}\n"
            f"Причина: {reason}"
        )

        if MOD_LOG_CHAT_ID:
            log_text = f"[{m.chat.title}] {action_text.upper()} | {format_name(target)} | админ {format_name(m.from_user)} | причина: {reason}"
            if duration:
                log_text += f" | длительность: {timedelta(seconds=duration)}"
            await safe_send_message(MOD_LOG_CHAT_ID, log_text)

    except TelegramBadRequest as e:
        error_str = str(e).lower()
        if "user is not a member" in error_str or "user not found" in error_str:
            await m.answer("Пользователь не найден в группе или уже не участник.")
        elif "not enough rights" in error_str:
            await m.answer("У меня недостаточно прав для этого действия.")
        else:
            await m.answer(f"Ошибка Telegram: {str(e)}")
    except Exception as e:
        await m.answer(f"Произошла ошибка: {str(e)}")
        print(f"Moderation error: {e}")

# ─── ЗАПУСК ─────────────────────────────────────────────────────────────

async def main():
    dp.include_router(router)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())