import asyncio
import logging
import json
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Optional
from collections import deque
import requests
import pytz
from bs4 import BeautifulSoup
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.exceptions import TelegramNetworkError
from aiohttp import ClientTimeout, TCPConnector, web
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from groq import Groq

# ========== НАСТРОЙКИ (из переменных окружения) ==========
BOT_TOKEN = os.getenv("BOT_TOKEN")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
YOUR_CHAT_ID = int(os.getenv("YOUR_CHAT_ID", 992586773))  # можно задать через переменную окружения

# Проверка наличия обязательных переменных
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в переменных окружения!")
if not GROQ_API_KEY:
    raise ValueError("❌ GROQ_API_KEY не задан в переменных окружения!")

# Настройки (можно тоже вынести в переменные окружения)
CHECK_INTERVAL_SECONDS = int(os.getenv("CHECK_INTERVAL_SECONDS", 300))  # 5 минут
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", 10))
POSTS_FETCH_LIMIT = int(os.getenv("POSTS_FETCH_LIMIT", 30))
MAX_CONCURRENT_CHECKS = int(os.getenv("MAX_CONCURRENT_CHECKS", 3))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 2))

# Определяем окружение
IS_PRODUCTION = bool(os.environ.get('RENDER_EXTERNAL_HOSTNAME') or os.environ.get('BOTHOST_HOSTNAME'))

# Инициализация Groq
groq_client = Groq(api_key=GROQ_API_KEY)
USE_AI = True

# Список каналов (username без @)
CHANNELS = [
    "minterbez_permkrai", "mud_no", "tass_agency", "mod_russia", "radarrussiia",
    "minprosrf", "permvkurse", "newskompanion", "uranews", "favt_info",
    "weather_GIS_psu", "rbc_perm", "mahonin59", "mc_holidays", "eduardsosnin",
    "transportperm", "minprirodaperm", "minstroy_perm", "minzdrav_permkrai",
    "permadmin", "mintranspermkrai", "minobrperm", "zspermkrai", "prokpermkrai",
    "gibdd_159", "chp_159_59", "parmabasketprm", "e1_news", "news59ru"
]

CHANNEL_NAMES = {
    "minterbez_permkrai": "Минтербез Пермского края",
    "mud_no": "Properm.ru",
    "tass_agency": "ТАСС",
    "mod_russia": "Минобороны России",
    "radarrussiia": "Радар по всей России | БПЛА, ракеты",
    "minprosrf": "Минпросвещения России",
    "permvkurse": "В курсе.ру | Новости Перми",
    "newskompanion": "Новый компаньон",
    "uranews": "URA.RU",
    "favt_info": "Говорит Росавиация",
    "weather_GIS_psu": "Опасные природные явления ПК",
    "rbc_perm": "РБК Пермь | Новости",
    "mahonin59": "Дмитрий Махонин",
    "mc_holidays": "Какой сегодня праздник",
    "eduardsosnin": "Эдуард Соснин",
    "transportperm": "Пермский транспорт",
    "minprirodaperm": "Минприроды Пермского края",
    "minstroy_perm": "Минстрой Пермского края",
    "minzdrav_permkrai": "Минздрав Пермского края",
    "permadmin": "Администрация города Перми",
    "mintranspermkrai": "Минтранс Пермского края",
    "minobrperm": "Минобр Пермского края",
    "zspermkrai": "Заксобрание Пермского края",
    "prokpermkrai": "Прокуратура Пермского края",
    "gibdd_159": "ГИБДД Пермского края",
    "chp_159_59": "ЧП Пермь",
    "parmabasketprm": "БК «ПАРМА» | Баскетбол",
    "e1_news": "E1.RU | Новости Екатеринбурга",
    "news59ru": "59.RU | Новости Перми"
}

# ========== БАЗА ДАННЫХ ==========
def init_db():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS sent_posts
                 (channel TEXT, post_id TEXT, sent_at TIMESTAMP,
                 PRIMARY KEY (channel, post_id))''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_sent_at ON sent_posts (sent_at)')
    conn.commit()
    conn.close()

def mark_as_sent(channel: str, post_id: str):
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    now = datetime.now(pytz.timezone('Asia/Yekaterinburg')).isoformat()
    c.execute("INSERT OR REPLACE INTO sent_posts (channel, post_id, sent_at) VALUES (?, ?, ?)",
              (channel, post_id, now))
    conn.commit()
    conn.close()

def is_sent(channel: str, post_id: str) -> bool:
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    c.execute("SELECT 1 FROM sent_posts WHERE channel = ? AND post_id = ?", (channel, post_id))
    result = c.fetchone() is not None
    conn.close()
    return result

def cleanup_old_posts():
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    time_threshold = (datetime.now(pytz.timezone('Asia/Yekaterinburg')) - timedelta(minutes=WINDOW_MINUTES)).isoformat()
    c.execute("DELETE FROM sent_posts WHERE sent_at < ?", (time_threshold,))
    deleted = c.rowcount
    conn.commit()
    conn.close()
    if deleted:
        console_print(f"🧹 Очищено {deleted} старых записей из БД")

init_db()

# ========== КОНТРОЛЬ ПАРАЛЛЕЛИЗМА ==========
semaphore = asyncio.Semaphore(MAX_CONCURRENT_CHECKS)

# ========== ЛОГИРОВАНИЕ ==========
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def console_print(msg):
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

# ========== УСТОЙЧИВАЯ СЕССИЯ ==========
session = requests.Session()
retry_strategy = Retry(total=MAX_RETRIES, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
session.mount("https://", adapter)
session.mount("http://", adapter)

def fetch_post_by_id(post_id):
    url = f"https://t.me/{post_id}"
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
    try:
        r = session.get(url, headers=headers, timeout=(10, 20))
        if r.status_code != 200:
            return None
        soup = BeautifulSoup(r.text, 'lxml')
        msg = soup.find('div', class_='tgme_widget_message')
        if not msg:
            return None
        text_elem = msg.find('div', class_='tgme_widget_message_text')
        return text_elem.get_text(strip=True)[:1000] if text_elem else "📄 Без текста"
    except Exception as e:
        console_print(f"Ошибка загрузки поста {post_id}: {e}")
        return None

def make_title_bold(text: str) -> str:
    lines = text.split('\n')
    if not lines:
        return text
    first_line = lines[0]
    if re.match(r'^\s*<b>.*</b>\s*$', first_line):
        pass
    elif '**' in first_line:
        first_line = re.sub(r'\*\*(.*?)\*\*', r'<b>\1</b>', first_line)
    else:
        first_line = f"<b>{first_line}</b>"
    lines[0] = first_line
    return '\n'.join(lines)

# ========== КОНТРОЛЬ ЛИМИТОВ GROQ ==========
class GroqRateLimiter:
    def __init__(self, max_per_minute=25, max_per_day=10000):
        self.max_per_minute = max_per_minute
        self.max_per_day = max_per_day
        self.minute_requests = deque()
        self.day_requests = deque()
        self.last_reset_day = datetime.now().date()
        self.base_delay = 2
    
    def _cleanup(self):
        now = datetime.now()
        while self.minute_requests and now - self.minute_requests[0] > timedelta(minutes=1):
            self.minute_requests.popleft()
        while self.day_requests and now - self.day_requests[0] > timedelta(days=1):
            self.day_requests.popleft()
        today = now.date()
        if today != self.last_reset_day:
            self.day_requests.clear()
            self.last_reset_day = today
    
    def can_make_request(self):
        self._cleanup()
        if len(self.minute_requests) >= self.max_per_minute:
            oldest = self.minute_requests[0]
            wait = 60 - (datetime.now() - oldest).total_seconds()
            return False, max(wait, 1)
        if len(self.day_requests) >= self.max_per_day:
            return False, 3600
        return True, self.base_delay
    
    def record_request(self):
        now = datetime.now()
        self.minute_requests.append(now)
        self.day_requests.append(now)

rate_limiter = GroqRateLimiter()
cache = {}

async def groq_generate_safe(prompt: str, temperature: float = 0.7, max_tokens: int = 600) -> Optional[str]:
    if not USE_AI:
        return None
    import hashlib
    cache_key = hashlib.md5(prompt.lower().strip().encode()).hexdigest()
    if cache_key in cache:
        return cache[cache_key]
    can_request, delay = rate_limiter.can_make_request()
    if not can_request:
        return None
    await asyncio.sleep(delay)
    loop = asyncio.get_event_loop()
    def _sync_generate():
        response = groq_client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=temperature
        )
        return response.choices[0].message.content.strip()
    try:
        result = await loop.run_in_executor(None, _sync_generate)
        rate_limiter.record_request()
        cache[cache_key] = result
        return result
    except Exception as e:
        console_print(f"❌ Ошибка Groq: {e}")
        return None

async def rate_post_interest(text):
    if not USE_AI or len(text.strip()) < 20:
        return "🤖 Короткий пост"
    prompt = f"Оцени новость для Перми одним словом: 'Интересно' 'Не интересно' 'Скучно' 'Устарело'.\n\n{text[:300]}"
    result = await groq_generate_safe(prompt, temperature=0, max_tokens=5)
    if not result:
        return "⏸️"
    if "Интересно" in result:
        return "🔥 Интересно"
    elif "Не интересно" in result:
        return "❄️ Не интересно"
    elif "Скучно" in result:
        return "😴 Скучно"
    elif "Устарело" in result:
        return "⏰ Устарело"
    return "🤔"

async def rewrite_post(text):
    if not USE_AI:
        return "❌ Нейросеть не настроена"
    prompt = f"Перепиши новость в стиле пермских пабликов (коротко, 1-3 эмодзи, без выдумок):\n\n{text[:500]}"
    result = await groq_generate_safe(prompt, temperature=0.8, max_tokens=500)
    if not result:
        return "⏸️ Лимит API"
    formatted = make_title_bold(result)
    links = f"\n\n<a href='https://t.me/+JnuI5n4BRLtiNmUy'>Подписаться на Чё по Перми |</a> <a href='https://t.me/chvprm_admin'>Прислать новость</a>"
    return formatted + links

def to_perm_time(date_str):
    if not date_str:
        return None
    try:
        utc_dt = datetime.fromisoformat(date_str.replace('T', ' ')[:19])
        utc_dt = utc_dt.replace(tzinfo=pytz.UTC)
        perm_tz = pytz.timezone('Asia/Yekaterinburg')
        return utc_dt.astimezone(perm_tz)
    except:
        return None

def is_post_in_last_minutes(date_str):
    perm_dt = to_perm_time(date_str)
    if not perm_dt:
        return False
    now_perm = datetime.now(pytz.timezone('Asia/Yekaterinburg'))
    return (now_perm - perm_dt).total_seconds() <= (WINDOW_MINUTES * 60)

async def check_channel(channel: str):
    async with semaphore:
        url = f"https://t.me/s/{channel}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        try:
            r = await asyncio.to_thread(session.get, url, headers=headers, timeout=(20, 30))
            if r.status_code != 200:
                return
            soup = BeautifulSoup(r.text, 'lxml')
            messages = soup.find_all('div', class_='tgme_widget_message')[:POSTS_FETCH_LIMIT]
            new_posts = []
            for msg in messages:
                post_id = msg.get('data-post', '')
                time_elem = msg.find('time', class_='time')
                date_str = time_elem.get('datetime', '') if time_elem else ''
                if not is_post_in_last_minutes(date_str):
                    continue
                if is_sent(channel, post_id):
                    continue
                text_elem = msg.find('div', class_='tgme_widget_message_text')
                text = text_elem.get_text(strip=True)[:1000] if text_elem else "📄 Без текста"
                new_posts.append({'post_id': post_id, 'text': text})
            if not new_posts:
                return
            console_print(f"📨 {len(new_posts)} новых постов в @{channel}")
            for post in new_posts:
                title = CHANNEL_NAMES.get(channel, channel)
                rating = await rate_post_interest(post['text'])
                msg_text = f"📢 <b>{title}</b>\n{rating}\n\n{post['text']}\n\n🔗 <a href='https://t.me/{post['post_id']}'>Читать пост</a>"
                await bot.send_message(YOUR_CHAT_ID, msg_text, parse_mode="HTML", 
                                       reply_markup=InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔄 Преобразовать", callback_data=f"rewrite_{post['post_id']}")]]))
                mark_as_sent(channel, post['post_id'])
                await asyncio.sleep(0.5)
        except Exception as e:
            console_print(f"❗ Ошибка @{channel}: {e}")

# ========== БОТ ==========
bot = None
dp = Dispatcher()
parsing_enabled = True

@dp.message(Command("start"))
async def start_cmd(message: types.Message):
    global parsing_enabled
    parsing_enabled = True
    await message.answer(
        f"🤖 <b>Парсер Telegram каналов + Groq</b>\n\n"
        f"📡 Каналов: {len(CHANNELS)}\n"
        f"🕒 Посты за последние {WINDOW_MINUTES} минут\n"
        f"🧠 Оценка + кнопка «Преобразовать»\n\n"
        f"<b>Команды:</b>\n"
        f"/stop — остановить парсинг\n"
        f"/start — возобновить парсинг\n"
        f"/channels — список каналов\n"
        f"/stats — статистика",
        parse_mode="HTML"
    )

@dp.message(Command("stop"))
async def stop_cmd(message: types.Message):
    global parsing_enabled
    parsing_enabled = False
    await message.answer("⏸ Парсинг остановлен. /start — возобновить.")

@dp.message(Command("channels"))
async def channels_cmd(message: types.Message):
    lines = [f"• {CHANNEL_NAMES.get(ch, ch)}" for ch in CHANNELS]
    await message.answer("📡 Каналы:\n\n" + "\n".join(lines))

@dp.message(Command("stats"))
async def stats_cmd(message: types.Message):
    total_posts = 0
    conn = sqlite3.connect('bot_data.db')
    c = conn.cursor()
    today_str = datetime.now(pytz.timezone('Asia/Yekaterinburg')).strftime("%Y-%m-%d")
    c.execute("SELECT COUNT(*) FROM sent_posts WHERE sent_at LIKE ?", (today_str + '%',))
    total_posts = c.fetchone()[0]
    conn.close()
    await message.answer(f"📊 <b>Статистика за сегодня</b>\n\nВсего постов: {total_posts}", parse_mode="HTML")

@dp.callback_query(lambda c: c.data and c.data.startswith("rewrite_"))
async def rewrite_callback(callback: CallbackQuery):
    post_id = callback.data.split("_", 1)[1]
    await callback.answer("🔄 Переписываю...")
    await callback.message.answer("⏸️ Функция преобразования временно отключена", parse_mode="HTML")
    await callback.answer()

async def background_checker():
    while True:
        if parsing_enabled:
            cleanup_old_posts()
            console_print(f"🔍 Проверка {len(CHANNELS)} каналов...")
            tasks = [check_channel(ch) for ch in CHANNELS]
            await asyncio.gather(*tasks)
            console_print(f"💤 Пауза {CHECK_INTERVAL_SECONDS // 60} минут...")
        await asyncio.sleep(CHECK_INTERVAL_SECONDS)

async def on_startup():
    console_print("=" * 50)
    console_print("🤖 БОТ ЗАПУЩЕН")
    console_print(f"📡 Каналов: {len(CHANNELS)}")
    console_print(f"🕒 Окно: {WINDOW_MINUTES} минут")
    console_print(f"⏱ Интервал: {CHECK_INTERVAL_SECONDS // 60} минут")
    console_print("=" * 50)
    asyncio.create_task(background_checker())

async def main():
    global bot
    timeout = ClientTimeout(total=30)
    connector = TCPConnector(keepalive_timeout=30, limit=50)
    bot = Bot(token=BOT_TOKEN, timeout=timeout, connector=connector)
    
    await on_startup()
    
    if IS_PRODUCTION:
        # На хостинге используем вебхуки
        app = web.Application()
        app.router.add_post('/webhook', lambda request: dp.feed_update(bot, request))
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        
        webhook_url = f"https://{os.environ.get('BOTHOST_HOSTNAME', os.environ.get('RENDER_EXTERNAL_HOSTNAME', 'localhost'))}/webhook"
        await bot.set_webhook(webhook_url)
        console_print(f"🌐 Вебхук установлен: {webhook_url}")
        
        await asyncio.Event().wait()
    else:
        # Локально используем поллинг
        console_print("📍 Запуск в режиме поллинга (локальная разработка)")
        await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())