import logging
import asyncio
import os
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import FSInputFile
import re
import sys

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

os.makedirs(os.path.join(ROOT_DIR, "data"), exist_ok=True)

DB_PATH = os.path.join(ROOT_DIR, "data", "products.db")

ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

load_dotenv(ENV_PATH)

DEFAULT_BOT_TOKEN = "DEFAULT_BOT_TOKEN"
DEFAULT_OPERATORS = "DEFAULT_OPERATORS"
DEFAULT_FILES_CHANNEL_ID = "DEFAULT_FILES_CHANNEL_ID"
DEFAULT_LOG_CHANNEL_ID = "DEFAULT_LOG_CHANNEL_ID"

API_TOKEN = os.getenv("BOT_TOKEN", DEFAULT_BOT_TOKEN)
OPERATORS_STR = os.getenv("OPERATORS", DEFAULT_OPERATORS)
OPERATORS = [int(op.strip()) for op in OPERATORS_STR.split(",") if op.strip()]
FILES_CHANNEL_ID = int(os.getenv("FILES_CHANNEL_ID", DEFAULT_FILES_CHANNEL_ID))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", DEFAULT_LOG_CHANNEL_ID))

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
logger.info(f"Используется BOT_TOKEN: {API_TOKEN[:5]}...{API_TOKEN[-5:]}")
logger.info(f"Операторы: {OPERATORS}")
logger.info(f"ID канала файлов: {FILES_CHANNEL_ID}")
logger.info(f"ID канала логов: {LOG_CHANNEL_ID}")

bot = Bot(token=API_TOKEN)
dp = Dispatcher()

def init_db():
    db_exists = os.path.exists(DB_PATH)

    if db_exists:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("PRAGMA table_info(products)")
        columns = [column[1] for column in cursor.fetchall()]

        if "ozon_link" not in columns:
            logger.info("Добавление колонки ozon_link в таблицу products")
            cursor.execute("ALTER TABLE products ADD COLUMN ozon_link TEXT DEFAULT ''")
        if "wb_link" not in columns:
            logger.info("Добавление колонки wb_link в таблицу products")
            cursor.execute("ALTER TABLE products ADD COLUMN wb_link TEXT DEFAULT ''")
        if "ym_link" not in columns:
            logger.info("Добавление колонки ym_link в таблицу products")
            cursor.execute("ALTER TABLE products ADD COLUMN ym_link TEXT DEFAULT ''")
        if "file_id" not in columns:
            logger.info("Добавление колонки file_id в таблицу products")
            cursor.execute("ALTER TABLE products ADD COLUMN file_id TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE products ADD COLUMN file_type TEXT DEFAULT ''")
            cursor.execute("ALTER TABLE products ADD COLUMN caption TEXT DEFAULT ''")
        if "photo_id" not in columns:
            logger.info("Добавление колонки photo_id в таблицу products")
            cursor.execute("ALTER TABLE products ADD COLUMN photo_id TEXT DEFAULT ''")

        conn.commit()
        conn.close()
        logger.info(f"База данных {DB_PATH} обновлена")
        return

    logger.info(f"Создание новой базы данных {DB_PATH}")
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS brands (
        id INTEGER PRIMARY KEY,
        name TEXT UNIQUE
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS categories (
        id INTEGER PRIMARY KEY,
        brand_id INTEGER,
        name TEXT,
        FOREIGN KEY (brand_id) REFERENCES brands (id),
        UNIQUE(brand_id, name)
    )
    ''')

    cursor.execute('''
    CREATE TABLE IF NOT EXISTS products (
        id INTEGER PRIMARY KEY,
        category_id INTEGER,
        name TEXT,
        channel_message_id INTEGER,
        ozon_link TEXT DEFAULT "",
        wb_link TEXT DEFAULT "",
        ym_link TEXT DEFAULT "",
        date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        file_id TEXT DEFAULT "",
        file_type TEXT DEFAULT "",
        caption TEXT DEFAULT "",
        photo_id TEXT DEFAULT "",
        FOREIGN KEY (category_id) REFERENCES categories (id),
        UNIQUE(category_id, name)
    )
    ''')

    conn.commit()
    conn.close()

    logger.info("База данных успешно инициализирована")

def get_brand_id(brand_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM brands WHERE name = ?", (brand_name,))
    result = cursor.fetchone()

    if result:
        brand_id = result[0]
    else:
        cursor.execute("INSERT INTO brands (name) VALUES (?)", (brand_name,))
        brand_id = cursor.lastrowid

    conn.commit()
    conn.close()
    return brand_id

def get_category_id(brand_id, category_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT id FROM categories WHERE brand_id = ? AND name = ?",
                  (brand_id, category_name))
    result = cursor.fetchone()

    if result:
        category_id = result[0]
    else:
        cursor.execute("INSERT INTO categories (brand_id, name) VALUES (?, ?)",
                      (brand_id, category_name))
        category_id = cursor.lastrowid

    conn.commit()
    conn.close()
    return category_id

def add_product(brand_name, category_name, product_name, channel_message_id,
                ozon_link="", wb_link="", ym_link="", photo_id=""):
    try:
        brand_id = get_brand_id(brand_name)
        category_id = get_category_id(brand_id, category_name)

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
        INSERT OR REPLACE INTO products
        (category_id, name, channel_message_id, ozon_link, wb_link, ym_link, date_added, photo_id)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (category_id, product_name, channel_message_id,
              ozon_link, wb_link, ym_link, datetime.now(), photo_id))

        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Ошибка при добавлении товара: {e}")
        return False

def get_brands():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("SELECT name FROM brands")
    brands = [row[0] for row in cursor.fetchall()]

    conn.close()
    return brands

def get_categories(brand_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT c.name FROM categories c
    JOIN brands b ON c.brand_id = b.id
    WHERE b.name = ?
    """, (brand_name,))

    categories = [row[0] for row in cursor.fetchall()]

    conn.close()
    return categories

def get_products(brand_name, category_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT p.name FROM products p
    JOIN categories c ON p.category_id = c.id
    JOIN brands b ON c.brand_id = b.id
    WHERE b.name = ? AND c.name = ?
    """, (brand_name, category_name))

    products = [row[0] for row in cursor.fetchall()]

    conn.close()
    return products

def get_product_info(product_name):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute("""
    SELECT p.channel_message_id, p.ozon_link, p.wb_link, p.ym_link, p.photo_id
    FROM products p
    WHERE p.name = ?
    """, (product_name,))

    result = cursor.fetchone()

    conn.close()

    if result:
        return {
            "channel_message_id": result[0],
            "ozon_link": result[1] or "",
            "wb_link": result[2] or "",
            "ym_link": result[3] or "",
            "photo_id": result[4] or ""
        }
    return None

def delete_product(brand_name, category_name, product_name):
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        cursor.execute("""
            SELECT p.id
            FROM products p
            JOIN categories c ON p.category_id = c.id
            JOIN brands b ON c.brand_id = b.id
            WHERE b.name = ? AND c.name = ? AND p.name = ?
        """, (brand_name, category_name, product_name))

        product = cursor.fetchone()

        if not product:
            conn.close()
            return False, f"Товар '{product_name}' в категории '{category_name}' бренда '{brand_name}' не найден в базе данных"

        cursor.execute("""
            DELETE FROM products
            WHERE id IN (
                SELECT p.id
                FROM products p
                JOIN categories c ON p.category_id = c.id
                JOIN brands b ON c.brand_id = b.id
                WHERE b.name = ? AND c.name = ? AND p.name = ?
            )
        """, (brand_name, category_name, product_name))

        conn.commit()
        conn.close()
        return True, f"Товар '{product_name}' из категории '{category_name}' бренда '{brand_name}' успешно удален"
    except Exception as e:
        logger.error(f"Ошибка при удалении товара: {e}")
        return False, f"Ошибка при удалении товара: {str(e)}"

@dp.message(Command("delete_product"))
async def delete_product_command(message: types.Message):
    if message.from_user.id not in OPERATORS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    try:
        args = re.findall(r'\[(.*?)\]', message.text)

        if len(args) < 3:
            await message.answer(
                "⚠️ Неверный формат команды. Используйте:\n"
                "/delete_product [Бренд] [Категория] [Название товара]"
            )
            return

        brand_name = args[0]
        category_name = args[1]
        product_name = args[2]

        success, message_text = delete_product(brand_name, category_name, product_name)

        if success:
            await message.answer(f"✅ {message_text}")
        else:
            await message.answer(f"❌ {message_text}")
    except Exception as e:
        logger.error(f"Ошибка в команде delete_product: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

class BotState(StatesGroup):
    waiting_for_brand = State()
    waiting_for_category = State()
    waiting_for_product = State()
    chatting_with_operator = State()

def create_dynamic_keyboard(items, add_back=True):
    keyboard = []

    if add_back:
        keyboard.append([KeyboardButton(text="⬅️ Назад")])

    for i in range(0, len(items), 2):
        row = []
        for j in range(2):
            if i + j < len(items):
                row.append(KeyboardButton(text=items[i + j]))
        keyboard.append(row)

    return ReplyKeyboardMarkup(keyboard=keyboard, resize_keyboard=True)

kb_main = ReplyKeyboardMarkup(
    keyboard=[
        [KeyboardButton(text="Наш ассортимент")],
        [KeyboardButton(text="Гарантия"), KeyboardButton(text="Возврат")],
        [KeyboardButton(text="👨‍💼 Связаться с оператором")]
    ],
    resize_keyboard=True
)

kb_exit_chat = ReplyKeyboardMarkup(
    keyboard=[[KeyboardButton(text="⬅️ Назад")]],
    resize_keyboard=True
)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Без имени"
    log_message = (
        f"🚀 <b>Бот запущен пользователем</b>\n"
        f"👤 Пользователь: <b>{username}</b>\n"
        f"🆔 ID: <code>{user_id}</code>"
    )
    await send_log(log_message, "USER_ACTION")

    kb_shops = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(
                text="Wildberries",
                url="https://www.wildberries.ru/seller/159267"
            )
        ],
        [
            InlineKeyboardButton(
                text="Ozon",
                url="https://www.ozon.ru/seller/oneenergy-69819/products/?miniapp=seller_69819"
            )
        ],
        [
            InlineKeyboardButton(
                text="Яндекс.Маркет",
                url="https://market.yandex.ru/business--oneenergy-llc/1044944?generalContext=t%3DshopInShop%3Bi%3D1%3Bbi%3D1044944%3B&rs=eJwzUv_EqMLBKLDwEKsEg8azbh6NnqOsGhuBuPE4q8aPU6waZ0-zajzv5gEAEloOnw%2C%2C&searchContext=sins_ctx"
            )
        ]
    ])

    await message.answer(
        "👋 Добро пожаловать в бот поддержки ONEENERGY!\n\n"
        "Здесь вы можете получить информацию о наших продуктах, гарантии и возврате.\n\n"
        "Ознакомиться с нашим ассортиментом можно в магазинах:",
        reply_markup=kb_shops
    )

    await message.answer("Выберите действие:", reply_markup=kb_main)

@dp.message(F.text == "Наш ассортимент")
async def show_assortment(message: types.Message, state: FSMContext):
    brands = get_brands()

    if not brands:
        await message.answer("В данный момент нет доступных товаров.", reply_markup=kb_main)
        return

    kb_brands = create_dynamic_keyboard(brands)

    await state.set_state(BotState.waiting_for_brand)
    await message.answer("Выберите бренд:", reply_markup=kb_brands)

@dp.message(StateFilter(BotState.waiting_for_brand))
async def brand_selected(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer(".", reply_markup=kb_main)
        return

    brand_name = message.text
    categories = get_categories(brand_name)

    if not categories:
        await message.answer(f"Для бренда {brand_name} нет доступных категорий.", reply_markup=kb_main)
        await state.clear()
        return

    kb_categories = create_dynamic_keyboard(categories)

    await state.update_data(selected_brand=brand_name)
    await state.set_state(BotState.waiting_for_category)
    await message.answer(f"Выберите категорию товаров {brand_name}:", reply_markup=kb_categories)

@dp.message(StateFilter(BotState.waiting_for_category))
async def category_selected(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Назад":
        brands = get_brands()
        kb_brands = create_dynamic_keyboard(brands)

        await state.set_state(BotState.waiting_for_brand)
        await message.answer("Выберите бренд:", reply_markup=kb_brands)
        return

    category_name = message.text
    user_data = await state.get_data()
    brand_name = user_data.get("selected_brand")

    products = get_products(brand_name, category_name)

    if not products:
        await message.answer(f"В категории {category_name} нет доступных товаров.",
                            reply_markup=kb_main)
        await state.clear()
        return

    kb_products = create_dynamic_keyboard(products)

    await state.update_data(selected_category=category_name)
    await state.set_state(BotState.waiting_for_product)
    await message.answer(f"Выберите товар из категории {category_name}:",
                         reply_markup=kb_products)

@dp.message(StateFilter(BotState.waiting_for_product))
async def product_selected(message: types.Message, state: FSMContext):
    if message.text == "⬅️ Назад":
        user_data = await state.get_data()
        brand_name = user_data.get("selected_brand")
        categories = get_categories(brand_name)
        kb_categories = create_dynamic_keyboard(categories)

        await state.set_state(BotState.waiting_for_category)
        await message.answer(f"Выберите категорию товаров {brand_name}:",
                            reply_markup=kb_categories)
        return

    product_name = message.text
    product_info = get_product_info(product_name)

    if not product_info:
        await message.answer(f"Информация о товаре {product_name} не найдена.",
                            reply_markup=kb_main)
        await state.clear()
        return

    try:
        buy_buttons = []

        if product_info["ozon_link"]:
            buy_buttons.append([InlineKeyboardButton(text="🛒 Купить на Ozon",
                                                    url=product_info["ozon_link"])])

        if product_info["wb_link"]:
            buy_buttons.append([InlineKeyboardButton(text="🛒 Купить на Wildberries",
                                                   url=product_info["wb_link"])])

        if product_info["ym_link"]:
            buy_buttons.append([InlineKeyboardButton(text="🛒 Купить на Яндекс.Маркет",
                                                   url=product_info["ym_link"])])

        buy_markup = InlineKeyboardMarkup(inline_keyboard=buy_buttons) if buy_buttons else None

        if product_info["photo_id"] and int(product_info["photo_id"]) > 0:
            try:
                await bot.copy_message(
                    chat_id=message.chat.id,
                    from_chat_id=FILES_CHANNEL_ID,
                    message_id=int(product_info["photo_id"])
                )
            except Exception as e:
                logger.error(f"Ошибка при отправке фото товара: {e}")

        await bot.copy_message(
            chat_id=message.chat.id,
            from_chat_id=FILES_CHANNEL_ID,
            message_id=product_info["channel_message_id"],
            reply_markup=buy_markup
        )

        await state.clear()
        await message.answer("Выберите дальнейшее действие:", reply_markup=kb_main)

    except Exception as e:
        logger.error(f"Ошибка при отправке товара: {e}")
        await message.answer(
            f"Произошла ошибка при загрузке информации о товаре {product_name}.",
            reply_markup=kb_main
        )
        await state.clear()

@dp.message(Command("add_product"))
async def add_product_command(message: types.Message):
    if message.from_user.id not in OPERATORS:
        await message.answer("У вас нет прав для выполнения этой команды.")
        return

    try:
        args = re.findall(r'\[(.*?)\]', message.text)

        if len(args) < 4:
            await message.answer(
                "⚠️ Неверный формат команды. Используйте:\n"
                "/add_product [Бренд] [Категория] [Название] [ID сообщения] [Ссылки (опционально)] [ID фото (опционально)]"
            )
            return

        brand_name = args[0]
        category_name = args[1]
        product_name = args[2]

        try:
            message_id = int(args[3])
        except ValueError:
            await message.answer("⚠️ ID сообщения должен быть числом.")
            return

        links = {"ozon_link": "", "wb_link": "", "ym_link": ""}
        photo_id = ""

        if len(args) >= 5:
            links_text = args[4]
            for link in links_text.split():
                if link.startswith("ozon:"):
                    links["ozon_link"] = link.split(":", 1)[1]
                elif link.startswith("wb:"):
                    links["wb_link"] = link.split(":", 1)[1]
                elif link.startswith("ym:"):
                    links["ym_link"] = link.split(":", 1)[1]

        if len(args) >= 6:
            try:
                photo_id = int(args[5])
            except ValueError:
                await message.answer("⚠️ ID фото должен быть числом.")
                return

        success = add_product(
            brand_name,
            category_name,
            product_name,
            message_id,
            links["ozon_link"],
            links["wb_link"],
            links["ym_link"],
            photo_id
        )

        if success:
            await message.answer(
                f"✅ Товар успешно добавлен:\n"
                f"Бренд: {brand_name}\n"
                f"Категория: {category_name}\n"
                f"Название: {product_name}\n"
                f"ID сообщения: {message_id}"
            )

            try:
                forwarded = await bot.forward_message(
                    chat_id=message.chat.id,
                    from_chat_id=FILES_CHANNEL_ID,
                    message_id=message_id
                )

                file_id = ""
                file_type = ""
                caption = ""

                if forwarded.document:
                    file_id = forwarded.document.file_id
                    file_type = "document"
                    caption = forwarded.caption or ""
                elif forwarded.photo:
                    file_id = forwarded.photo[-1].file_id
                    file_type = "photo"
                    caption = forwarded.caption or ""

                await bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=forwarded.message_id
                )

                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute("""
                UPDATE products
                SET file_id = ?, file_type = ?, caption = ?
                WHERE channel_message_id = ?
                """, (file_id, file_type, caption, message_id))
                conn.commit()
                conn.close()

            except Exception as e:
                logger.error(f"Ошибка при получении file_id: {e}")
        else:
            await message.answer("❌ Ошибка при добавлении товара.")

    except Exception as e:
        logger.error(f"Ошибка в команде add_product: {e}")
        await message.answer(f"❌ Произошла ошибка: {str(e)}")

@dp.message(F.text == "👨‍💼 Связаться с оператором")
async def contact_operator_start(message: types.Message, state: FSMContext):
    await message.answer("Вы подключены к оператору. Напишите ваш вопрос:", reply_markup=kb_exit_chat)
    await state.set_state(BotState.chatting_with_operator)

@dp.message(StateFilter(BotState.chatting_with_operator))
async def forward_to_operator(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    username = message.from_user.username or "Без имени"
    user_text = message.text

    log_message = (
        f"💬 <b>Сообщение оператору</b>\n"
        f"👤 От: <b>{username}</b>\n"
        f"🆔 ID: <code>{user_id}</code>\n"
        f"📝 Текст: <i>{user_text}</i>"
    )
    await send_log(log_message, "USER_ACTION")

    if message.text == "⬅️ Назад":
        await state.clear()
        await message.answer("Вы вышли из чата с оператором.", reply_markup=kb_main)
        return

    text = (
        f"📩 Новое сообщение от пользователя:\n\n"
        f"👤 <b>{username}</b>\n"
        f"🆔 <code>{user_id}</code>\n"
        f"💬 {user_text}\n\n"
        f"Ответьте командой:\n"
        f"/reply {user_id} ваш_ответ"
    )

    sent_to_someone = False
    for operator in OPERATORS:
        try:
            await bot.send_message(chat_id=operator, text=text, parse_mode="HTML")
            sent_to_someone = True
        except Exception as e:
            logger.error(f"Не удалось отправить сообщение оператору {operator}: {e}")

    if sent_to_someone:
        await message.answer("Сообщение отправлено оператору. Ожидайте ответа.", reply_markup=kb_main)
        await state.clear()
    else:
        await message.answer("К сожалению, сейчас нет доступных операторов. Попробуйте позже.", reply_markup=kb_main)
        await state.clear()

@dp.message(Command("reply"))
async def operator_reply(message: types.Message):
    if message.from_user.id not in OPERATORS:
        return

    args = message.text.split(maxsplit=2)
    if len(args) < 3:
        await message.answer("⚠️ Используйте формат: /reply user_id сообщение")
        return

    user_id = args[1]
    reply_text = args[2]

    operator_id = message.from_user.id
    operator_username = message.from_user.username or "Оператор"
    log_message = (
        f"🔄 <b>Ответ оператора</b>\n"
        f"👨‍💼 Оператор: <b>{operator_username}</b> (<code>{operator_id}</code>)\n"
        f"👤 Пользователю: <code>{user_id}</code>\n"
        f"📝 Текст: <i>{reply_text}</i>"
    )
    await send_log(log_message, "OPERATOR_ACTION")

    try:
        await bot.send_message(chat_id=int(user_id), text=f"📩 Ответ от оператора:\n\n{reply_text}")
        await message.answer(f"✅ Ответ отправлен пользователю {user_id}.")
    except Exception as e:
        await message.answer(f"❌ Ошибка при отправке сообщения: {e}")

@dp.message(F.text == "Гарантия")
async def show_warranty(message: types.Message):
    warranty_text = (
        "📝 *Информация о гарантии*\n\n"
        "Гарантия на товар 2 года. Возврат товара возможен только при наличии брака или надлежащего качества с сохранением его товарного вида (не нарушением упаковки).\n\n"
        "*В соответствии с Постановлением Правительства РФ от 31.12.2020 N 2463 (ред. от 17.05.2024) \"Об утверждении Правил продажи товаров по договору розничной купли-продажи, перечня товаров длительного пользования, на которые не распространяется требование потребителя о безвозмездном предоставлении ему товара, обладающего этими же основными потребительскими свойствами, на период ремонта или замены такого товара, и перечня непродовольственных товаров надлежащего качества, не подлежащих обмену, а также о внесении изменений в некоторые акты Правительства Российской Федерации\" с пунктом 11 перечня непродовольственных товаров надлежащего качества, не подлежащих обмену наш товар относится к технически сложному товару, на который установлен срок годности не менее 1 года.*\n\n"
        "*В соответствии со статьей 25 закона о защите прав потребителя данный товар подлежит возврату, если указанный товар не был в употреблении, сохранены его товарный вид, потребительские свойства, пломбы, фабричные ярлыки. В иных случаях возврат возможен только при наличии технического брака, подтвержденного СЦ.*"
    )
    await message.answer(warranty_text, parse_mode="Markdown", reply_markup=kb_main)

@dp.message(F.text == "Возврат")
async def show_return_policy(message: types.Message):
    return_text = (
        "📦 *Политика возврата*\n\n"
        "Возврат осуществляется через оформление заявки на возврат по браку в личном кабинете маркетпейса, в котором был приобретен товар."
    )
    await message.answer(return_text, parse_mode="Markdown", reply_markup=kb_main)

async def send_log(message, log_type="INFO"):
    try:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        header = f"📋 #{log_type} | {timestamp}\n\n"

        await bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=header + message,
            parse_mode="HTML",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Не удалось отправить лог в канал: {e}")

async def main():
    init_db()

    try:
        startup_message = (
            f"🤖 <b>Бот запущен</b>\n"
            f"⏱ Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
            f"📢 Канал файлов: <code>{FILES_CHANNEL_ID}</code>\n"
            f"👨‍💼 Операторы: <code>{OPERATORS}</code>"
        )
        await bot.send_message(
            chat_id=LOG_CHANNEL_ID,
            text=startup_message,
            parse_mode="HTML"
        )
        logger.info("Лог запуска отправлен в канал")
    except Exception as e:
        logger.error(f"Не удалось отправить стартовый лог: {e}")

    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())