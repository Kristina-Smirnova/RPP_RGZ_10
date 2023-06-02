from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.dispatcher import FSMContext
from aiogram.contrib.middlewares.logging import LoggingMiddleware
from aiogram.types import KeyboardButton, ReplyKeyboardMarkup, Message

import numpy as np

import os
import logging
import psycopg2

import requests
import threading
import json

api_key = "API_KEY"

period = 20
risk_tolerance = 0.03
max_potential_loss = 0.05


def calculate_and_store_data(symbol):
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey={api_key}"
    response = requests.get(url)
    data = json.loads(response.text)
    closing_prices = []
    for date in list(data["Time Series (Daily)"].keys())[:int(period)]:
        closing_prices.append(float(data["Time Series (Daily)"][date]["4. close"]))

    returns = []
    for item in range(1, len(closing_prices)):
        returns.append((closing_prices[i] - closing_prices[item - 1]) / closing_prices[item - 1])

    risk = np.std(returns)

    position_size = risk_tolerance / (risk * max_potential_loss)

    print(f"Оптимальный размер позиции для ценной бумаги {symbol}: {position_size:.2f}")

    conn = psycopg2.connect(
        host="localhost",
        database="RGZ_RPP",
        user="postgre",
        password="postgre"
    )
    cursor = conn.cursor()
    cursor.execute(
        "INSERT INTO securities(position_size) VALUES (%(position_size)s",
        {
            "position_size": position_size
        }
    )
    conn.commit()
    conn.close()


ticker = threading.Event()
while not ticker.wait(period):
    # выбрать из БД
    conn = psycopg2.connect(
        host="localhost",
        database="RGZ_RPP",
        user="postgre",
        password="postgre"
    )
    cursor = conn.cursor()
    # Запрашиваем все имеющиеся ценные бумаги
    cursor.execute("SELECT * FROM securities")
    found_securities = cursor.fetchall()
    # через цикл отдать функции обработки
    for i in range(1, len(found_securities)):
        calculate_and_store_data(i)


# Активация системы логирования
logging.basicConfig(level=logging.INFO)

# Получение токена из переменных окружения
bot_token = os.getenv('API_TOKEN')

# Создание объекта типа бот
bot = Bot(token=bot_token)

# Инициализация диспетчера команд
dp = Dispatcher(bot, storage=MemoryStorage())


class ManageStateGroup(StatesGroup):
    Add_securities_state = State()
    Get_securities_state = State()


@dp.message_handler(commands=['start'])
async def start_command(message: types.Message):
    kb = ReplyKeyboardMarkup(is_persistent=True, resize_keyboard=True, row_width=1)
    kb.add(KeyboardButton('/add_securities'))
    kb.add(KeyboardButton('/get_securities_positions'))
    await message.reply("Добро пожаловать в бота", reply_markup=kb)


@dp.message_handler(commands=['add_securities'])
async def add_securities_command(message: types.Message):
    await ManageStateGroup.Add_securities_state.set()
    await message.answer('Введите имя ценной бумаги')


@dp.message_handler(state=ManageStateGroup.Add_securities_state)
async def save_security(message: types.Message, state: FSMContext):
    test = message.text
    msg = await add_security_in_database(test)
    await message.answer(msg)
    await state.finish()


async def add_security_in_database(security_name: str):
    conn = psycopg2.connect(
        host="localhost",
        database="RGZ_RPP",
        user="postgre",
        password="postgre"
    )
    cursor = conn.cursor()
    # Запрашиваем все имеющиеся ценные бумаги
    cursor.execute("SELECT 1 FROM securities WHERE security_name = %(security_name)s", {"security_name": security_name})
    found_securities = cursor.fetchall()

    # Если найдена хотя бы одна ценная бумага, security_name которой совпадает с тем, что мы пытаемся сохранить, тогда
    # кидаем исключение с текстом "Ценная бумага уже существует"
    if len(found_securities) > 0:
        raise Exception(f"Ценная бумага {security_name} уже существует")

    cursor.execute(
        "INSERT INTO securities(security_name) VALUES (%(security_name)s)",
        {
            "security_name": security_name
        }
    )
    if len(found_securities) == 0:
        raise Exception(f"Ценная бумага {security_name} добавлена к отслеживаемым")
    conn.commit()
    conn.close()


@dp.message_handler(commands=['get_securities_positions'])
async def get_securities_indicators_command(message: type.Message):
    securities_indicators = get_position_size()
    response = ""
    if securities_indicators:
        response = "Показатели доходности ценных бумаг:\n"
        for position_size in securities_indicators:
            response += f"{position_size[0]}: {position_size[1]} \n"
    else:
        response = "Показатели доходности ценных бумаг не найдены"
    await bot.send_message(message.chat.id, response)


@dp.message_handler(state=ManageStateGroup.Get_securities_state)
async def save_stock(message: Message, state: FSMContext):
    msg = get_position_size(message.text)
    await message.answer(msg)
    await state.finish()


async def get_position_size(security_name):
    conn = psycopg2.connect(
        host="localhost",
        database="RGZ_RPP",
        user="postgre",
        password="postgre"
    )
    cursor = conn.cursor()
    cursor.execute("SELECT security_name, position_size FROM securities WHERE security_name = %(security_name)s")
    rows = cursor.fetchall()
    conn.close()
    logging.info(rows)
    return rows


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    dp.middleware.setup(LoggingMiddleware())
    executor.start_polling(dp, skip_updates=True)
