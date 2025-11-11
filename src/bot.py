import os
import aiohttp
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import FSInputFile
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_URL = os.getenv("API_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())


class ExportStates(StatesGroup):
    waiting_start_date = State()
    waiting_end_date = State()


@dp.message(Command("start"))
async def start_cmd(message: types.Message, state: FSMContext):
    await message.answer(
        "👋 Привет! Отправь дату начала периода в формате `YYYY-MM-DD` (например: 2025-11-01)"
    )
    await state.set_state(ExportStates.waiting_start_date)


@dp.message(ExportStates.waiting_start_date)
async def process_start_date(message: types.Message, state: FSMContext):
    try:
        start_date = datetime.strptime(message.text, "%Y-%m-%d").date()
        await state.update_data(start_date=start_date)
        await message.answer("✅ Отлично! Теперь отправь дату окончания периода (в формате `YYYY-MM-DD`):")
        await state.set_state(ExportStates.waiting_end_date)
    except ValueError:
        await message.answer("⚠️ Неверный формат даты. Попробуй ещё раз (пример: 2025-11-01).")


@dp.message(ExportStates.waiting_end_date)
async def process_end_date(message: types.Message, state: FSMContext):
    try:
        end_date = datetime.strptime(message.text, "%Y-%m-%d").date()
        data = await state.get_data()
        start_date = data["start_date"]

        await message.answer("⏳ Формирую CSV...")

        async with aiohttp.ClientSession() as session:
            params = {"start_date": str(start_date), "end_date": str(end_date)}
            print(f"➡️ GET {API_URL} | Параметры: {params}")

            async with session.get(API_URL, params=params) as resp:
                print(f"⬅️ Статус ответа: {resp.status}")

                if resp.status != 200:
                    await message.answer(f"❌ Ошибка при запросе данных ({resp.status})")
                    return

                file_bytes = await resp.read()
                file_path = f"vacancies_{start_date}_{end_date}.csv"

                with open(file_path, "wb") as f:
                    f.write(file_bytes)

        await message.answer_document(
            document=FSInputFile(file_path),
            caption=f"📊 Вакансии с {start_date} по {end_date}",
        )

    except ValueError:
        await message.answer("⚠️ Неверный формат даты. Попробуй ещё раз (пример: 2025-11-07).")

    except Exception as e:
        await message.answer(f"❌ Произошла ошибка: {e}")

    finally:
        await state.clear()


if __name__ == "__main__":
    import asyncio

    async def main():
        print("🚀 Бот запущен...")
        await dp.start_polling(bot)

    asyncio.run(main())