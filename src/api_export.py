"""
📦 FastAPI endpoint для выгрузки вакансий из Supabase в CSV.
Позволяет запрашивать вакансии за произвольный период.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import StreamingResponse
import asyncpg
import pandas as pd
from io import StringIO
from datetime import datetime
import os
from dotenv import load_dotenv

# 🔹 Загружаем .env
load_dotenv()

DB_URL = os.getenv("SUPABASE_DB_URL")  # postgresql://user:pass@host:port/dbname
if not DB_URL:
    raise RuntimeError("SUPABASE_DB_URL не найден в окружении")

app = FastAPI(title="HH Vacancy Export API", version="1.0")


@app.get("/")
async def root():
    return {"message": "✅ API для выгрузки CSV работает! Используй /export_csv"}


@app.on_event("shutdown")
async def shutdown_event():
    pool = getattr(app.state, "db_pool", None)
    if pool:
        await pool.close()
        print("🔒 Пул соединений закрыт")


async def get_pool():
    pool = getattr(app.state, "db_pool", None)
    if pool is None:
        print("🔌 Пул отсутствует, создаём новое соединение с Supabase...")
        app.state.db_pool = await asyncpg.create_pool(DB_URL, statement_cache_size=0)
        print("✅ Пул создан")
        pool = app.state.db_pool
    return pool


@app.get("/export_csv")
async def export_csv(
    start_date: str = Query(..., description="Начало периода (YYYY-MM-DD)"),
    end_date: str = Query(..., description="Конец периода (YYYY-MM-DD)"),
):
    """
    Возвращает CSV-файл вакансий за выбранный период.
    """
    try:
        print("🚀 Эндпоинт вызван")
        start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
        end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
        print(f"📅 Получен запрос: {start_dt} → {end_dt}")

        if start_dt > end_dt:
            raise HTTPException(status_code=400, detail="Дата начала позже даты окончания")

        pool = await get_pool()

        query = f"""
            SELECT 
                id, title, company, location, salary,
                general_title, category, level, published_at
            FROM vacancies
            WHERE published_at BETWEEN $1 AND $2
            ORDER BY published_at DESC
        """

        print("▶️ Отправляем SQL в Supabase...")
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, start_dt, end_dt)
        print(f"✅ Получено строк: {len(rows)}")

        if not rows:
            print("⚠️ Нет данных за этот период")
            return {"message": "⚠️ За этот период вакансий не найдено."}

        print("🧾 Преобразуем в CSV...")
        df = pd.DataFrame([dict(r) for r in rows])
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        csv_data = csv_buffer.getvalue()

        filename = f"vacancies_{start_dt}_{end_dt}.csv"
        print("✅ Всё готово! Отправляем CSV клиенту через StreamingResponse...")

        # ⚡️ Отправляем как поток, чтобы бот не зависал
        return StreamingResponse(
            iter([csv_data.encode("utf-8")]),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}",
                "Content-Type": "text/csv; charset=utf-8",
            },
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Ошибка: {e}")
        return {"error": str(e)}
