from datetime import datetime
from pathlib import Path
import sys

from fastapi.testclient import TestClient
import pytest

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.api_export import app  # noqa: E402


class DummyConnection:
    def __init__(self, rows):
        self.rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def fetch(self, query, start_dt, end_dt):
        return self.rows


class DummyPool:
    def __init__(self, rows):
        self.rows = rows

    def acquire(self):
        return DummyConnection(self.rows)

    async def close(self):
        return None


@pytest.fixture()
def client():
    # данные будут переопределяться в каждом тесте
    if hasattr(app.state, "db_pool"):
        delattr(app.state, "db_pool")

    with TestClient(app) as test_client:
        yield test_client

    if hasattr(app.state, "db_pool"):
        delattr(app.state, "db_pool")


def set_pool(rows):
    app.state.db_pool = DummyPool(rows)


def test_export_csv_success(client):
    rows = [
        {
            "id": 1,
            "title": "Python Dev",
            "company": "Test Corp",
            "location": "Remote",
            "salary": "200k",
            "general_title": "Разработка",
            "category": "Backend",
            "level": "Middle",
            "published_at": datetime(2025, 1, 1, 12, 0),
        }
    ]
    set_pool(rows)

    response = client.get(
        "/export_csv", params={"start_date": "2025-01-01", "end_date": "2025-01-31"}
    )

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "Python Dev" in response.text


def test_export_csv_invalid_range(client):
    set_pool([])

    response = client.get(
        "/export_csv", params={"start_date": "2025-02-01", "end_date": "2025-01-31"}
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Дата начала позже даты окончания"
