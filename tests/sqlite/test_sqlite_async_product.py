import pytest

from src.sqlite.models.product import Product


@pytest.mark.asyncio
async def test_sqlite_bulk_insert(db_session):
    async with db_session.begin():
        db_session.add_all(
            [
                Product(),
                Product(),
                Product(),
            ]
        )
