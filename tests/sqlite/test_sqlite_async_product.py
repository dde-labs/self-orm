import pytest
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.sqlite.models.product import Product


def test_check_db_path(db_file):
    assert db_file.exists()


@pytest.mark.asyncio
async def test_exec_with_session(db_session: AsyncSession):
    rs = (
        await db_session.execute(text('SELECT 1 as result'))
    ).scalar_one_or_none()
    assert rs == 1


@pytest.mark.asyncio
async def test_exec_with_db_manage(db_manage):
    async with db_manage.async_session_maker() as session:
        try:
            rs = (
                await session.execute(text('SELECT 2 as result'))
            ).scalar_one_or_none()
            assert rs == 2
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@pytest.mark.asyncio
async def test_bulk_load_transaction(db_session):
    async with db_session.begin():
        db_session.add_all(
            [
                Product(),
                Product(),
                Product(),
            ]
        )
