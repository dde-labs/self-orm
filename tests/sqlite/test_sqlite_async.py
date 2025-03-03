import pytest
from sqlalchemy.sql import text
from sqlalchemy.ext.asyncio import AsyncSession


@pytest.mark.asyncio
async def test_sqlite_exec_with_session(db_session: AsyncSession):
    rs = (
        await db_session.execute(text('SELECT 1 as result'))
    ).scalar_one_or_none()
    assert rs == 1


@pytest.mark.asyncio
async def test_sqlite_exec_with_db_manage(db_manage):
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
