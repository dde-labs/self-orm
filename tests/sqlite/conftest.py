import os
from collections.abc import AsyncGenerator
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncSession
from src.sqlite.db import AsyncManage

from ..conftest import test_path


@pytest.fixture(scope="session")
def db_file(test_path) -> Path:
    return test_path / 'sqlite.async.db'


@pytest.fixture(scope='session', autouse=True)
def db_manage(db_file) -> AsyncManage:
    print("Start setup SQLite database")
    manage = AsyncManage()
    manage.init(f"sqlite+aiosqlite:///{db_file}")
    return manage


@pytest.fixture(scope='function')
async def db_session(db_manage) -> AsyncGenerator[AsyncSession]:
    print("Start setup SQLite session")
    async with db_manage.async_session_maker() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


@pytest.fixture(scope='session', autouse=True)
async def initial_objects(db_manage, db_file) -> None:
    await db_manage.initialize()

    yield

    from src.sqlite.models import Base

    async with db_manage.engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)

    await db_manage.close()

    if os.path.exists(db_file):
        os.remove(db_file)
