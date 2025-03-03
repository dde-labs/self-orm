import pytest

from sqlalchemy.ext.asyncio import AsyncSession

from src.sqlite.models import User


@pytest.mark.asyncio
async def test_sqlite_create_user(db_session: AsyncSession):
    async with db_session.begin():
        user = User(name="custom", email="custom@email.com")
        db_session.add(user)
        await db_session.commit()

    await db_session.refresh(user)
    assert user.name == 'custom'


@pytest.mark.asyncio
async def test_sqlite_create_users(db_session: AsyncSession):
    users: list[User] = []
    for i in range(1, 11):
        users.append(
            User(name=f"User {i}", email=f"user{i}@example.com")
        )
    db_session.add_all(users)
    await db_session.commit()

    initial_count = await User.count_users(db_session)
    assert initial_count == 10
