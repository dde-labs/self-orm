import pytest
from sqlalchemy.sql import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.sqlite.models import Policy, Role


@pytest.mark.asyncio
async def test_sqlite_create_policies(db_session: AsyncSession):
    policies = [
        Policy(resource="workflow", action="read"),
        Policy(resource="workflow", action="create"),
        Policy(resource="workflow", action="update"),
        Policy(resource="workflow", action="delete"),
        Policy(resource="auth", action="read"),
        Policy(resource="auth", action="create"),
        Policy(resource="auth", action="update"),
        Policy(resource="auth", action="delete"),
    ]
    db_session.add_all(policies)
    await db_session.commit()

    policies = (await db_session.execute(select(Policy))).scalars().all()
    print(policies)
    assert len(policies) == 8


@pytest.mark.asyncio
async def test_sqlite_create_role_with_policies(db_session: AsyncSession):
    policies = [
        Policy(resource="custom", action="read"),
        Policy(resource="custom", action="create"),
        Policy(resource="custom", action="update"),
        Policy(resource="custom", action="delete"),
    ]
    db_session.add_all(policies)
    await db_session.commit()

    stmt = select(Policy).where(Policy.resource == "custom")
    auth_policies = (await db_session.execute(stmt)).scalars().all()
    assert len(auth_policies) == 4

    role = Role(name="custom")
    for policy in auth_policies:
        role.policies.append(policy)

    db_session.add(role)
    await db_session.commit()
    await db_session.refresh(role)

    assert len(role.policies) == 4
