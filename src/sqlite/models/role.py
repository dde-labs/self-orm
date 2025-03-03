# ------------------------------------------------------------------------------
# Copyright (c) 2022 Korawich Anuttra. All rights reserved.
# Licensed under the MIT License. See LICENSE in the project root for
# license information.
# ------------------------------------------------------------------------------
from sqlalchemy import ForeignKey, UniqueConstraint
from sqlalchemy.orm import Mapped, relationship, mapped_column
from sqlalchemy.types import Integer, String

from . import Base

# NOTE: This will work with add this line to the Role model.
#
#   policies: Mapped[list["Policy"]] = relationship(
#       secondary=associate_roles_policies,
#       back_populates="roles",
#       uselist=True,
#       viewonly=True,
#   )
# ---
# from sqlalchemy import Column, Table
#
# associate_roles_policies = Table(
#     "associate_roles_policies",
#     Base.metadata,
#     Column("role_id", ForeignKey("roles.id"), primary_key=True),
#     Column("policy_id", ForeignKey("policies.id"), primary_key=True),
# )

# WARNING: This is not work when linking relationship to this object.
#
class RolePolicy(Base):
    __tablename__ = "associate_roles_policies"

    role_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("roles.id"),
        primary_key=True,
    )
    policy_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("policies.id"),
        primary_key=True,
    )

    role: Mapped["Role"] = relationship(
        "Role",
        back_populates="policy_associations",
    )
    policy: Mapped["Policy"] = relationship(
        "Policy",
        back_populates="role_associations",
    )


class Role(Base):
    """A Role model for keep a group of policies that mean one role can handle
    many policies.

        Initial roles that will create when start this application:
            - admin
            - develop
            - monitor
            - anon
    """

    __tablename__ = "roles"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String, unique=True, nullable=False)

    # NOTE: Use with Table object
    # policies: Mapped[list["Policy"]] = relationship(
    #     "Policy",
    #     secondary=associate_roles_policies,
    #     back_populates="roles",
    #     lazy="selectin",
    # )

    policies: Mapped[list["Policy"]] = relationship(
        secondary="associate_roles_policies",
        back_populates="roles",
        lazy="selectin",
    )

    policy_associations: Mapped[list["RolePolicy"]] = relationship(
        "RolePolicy",
        back_populates="role",
    )


class Policy(Base):
    """A Policy model for keep mapping of resource and action that exists on
    your application. A resource is alias of route that you want to assign name
    for it such as at the logs route, you assign resource name is `monitor`.

        Initial phase it will allow to have 4 actions:
            - create: Post
            - update: Put
            - delete: Delete
            - read: Get
    """

    __tablename__ = "policies"
    __table_args__ = (
        UniqueConstraint(
            "resource", "action", name="policies_resource_action_key"
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    resource: Mapped[str] = mapped_column(String(64), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)

    # NOTE: Use with Table object
    # roles: Mapped[list["Role"]] = relationship(
    #     "Role",
    #     secondary=associate_roles_policies,
    #     back_populates="policies",
    #     lazy="selectin",
    # )

    roles: Mapped[list["Role"]] = relationship(
        secondary="associate_roles_policies",
        back_populates="policies",
        lazy="selectin",
    )

    role_associations: Mapped[list["RolePolicy"]] = relationship(
        "RolePolicy",
        back_populates="policy",
    )
