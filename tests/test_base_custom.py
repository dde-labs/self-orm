from sqlalchemy import MetaData
from sqlalchemy.orm import DeclarativeBase


DB_INDEXES_NAMING_CONVENTION: dict[str, str] = {
    "ix": "%(column_0_label)s_idx",
    "uq": "%(table_name)s_%(column_0_name)s_key",
    "ck": "%(table_name)s_%(constraint_name)s_check",
    "fk": "%(table_name)s_%(column_0_name)s_fkey",
    "pk": "%(table_name)s_pkey",
}


class Base(DeclarativeBase):
    """Subclass of DeclarativeBase that use to implement this application
    custom metadata.
    """

    __abstract__ = True

    metadata = MetaData(
        naming_convention=DB_INDEXES_NAMING_CONVENTION,
        # NOTE: In SQLite schema, the value should be `main` only because it
        #   does not implement with schema system.
        schema="main",
    )

    def __repr__(self) -> str:
        columns = ", ".join(
            [
                f"{k}={repr(v)}"
                for k, v in self.__dict__.items()
                if not k.startswith("_")
            ]
        )
        return f"<{self.__class__.__name__}({columns})>"
