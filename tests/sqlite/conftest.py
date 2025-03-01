import pytest


@pytest.fixture(scope='session', autouse=True)
def setup_sqlite():
    print("Start setup SQLite database")
