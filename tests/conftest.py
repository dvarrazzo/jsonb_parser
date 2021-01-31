import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--test-dsn",
        metavar="DSN",
        default=os.environ.get("JSONB_TEST_DSN"),
        help="Connection string to run database tests requiring a connection"
        " [you can also use the JSONB_TEST_DSN env var].",
    )


def pytest_configure(config):
    # register slow marker
    config.addinivalue_line(
        "markers", "slow: this test is kinda slow (skip with -m 'not slow')"
    )


@pytest.fixture(scope="session")
def dsn(request):
    """Return the dsn used to connect to the `--test-dsn` database."""
    dsn = request.config.getoption("--test-dsn")
    if dsn is None:
        pytest.skip("skipping test as no --test-dsn")
    return dsn


@pytest.fixture
def conn(dsn):
    """Return a `Connection` connected to the ``--test-dsn`` database.

    The connection is autocommit and the database will contain a jsonb to bytea
    cast that can be used in the tests.
    """
    import psycopg2

    conn = psycopg2.connect(dsn)
    conn.autocommit = True
    ensure_jsonb_bytea_cast(conn)
    yield conn
    conn.close()


def ensure_jsonb_bytea_cast(conn):
    GET_CAST_SQL = """
        select castmethod from pg_cast
        where castsource::regtype = 'jsonb'::regtype
        and casttarget::regtype = 'bytea'::regtype
        """
    cur = conn.cursor()
    cur.execute(GET_CAST_SQL)
    rec = cur.fetchone()
    if not rec:
        cur.execute("create cast (jsonb as bytea) without function")
        cur.execute(GET_CAST_SQL)
        rec = cur.fetchone()
    if rec[0] != "b":
        pytest.fail("jsonb -> bytea cast is not binary")
