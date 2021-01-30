import json
import pytest

from jsonb_parser import parse_jsonb


@pytest.mark.parametrize("value", [None, True, False, "hello", 42])
def test_scalar(conn, value):
    cur = conn.cursor()
    cur.execute("select %s::jsonb::bytea", (json.dumps(value),))
    data = cur.fetchone()[0]
    got = parse_jsonb(data)
    assert value == got
