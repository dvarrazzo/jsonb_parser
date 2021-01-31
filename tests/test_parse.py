import json
import pytest
from string import ascii_letters

from jsonb_parser import parse_jsonb


@pytest.mark.parametrize("value", [None, True, False, "hello", 42])
def test_scalar(conn, value):
    cur = conn.cursor()
    got = roundtrip(cur, value)
    assert got == value


def test_list_of_chars(conn):
    # test list of 1-char elements
    cur = conn.cursor()
    for i in range(len(ascii_letters) + 1):
        value = ascii_letters[:i]
        got = roundtrip(cur, value)
        assert got == value


@pytest.mark.parametrize(
    "value", ["[]", "[[]]", "[[[]]]", "[[], []]", '["a", []]']
)
def test_array(conn, value):
    cur = conn.cursor()
    value = eval(value)
    got = roundtrip(cur, value)
    assert got == value


def roundtrip(cur, obj):
    cur.execute("select %s::jsonb::bytea", (json.dumps(obj),))
    data = cur.fetchone()[0]
    return parse_jsonb(data)
