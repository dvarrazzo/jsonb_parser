import json
import pytest
from string import ascii_letters
from random import random, randrange, choice

from jsonb_parser import parse_jsonb

EUR = "\u20ac"
POO = "\U0001F4A9"


@pytest.mark.parametrize("value", [None, True, False, "hello", EUR, POO, 42])
def test_scalar(conn, value):
    if value == 42:
        pytest.xfail("numeric please")
    got = roundtrip(conn, value)
    assert got == value


def test_array_of_chars(conn):
    # test list of 1-char elements
    for i in range(len(ascii_letters) + 1):
        value = list(ascii_letters[:i])
        got = roundtrip(conn, value)
        assert got == value


def test_object_of_chars(conn):
    # test list of 1-char elements
    for i in range(len(ascii_letters) + 1):
        value = {c: c for c in ascii_letters[:i]}
        got = roundtrip(conn, value)
        assert got == value


@pytest.mark.parametrize(
    "value", ["[]", "[[]]", "[[[]]]", "[[], []]", '["a", []]']
)
def test_array(conn, value):
    value = eval(value)
    got = roundtrip(conn, value)
    assert got == value


@pytest.mark.parametrize(
    "value", ["{}", '{"a": "bb"}', '{"a": ["b", "c"]}', '{"a": {"b": "c"}}']
)
def test_object(conn, value):
    value = eval(value)
    got = roundtrip(conn, value)
    assert got == value


def roundtrip(conn, obj):
    cur = conn.cursor()
    cur.execute("select %s::jsonb::bytea", (json.dumps(obj),))
    data = cur.fetchone()[0]
    return parse_jsonb(data)
