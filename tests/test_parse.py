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


def test_random(conn):
    value = JsonFaker().random_json()
    got = roundtrip(conn, value)
    assert got == value


def roundtrip(conn, obj):
    cur = conn.cursor()
    cur.execute("select %s::jsonb::bytea", (json.dumps(obj),))
    data = cur.fetchone()[0]
    return parse_jsonb(data)


class JsonFaker:
    def __init__(self, contchance=0.66, contmax=100, strmax=100, keymax=50):
        self.contchance = contchance
        self.contmax = contmax
        self.strmax = strmax
        self.keymax = keymax

    def random_json(self, contchance=None):
        if contchance is None:
            contchance = self.contchance
        if random() < contchance:
            return self.random_container(contchance=contchance)
        else:
            return self.random_scalar()

    def random_container(self, contchance=None):
        cont = choice([list, dict])
        if cont is list:
            return self.random_list(contchance=contchance)
        elif cont is dict:
            return self.random_object(contchance=contchance)
        else:
            assert False, f"unknown container type: {cont}"

    def random_scalar(self):
        val = choice([None, True, False, str])  # TODO: numbers
        if val is str:
            return self.random_string()
        else:
            return val

    def random_list(self, contchance=None):
        if contchance is None:
            contchance = self.contchance
        return [
            self.random_json(contchance=contchance / 2.0)
            for i in range(randrange(self.contmax))
        ]

    def random_object(self, contchance=None):
        if contchance is None:
            contchance = self.contchance
        return {
            self.random_string(self.keymax): self.random_json(
                contchance=contchance / 2.0
            )
            for i in range(randrange(self.contmax))
        }

    def random_string(self, strmax=None, unichance=0.2):
        if strmax is None:
            strmax = self.strmax

        length = randrange(strmax)

        rv = []
        while len(rv) < length:
            if random() < unichance:
                c = randrange(1, 0x110000)
                if 0xD800 <= c <= 0xDBFF or 0xDC00 <= c <= 0xDFFF:
                    continue
            else:
                c = randrange(1, 128)
            rv.append(c)

        return "".join(map(chr, rv))
