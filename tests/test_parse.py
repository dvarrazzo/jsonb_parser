import json
import pytest
from string import ascii_letters
from random import random, randint, randrange, choice

from jsonb_parser import parse_jsonb

EUR = "\u20ac"
POO = "\U0001F4A9"


@pytest.mark.parametrize("value", [None, True, False, "hello", EUR, POO, 0, 1])
def test_scalar(conn, value):
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
    "value",
    [
        "{}",
        '{"a": "bb"}',
        '{"a": ["b", "c"]}',
        '{"a": {"b": "c"}}',
        '{"X": -23719158070000003380}',
        '{"x": 1, "": 2, "zz": 3}',
    ],
)
def test_object(conn, value):
    value = eval(value)
    got = roundtrip(conn, value)
    assert got == value


def test_numbers(conn):
    cur = conn.cursor()

    funcs = [
        (lambda i: "1" + "0" * i),
        (lambda i: "-1" + "0" * i),
        (lambda i: "0." + "0" * i + "1"),
        (lambda i: "-0." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "1"),
        (lambda i: "1." + "0" * i + "10"),
        (lambda i: "1" + "0" * i + ".001"),
    ]

    for i in range(30):
        for f in funcs:
            snum = f(i)
            cur.execute("select %s::jsonb::bytea", (snum,))
            data = cur.fetchone()[0]
            got = parse_jsonb(data)
            assert got == pytest.approx(float(snum))


@pytest.mark.slow
def test_random(conn, faker):
    value = faker.random_json()
    with faker.reduce(value):
        got = roundtrip(conn, value)
    assert got == value


def roundtrip(conn, obj):
    cur = conn.cursor()
    cur.execute("select %s::jsonb::bytea", (json.dumps(obj),))
    data = cur.fetchone()[0]
    return parse_jsonb(data)


@pytest.fixture
def faker(conn):
    return JsonFaker(conn)


class JsonFaker:
    def __init__(
        self, conn, contchance=0.66, contmax=100, strmax=100, keymax=50
    ):
        self.conn = conn
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

    def reduce(self, value):
        return Reducer(self, value)

    def random_container(self, contchance=None):
        cont = choice([list, dict])
        if cont is list:
            return self.random_list(contchance=contchance)
        elif cont is dict:
            return self.random_object(contchance=contchance)
        else:
            assert False, f"unknown container type: {cont}"

    def random_scalar(self):
        typ = choice([bool, str, int, float])
        meth = getattr(self, f"random_{typ.__name__}")
        return meth()

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
            self.random_str(self.keymax): self.random_json(
                contchance=contchance / 2.0
            )
            for i in range(randrange(self.contmax))
        }

    def random_str(self, strmax=None, unichance=0.2):
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

    def random_bool(self):
        # I give you a None for free too
        return choice([None, True, False])

    def random_int(self):
        return randint(-100000000000000000000, 1000000000000000000)

    def random_float(self):
        n = self.random_int()
        return n * 10 ^ randint(-20, 20)


class Reducer:
    """
    Bisect a container to find the minimal data failing a test.
    """

    def __init__(self, faker, value):
        self.conn = faker.conn
        self.value = value

    def __enter__(self):
        return self

    def __exit__(self, t, e, tb):
        if not e:
            return

        rval = self.reduce(self.value)
        # Raise the exception with the reduced item only
        roundtrip(self.conn, rval)

    def reduce(self, value):
        if isinstance(value, list):
            return self.reduce_list(value)
        elif isinstance(value, dict):
            return self.reduce_dict(value)
        else:
            return value

    def reduce_list(self, value):
        for item in value:
            try:
                roundtrip(self.conn, item)
            except Exception:
                return self.reduce(item)

        # couldn't reduce
        return value

    def reduce_dict(self, value):
        for k, v in value.items():
            try:
                roundtrip(self.conn, k)
            except Exception:
                return k
            try:
                roundtrip(self.conn, v)
            except Exception:
                return self.reduce(v)

        # couldn't reduce to a single element: bisect
        if len(value) > 1:
            keys = list(value.keys())
            halves = [
                {k: value[k] for k in keys[: len(keys) // 2]},
                {k: value[k] for k in keys[len(keys) // 2 :]},
            ]
            for half in halves:
                try:
                    roundtrip(self.conn, half)
                except Exception:
                    return self.reduce(half)

        # reduce by removing one element at time
        for k in value:
            reduced = {k1: v for k1, v in value.items() if k1 != k}
            try:
                roundtrip(self.conn, reduced)
            except Exception:
                return self.reduce(reduced)

        # couldn't reduce further
        return value
