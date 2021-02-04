#!/usr/bin/env python3
"""Test the speed of json vs. jsonb
"""

import time
import logging
from typing import Any
from argparse import ArgumentParser, Namespace
from collections import defaultdict

import orjson
import psycopg3
from psycopg3.pq import Format
from psycopg3.types.json import Json
from psycopg3.adapt import Loader

from jsonb_parser import parse_jsonb
from jsonb_parser.faker import JsonFaker

logger = logging.getLogger()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
)


def make_random_table(opt: Namespace) -> None:
    with psycopg3.connect(opt.dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            logger.info("creating jsonb table if necessary")
            cur.execute(
                """
                create table if not exists test_jsonb (
                    id serial primary key,
                    data jsonb)
                """
            )
            cur.execute("select count(*) from test_jsonb")
            nrecs = cur.fetchone()[0]  # type: ignore[index]

            if opt.make_random is not None:
                if nrecs < opt.make_random:
                    logger.info(f"adding {opt.make_random - nrecs} records")
                    faker = JsonFaker(contchance=0.25)
                    for i in range(opt.make_random - nrecs):
                        j = faker.random_container()
                        cur.execute(
                            "insert into test_jsonb (data) values (%s)",
                            [Json(j)],
                        )
                elif nrecs > opt.make_random:
                    logger.info(f"removing {nrecs - opt.make_random} records")
                    cur.execute(
                        "select id from test_jsonb order by id desc limit %s",
                        [nrecs - opt.make_random],
                    )
                    min_id = cur.fetchall()[-1][0]
                    cur.execute(
                        "delete from test_jsonb where id >= %s", [min_id]
                    )

                if nrecs != opt.make_random:
                    logger.info("vacuuming")
                    cur.execute("vacuum analyze test_jsonb")

        ensure_jsonb_bytea_cast(conn)


def main() -> None:
    opt = parse_cmdline()
    make_random_table(opt)

    with psycopg3.connect(opt.dsn, autocommit=True) as conn:

        queries = {
            "jsonb": "select data from test_jsonb",
            "orjson": "select data from test_jsonb",
            "bytea": "select data::bytea from test_jsonb",
            "jsonb-disk": "select data::bytea from test_jsonb",
        }
        timings = defaultdict(list)

        def test(cur: psycopg3.Cursor, title: str) -> None:
            t0 = time.time()
            cur.execute(queries[title])
            t1 = time.time()
            for row in cur:
                pass
            t2 = time.time()
            logger.info(
                f"time {title}: {t1-t0:f} xfer, {t2-t1:f} parsing, {t2-t0:f} total"
            )
            timings[title].append((t0, t1, t2))

        with conn.cursor() as cur:

            logger.info("warming up")
            cur.execute(
                """
                select
                    count(*),
                    pg_size_pretty(pg_total_relation_size('test_jsonb'))
                from test_jsonb"""
            )
            nrecs, size = cur.fetchone()  # type: ignore
            cur.execute("select data from test_jsonb")
            logger.info(f"number of records: {nrecs}, table size {size}")

            for i in range(3):
                cur = conn.cursor()
                test(cur, "jsonb")

                cur = conn.cursor()
                ORJsonLoader.register("jsonb", cur)
                test(cur, "orjson")

                cur = conn.cursor(binary=True)
                test(cur, "bytea")

                cur = conn.cursor(binary=True)
                JsonbByteaLoader.register("bytea", cur)
                test(cur, "jsonb-disk")

    bests = sorted(
        (min(t2 - t0 for t0, _, t2 in timings[title]), title)
        for title in queries
    )
    for t, title in bests:
        logger.info(f"best for {title}: {t:f} sec")


class JsonbByteaLoader(Loader):
    format = Format.BINARY

    def load(self, data: bytes) -> Any:
        return parse_jsonb(data)


class ORJsonLoader(Loader):

    format = Format.TEXT

    def load(self, data: bytes) -> Any:
        # memoryview not supported
        if isinstance(data, memoryview):
            data = bytes(data)
        return orjson.loads(data)


def parse_cmdline() -> Namespace:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument(
        "--make-random",
        metavar="SIZE",
        type=int,
        help="create a random table with SIZE random values",
    )
    parser.add_argument(
        "--dsn", default="", help="where to connect [default: %(default)r]"
    )

    opt = parser.parse_args()

    return opt


def ensure_jsonb_bytea_cast(conn: psycopg3.Connection) -> None:
    GET_CAST_SQL = """
        select castmethod from pg_cast
        where castsource::regtype = 'jsonb'::regtype
        and casttarget::regtype = 'bytea'::regtype
        """
    cur = conn.cursor()
    cur.execute(GET_CAST_SQL)
    rec = cur.fetchone()
    if not rec:
        logger.info("creating jsonb to bytea cast")
        cur.execute("create cast (jsonb as bytea) without function")
        cur.execute(GET_CAST_SQL)
        rec = cur.fetchone()
    if not rec or rec[0] != "b":
        raise Exception("jsonb -> bytea cast is not binary")


if __name__ == "__main__":
    main()
