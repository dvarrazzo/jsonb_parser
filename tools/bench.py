#!/usr/bin/env python3
"""Test the speed of json vs. jsonb
"""

import time
import logging
from typing import Any
from argparse import ArgumentParser, Namespace

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
        with conn.cursor() as cur:

            # Return binary data from the postgres
            cur.format = Format.BINARY

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

            t0 = time.time()
            cur.execute("select data from test_jsonb")
            t1 = time.time()
            for row in cur:
                pass
            t2 = time.time()
            logger.info(f"time text: {t1 - t0:f} xfer, {t2 - t1:f} parsing")

            t0 = time.time()
            cur.execute("select data::bytea from test_jsonb -- bytes")
            t1 = time.time()
            for row in cur:
                pass
            t2 = time.time()
            logger.info(f"time bytea: {t1 - t0:f} xfer, {t2 - t1:f} parsing")

            # Register the adapter to parse jsonb from disk format
            JsonLoader.register("bytea", cur)

            t0 = time.time()
            cur.execute("select data::bytea from test_jsonb -- jsonb")
            t1 = time.time()
            for row in cur:
                pass
            t2 = time.time()
            logger.info(
                f"time disk jsonb: {t1 - t0:f} xfer, {t2 - t1:f} parsing"
            )


class JsonLoader(Loader):
    format = Format.BINARY

    def load(self, data: bytes) -> Any:
        return parse_jsonb(data)


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
