#  Copyright 2021 Zeppelin Bend Pty Ltd
#
#  This Source Code Form is subject to the terms of the Mozilla Public
#  License, v. 2.0. If a copy of the MPL was not distributed with this
#  file, You can obtain one at https://mozilla.org/MPL/2.0/.

import sqlite3
from contextlib import contextmanager
from typing import Generator, List, Iterable

from datetime import date, datetime
from dataclassy import dataclass

from load_synthesiser.db.energy_profile import EnergyProfile, EnergyProfileStat, accumulating, sx as eps_sx
from load_synthesiser.db.id_date_range import IdDateRange
from load_synthesiser.db.readings import Readings, sx as readings_sx
from load_synthesiser.db.cacheable_serialiser import sx as cacheable_sx

SCHEMA = [
    "CREATE TABLE schema_version (version TEXT);",
    "CREATE TABLE entity_ids (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT);",
    "CREATE UNIQUE INDEX entity_ids_idx on entity_ids (entity_id);",
    "CREATE TABLE metadata (key TEXT, value TEXT);",
    "CREATE TABLE W_out (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID;",
    "CREATE TABLE W_in (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID;",
    "CREATE TABLE cacheable (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID;",
    "CREATE TABLE maximums (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID;"
]


def write_database(path: str):
    db = SqliteReadingsDatabase(path)
    db.initialise()
    return db


@dataclass
class SqliteDatabase(object):
    path: str

    @contextmanager
    def connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        yield conn
        conn.commit()
        conn.close()

    @contextmanager
    def cursor(self) -> Generator[sqlite3.Cursor, None, None]:
        with self.connection() as conn:
            cursor = conn.cursor()
            yield cursor
            cursor.close()

    def write_entity_ids(self, entity_ids: Iterable[str]):
        with self.cursor() as c:
            for id in entity_ids:
                c.execute(f"INSERT INTO entity_ids VALUES (NULL, '{id}')")


class SqliteReadingsDatabase(SqliteDatabase):
    SELECT_ID_FROM_INDEX = "(SELECT id FROM entity_ids where entity_id = ?)"
    SQL_INSERT_W_IN_FORMAT = f"INSERT INTO W_in (id, data) VALUES ({SELECT_ID_FROM_INDEX}, ?)"
    SQL_INSERT_W_OUT_FORMAT = f"INSERT INTO W_out (id, data) VALUES ({SELECT_ID_FROM_INDEX}, ?)"
    SQL_INSERT_MAXIMUMS = f"INSERT INTO maximums (id, data) VALUES ({SELECT_ID_FROM_INDEX}, ?)"
    SQL_INSERT_CACHEABLE = f"INSERT INTO cacheable (id, data) VALUES ({SELECT_ID_FROM_INDEX}, ?)"

    def initialise(self):
        self.create_schema()

    def finalise(self, dt: date):
        self.write_metadata(dt.isoformat())

    def create_schema(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            for ddl in SCHEMA:
                cursor.execute(ddl)

    def write_metadata(self, dt):
        with self.cursor() as c:
            c.execute(f"INSERT INTO metadata VALUES ('date', '{dt}')")
            c.execute(f"INSERT INTO metadata VALUES ('timezone', 'Australia/Sydney')")
            c.execute(f"INSERT INTO schema_version VALUES (1)")

    def write_readings(self, energy_profiles: Iterable[EnergyProfile]):
        with self.cursor() as c:
            for profile in energy_profiles:
                serialised_readings_in = readings_sx(profile.kw_in).read("bytes")
                serialised_readings_out = readings_sx(profile.kw_out).read("bytes")
                c.execute(self.SQL_INSERT_W_IN_FORMAT, (profile.id, serialised_readings_in))
                c.execute(self.SQL_INSERT_W_OUT_FORMAT, (profile.id, serialised_readings_out))
                self.write_maximum(c, profile)
                self.write_cacheable(c, profile.id, True)

    def write_maximum(self, cursor, energy_profile: EnergyProfile):
        eps = accumulating(energy_profile, max, lambda l, v: v, 0, 0, -999999999999.9999)
        serialised_eps = eps_sx(eps).read("bytes")
        cursor.execute(self.SQL_INSERT_MAXIMUMS, (energy_profile.id, serialised_eps))

    def write_cacheable(self, cursor, id: str, cacheable: bool):
        serialised = cacheable_sx(cacheable)
        cursor.execute(self.SQL_INSERT_CACHEABLE, (id, serialised))


class IndexSqliteDatabase(SqliteDatabase):
    SCHEMA = ["CREATE TABLE schema_version (version TEXT);",
              "CREATE TABLE entity_ids (id INTEGER PRIMARY KEY AUTOINCREMENT, entity_id TEXT);",
              "CREATE UNIQUE INDEX entity_ids_idx on entity_ids (entity_id);",
              "CREATE TABLE metadata (key TEXT, value TEXT);",
              "CREATE TABLE dateRange (id INTEGER PRIMARY KEY, data BLOB) WITHOUT ROWID;"
              ]
    SELECT_ID_FROM_INDEX = "(SELECT id FROM entity_ids where entity_id = ?)"
    SQL_INSERT_DATE_RANGE = f"INSERT INTO dateRange (id, data) VALUES ({SELECT_ID_FROM_INDEX}, ?)"

    def create_schema(self):
        with self.connection() as conn:
            cursor = conn.cursor()
            for ddl in self.SCHEMA:
                cursor.execute(ddl)
            cursor.execute("INSERT INTO schema_version VALUES (1)")

    def write_index(self, id: str, date_range: IdDateRange):
        with self.cursor() as c:
            serialised_date_range = date_range.sx().read("bytes")
            c.execute(self.SQL_INSERT_DATE_RANGE, (id, serialised_date_range))
