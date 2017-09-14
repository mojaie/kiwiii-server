#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle
import sqlite3

from chorus import molutil
from kiwiii import loader
from kiwiii.definition import molobj
from kiwiii.util import lod


class Connection(object):

    def __init__(self, path):
        con = sqlite3.connect(path)
        con.row_factory = sqlite3.Row
        self._cursor = con.cursor()

    def document(self):
        sql = "SELECT * FROM document"
        return json.loads(self._cursor.execute(sql).fetchone()["document"])

    def rows_iter(self, tables=None, orderby="", arraysize=1000):
        """Iterate over rows of tables"""
        if tables is None or not len(tables):
            tables = [t["id"] for t in self.document()["tables"]]
        if orderby != "":
            orderby = " ORDER BY " + ", ".join(orderby)
        for table in tables:
            sql = "SELECT * FROM {}{}".format(table, orderby)
            self._cursor.execute(sql)
            while True:
                # successive fetchmany(arraysize) instead of fetchAll()
                # improved performance and memory consumption
                rows = self._cursor.fetchmany(arraysize)
                if not rows:
                    break
                for row in rows:
                    yield row

    def rows_count(self, tables=None):
        """Returns number of records of tables"""
        if tables is None or not len(tables):
            tables = [t["id"] for t in self.document()["tables"]]
        cnts = []
        for table in tables:
            sql = "SELECT count(*) FROM {}".format(table)
            cnts.append(self._cursor.execute(sql).fetchone()["count(*)"])
        return sum(cnts)

    def find_iter(self, key, values, tables=None, op="=", arraysize=1000):
        """find records and return result generator"""
        if tables is None or not len(tables):
            tables = [t["id"] for t in self.document()["tables"]]
        if op == "IN":
            ph = "({})".format(", ".join(["?"] * len(values)))
            where = " WHERE {} in{}".format(key, ph)
        else:
            where = " WHERE {} {} ?".format(key, op)
        for table in tables:
            sql = "SELECT * FROM {}{}".format(table, where)
            self._cursor.execute(sql, values)
            while True:
                rows = self._cursor.fetchmany(arraysize)
                if not rows:
                    break
                for row in rows:
                    yield row

    def find_first(self, key, values, tables=None):
        """find records and return first one

        Returns:
            dict: result rows
            None: if nothing is found
        """
        if tables is None or not len(tables):
            tables = [t["id"] for t in self.document()["tables"]]
        for table in tables:
            sql = "SELECT * FROM {} WHERE {}=?".format(table, key)
            res = self._cursor.execute(sql, values).fetchone()
            if res:
                return res


def resources_iter(targets):
    dbs = {}
    for trgt in targets:
        db, table = trgt.split(':')
        if db not in dbs:
            dbs[db] = Connection(loader.sqlite_path(db))
        yield dbs[db], table


def records_iter(query):
    for conn, tbl in resources_iter(query["targets"]):
        for res in conn.rows_iter((tbl["table"],)):
            row = dict(res)
            row["source"] = tbl["id"]
            yield row


def first_match(query):
    for val in query["values"]:
        for conn, tbl in resources_iter(query["targets"]):
            dc_exists = lod.find("dataColumn", query["key"], tbl["columns"])
            if dc_exists is None:
                continue
            res = conn.find_first(query["key"], (val,), (tbl["table"],))
            if res is not None:
                row = dict(res)
                row["source"] = tbl["id"]
                yield row
                break
        else:
            yield {query["key"]: val}


def chem_first_match(query):
    for val in query["values"]:
        for conn, tbl in resources_iter(query["targets"]):
            key_exists = lod.find("key", query["key"], tbl["columns"])
            if key_exists is None:
                continue
            res = conn.find_first(query["key"], (val,), (tbl["table"],))
            if res is not None:
                row = dict(res)
                row["source"] = tbl["id"]
                yield row
                break
        else:
            null_record = {query["key"]: val}
            null_record[molobj["key"]] = pickle.dumps(
                molutil.null_molecule().jsonized())
            return null_record


def find_all(query):
    op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
          "lk": "LIKE", "in": "IN"}[query["operator"]]
    for conn, tbl in resources_iter(query["targets"]):
        dc_exists = lod.find("dataColumn", query["key"], tbl["columns"])
        if dc_exists is None:
            continue
        for res in conn.find_iter(query["key"], query["values"],
                                  (tbl["table"],), op):
            row = dict(res)
            row["source"] = tbl["id"]
            yield row


def chem_find_all(query):
    op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
          "lk": "LIKE", "in": "IN"}[query["operator"]]
    for conn, tbl in resources_iter(query["targets"]):
        key_exists = lod.find("key", query["key"], tbl["columns"])
        if key_exists is None:
            continue
        for res in conn.find_iter(query["key"], query["values"],
                                  (tbl["table"],), op):
            row = dict(res)
            row["source"] = tbl["id"]
            yield row
