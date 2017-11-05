#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import sqlite3

from kiwiii.core.node import Task

data_type = {
    "compound_id": "text",
    "text": "text",
    "numeric": "real",
    "count": "integer",
    "flag": "integer"
}


class SQLiteWriter(Task):
    def __init__(self, in_edges, wf, dest_path, allow_overwrite=True):
        super().__init__()
        self._in_edges = in_edges
        self.wf = wf
        self.dest_path = dest_path
        self.allow_overwrite = allow_overwrite

    def in_edges(self):
        return self._in_edges

    def out_edges(self):
        return tuple()

    def run(self):
        self.on_start()
        conn = sqlite3.connect(self.dest_path)
        conn.isolation_level = None
        cur = conn.cursor()
        cur.execute("PRAGMA page_size = 4096")
        cur.execute("BEGIN")
        try:
            if os.path.exists(self.dest_path) and self.allow_overwrite:
                # This can be rolled back when the operation failed
                # unlike delete file and generate new one
                print("Truncate existing database ...")
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]
                for t in tables:
                    cur.execute("DROP TABLE {}".format(t))
                    print("Table {} dropped".format(t))
            cur.execute("CREATE TABLE document(document text)")
            schema = self.wf.params
            schema["resources"] = []
            for in_edge in self.in_edges():
                # Create table
                sqcols = []
                for field in in_edge.fields:
                    sqtype = " {}".format(
                        data_type.get(field.get("valueType"), "text"))
                    sqpk = ""
                    if field["key"] == "id":
                        sqpk = " primary key check(id != '')"
                    sqnocase = ""
                    if field.get("valueType", "text") in ("text",):
                        sqnocase = " collate nocase"
                    sqcol = "".join((field["key"], sqtype, sqpk, sqnocase))
                    sqcols.append(sqcol)
                sql = "CREATE TABLE {} ({})".format(in_edge.params["table"],
                                                    ", ".join(sqcols))
                cur.execute(sql)
                # Insert records
                for i, rcd in enumerate(in_edge.records):
                    sqflds = "{} ({})".format(
                        in_edge.params["table"], ", ".join(rcd.keys()))
                    ph = ", ".join(["?"] * len(rcd))
                    sql_row = "INSERT INTO {} VALUES ({})".format(sqflds, ph)
                    values = [rcd[c] for c in rcd.keys()]
                    try:
                        cur.execute(sql_row, values)
                    except sqlite3.IntegrityError as e:
                        print("skip #{}: {}".format(i, e))
                    if i and not i % 100:
                        print("{} rows processed...".format(i))
                cnt = cur.execute(
                    "SELECT COUNT(*) FROM {}".format(in_edge.params["table"]))
                print("{} rows -> {}".format(
                    cnt.fetchone()[0], in_edge.params["table"]))
                # Resources
                schema["resources"].append(in_edge.params)
            """Save document"""
            cur.execute(
                "INSERT INTO document VALUES (?)",
                (json.dumps(schema),)
            )
        except ValueError:  # TODO: which error raised when interrupt
            self.on_aborted()
        else:
            self.wf.done_count = self.wf.result_count = self.in_edge.task_count
            self.on_finish()

    def on_submitted(self):
        self.wf.task_count = sum(i.task_count for i in self._in_edges)
        self.wf.result_count = 0
        self.wf.done_count = 0
        if os.path.exists(self.dest_path) and not self.allow_overwrite:
            raise ValueError("SQLite file already exists.")

    def interrupt(self):
        # TODO: core.workflow will call this when interrupted
        # TODO: conn.interrupt()
        pass
