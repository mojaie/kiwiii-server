#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import sqlite3
import traceback

from kiwiii.core.node import Task

data_type = {
    "compound_id": "text",
    "text": "text",
    "numeric": "real",
    "count": "integer",
    "flag": "integer"
}


class SQLiteWriter(Task):
    def __init__(self, in_edges, wf, dest_path,
                 allow_overwrite=True, notice_per_records=10000):
        super().__init__()
        self._in_edges = in_edges
        self.wf = wf
        self.dest_path = dest_path
        self.allow_overwrite = allow_overwrite
        self.notice_per_records = notice_per_records
        self.conn = None

    def in_edges(self):
        return self._in_edges

    def out_edges(self):
        return tuple()

    def run(self):
        self.on_start()
        self.conn = sqlite3.connect(self.dest_path)
        self.conn.isolation_level = None
        cur = self.conn.cursor()
        cur.execute("PRAGMA page_size = 4096")
        cur.execute("BEGIN")
        try:
            # Truncate tables should be done in the transaction.
            if os.path.exists(self.dest_path) and self.allow_overwrite:
                print("Truncate existing database ...")
                cur.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'")
                tables = [row[0] for row in cur.fetchall()]
                for t in tables:
                    cur.execute("DROP TABLE {}".format(t))
                    print("Table {} dropped".format(t))
            # Schema document table
            cur.execute("CREATE TABLE document(document text)")
            schema = self.wf.params
            schema["resources"] = []
            # Tables
            for in_edge in self.in_edges():
                # Create table
                phrases = []
                table_name = in_edge.params["table"]
                for field in in_edge.fields:
                    fieldphrase = field["key"]
                    sqtype = data_type.get(field.get("valueType"), "text")
                    fieldphrase += " {}".format(sqtype)
                    if field["key"] == "id":
                        fieldphrase += " primary key check(id != '')"
                    if sqtype == "text":
                        fieldphrase += " collate nocase"
                    phrases.append(fieldphrase)
                fielddef = ", ".join(phrases)
                sql = "CREATE TABLE {} ({})".format(table_name, fielddef)
                cur.execute(sql)
                # Insert records
                for i, rcd in enumerate(in_edge.records):
                    fieldkeys = ", ".join(rcd.keys())
                    sql = "INSERT INTO {} ({})".format(table_name, fieldkeys)
                    placeholders = ", ".join(["?"] * len(rcd))
                    sql += " VALUES ({})".format(placeholders)
                    values = [rcd[c] for c in rcd.keys()]
                    try:
                        cur.execute(sql, values)
                    except sqlite3.IntegrityError as e:
                        print("skip #{}: {}".format(i, e))
                    if i and not i % self.notice_per_records:
                        print("{} rows processed...".format(i))
                cnt = cur.execute("SELECT COUNT(*) FROM {}".format(table_name))
                print("{} rows -> {}".format(cnt.fetchone()[0], table_name))
                # Append a resource schema
                table_schema = in_edge.params
                table_schema["fields"] = in_edge.fields
                schema["resources"].append(table_schema)
            # Save database schema
            cur.execute(
                "INSERT INTO document VALUES (?)", (json.dumps(schema),))
        except KeyboardInterrupt:
            print("User cancel")
            self.conn.rollback()
            self.conn.close()
            self.on_aborted()
        except:
            print(traceback.format_exc())
            self.conn.rollback()
            self.conn.close()
            self.on_aborted()
        else:
            self.conn.commit()
            print("Cleaning up...")
            cur.execute("VACUUM")
            self.conn.close()
            self.wf.done_count = self.wf.result_count = self.wf.task_count
            self.on_finish()

    def on_submitted(self):
        self.wf.task_count = sum(i.task_count for i in self.in_edges())
        self.wf.result_count = 0
        self.wf.done_count = 0
        if os.path.exists(self.dest_path) and not self.allow_overwrite:
            raise ValueError("SQLite file already exists.")

    def interrupt(self):
        # TODO: core.workflow will call this when interrupted
        print("interrupt called")
        self.conn.interrupt()
