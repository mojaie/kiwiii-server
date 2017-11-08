#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.sqliteconnection import Connection
from kiwiii.sqlitehelper import SQLITE_HELPER as sq
from kiwiii.core.node import Node


class SQLiteInput(Node):
    def __init__(self, query, params=None):
        super().__init__()
        self.query = query
        self.fields = sq.resource_fields(query["targets"])
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self.out_edge.records = sq.records_iter(self.query["targets"])
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)

    def in_edges(self):
        return tuple()


class SQLiteSearchInput(SQLiteInput):
    def on_submitted(self):
        self.out_edge.records = (
            sq.search(self.query["targets"], self.query["key"], v)
            for v in self.query["values"]
        )
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)


class SQLiteFilterInput(SQLiteInput):
    def on_submitted(self):
        self.out_edge.records = sq.find_all(
            self.query["targets"], self.query["key"],
            self.query["values"], self.query["operator"],
            fields=self.query.get("fields")
        )
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)


class SQLiteCustomQueryInput(Node):
    def __init__(self, sql, table, fields=None):
        super().__init__()
        self.sql = sql
        self.table = table
        if fields is None:
            self.fields = []
        else:
            self.fields = fields

    def on_submitted(self):
        conn = Connection()
        self.out_edge.records = conn.fetch_iter(self.sql)
        self.out_edge.task_count = conn.rows_count(self.table)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)

    def in_edges(self):
        return tuple()
