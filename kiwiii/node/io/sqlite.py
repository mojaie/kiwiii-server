#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.sqlitehelper import SQLITE_HELPER as sq
from kiwiii.core.node import Node, Synchronizer


class SQLiteInput(Node):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = sq.resource_fields(query["targets"])

    def on_submitted(self):
        self.out_edge.records = sq.records_iter(self.query["targets"])
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.extend(self.in_edge.fields)
        self.out_edge.fields.extend(self.fields)

    def in_edges(self):
        return tuple()


class SQLiteSearchInput(SQLiteInput):
    def on_submitted(self):
        self.out_edge.records = (
            sq.search(self.query["targets"], self.query["key"], v)
            for v in self.query["values"]
        )
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.extend(self.fields)


class SQLiteFilterInput(SQLiteInput):
    def on_submitted(self):
        self.out_edge.records = sq.find_all(
            self.query["targets"], self.query["key"],
            self.query["values"], self.query["operator"],
            fields=self.query.get("fields")
        )
        self.out_edge.task_count = sq.record_count(self.query["targets"])
        self.out_edge.fields.extend(self.fields)


class SQLiteWriter(Synchronizer):
    def __init__(self, query):
        super().__init__()
        self.query = query

    def on_submitted(self):
        self.out_edge.records = sq.records_iter(self.query["targets"])
        self.out_edge.task_count = sq.record_count(self.query["targets"])

    def in_edges(self):
        return tuple()
