#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.sqlitehelper import SQLITE_HELPER as sq
from kiwiii.core.node import Node
from kiwiii.core.edge import Edge


class SQLiteInput(Node):
    def __init__(self, query):
        super().__init__()
        self.out_edge = Edge()
        self.query = query
        self.out_edge.task_count = sq.record_count(query["targets"])

    def run(self):
        self.out_edge.records = sq.records_iter(self.query["targets"])
        self.on_finish()

    def in_edges(self):
        return tuple()


class SQLiteSearchInput(SQLiteInput):
    def run(self):
        self.out_edge.records = itertools.chain.from_iterable(
            sq.search(self.query["targets"], self.query["key"], v)
            for v in self.query["values"]
        )
        self.on_finish()


class SQLiteFilterInput(SQLiteInput):
    def run(self):
        self.out_edge.records = sq.find_all(
            self.query["targets"], self.query["key"],
            self.query["values"], self.query["operator"]
        )
        self.on_finish()
