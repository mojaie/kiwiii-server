#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqlitehelper as helper
from kiwiii.core.node import Node
from kiwiii.core.edge import Edge


def resource_fields(tables):
    return helper.resource_fields(tables)


class SQLiteInput(Node):
    def __init__(self, query):
        super().__init__()
        self.out_edge = Edge()
        self.query = query
        self.out_edge.task_count = helper.record_count(
            query["tables"], query["resourceFile"])

    def run(self):
        self.out_edge.records = helper.records_iter(
            self.query["tables"], self.query["resourceFile"])
        self.on_finish()

    def in_edges(self):
        return tuple()


class SQLiteSearchInput(SQLiteInput):
    def run(self):
        self.out_edge.records = helper.first_match(
            self.query["tables"],
            self.query["resourceFile"],
            self.query["key"],
            self.query["values"]
        )
        self.on_finish()


class SQLiteFilterInput(SQLiteInput):
    def run(self):
        self.out_edge.records = helper.find_all(
            self.query["tables"],
            self.query["resourceFile"],
            self.query["key"],
            self.query["values"],
            self.query["operator"]
        )
        self.on_finish()
