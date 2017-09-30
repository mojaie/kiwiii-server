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


class SQLiteQuery(Node):
    def __init__(self, type_, query):
        super().__init__()
        self.out_edge = Edge()
        self.query = [query["tables"], query["resourceFile"]]
        if type_ != "all":
            self.query.extend([query["key"], query["values"]])
        if type_ == "filter":
            self.query.append(query["operator"])
        self.func = {
            "all": helper.records_iter,
            "search": helper.first_match,
            "filter": helper.find_all
        }[type_]
        self.out_edge.task_count = helper.record_count(
            query["tables"], query["resourceFile"])
        self.type_ = type_

    def run(self):
        self.out_edge.records = self.func(*self.query)
        self.on_finish()

    def in_edges(self):
        return tuple()
