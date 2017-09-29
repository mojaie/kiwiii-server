#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqlitehelper as helper
from kiwiii.core.node import Node
from kiwiii.core.edge import Edge


class SQLiteQuery(Node):
    def __init__(self, type_, query=None):
        super().__init__()
        self.out_edge = Edge()
        self.query = query
        self.func = {
            "all": helper.records_iter,
            "search": helper.first_match,
            "filter": helper.find_all
        }[type_]
        self.out_edge.task_count = helper.record_count(query["targets"])
        self.fields = helper.resource_fields(query["targets"])

    def run(self):
        self.out_edge.records = self.func(self.query)
        self.on_finish()

    def in_edges(self):
        return tuple()
