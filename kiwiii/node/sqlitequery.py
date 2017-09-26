#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqlitehelper as helper
from kiwiii.task import Node, Edge


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

    def run(self):
        self.out_edge.records = self.func(self.query)
        self.on_finish()

    def out_edges(self):
        return (self.out_edge,)
