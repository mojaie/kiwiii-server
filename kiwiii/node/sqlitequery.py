#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.node.node import Node, Edge
from kiwiii.node import sqlitehelper as helper


class SQLiteQuery(Node):
    def __init__(self, type_, query=None):
        self.out_edge = Edge()
        self.query = query
        self.func = {
            "all": helper.records_iter,
            "search": helper.first_match,
            "filter": helper.find_all
        }[type_]

    def run(self):
        self.out_edge.records = self.func(self.query)

    def out_edges(self):
        return (self.out_edge,)
