#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqliteconnection as sqlite
from kiwiii.node.node import Node, Edge


class SQLiteQuery(Node):
    def __init__(self, query=None):
        self.out_edge = Edge()
        self.query = query

    def run(self):
        if self.query["type"] == "all":
            self.out_edge.records = sqlite.records_iter(self.query)
        elif self.query["operator"] == "fm":
            if self.query["type"] == "chem":
                self.out_edge.records = sqlite.chem_first_match(self.query)
            else:
                self.out_edge.records = sqlite.first_match(self.query)
        else:
            if self.query["type"] == "chem":
                self.out_edge.records = sqlite.chem_find_all(self.query)
            else:
                self.out_edge.records = sqlite.find_all(self.query)

    def out_edges(self):
        return (self.out_edge,)
