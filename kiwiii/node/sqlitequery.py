#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#


from tornado import gen
from kiwiii import sqliteconnection as sqlite
from kiwiii.node.node import Node, Edge


class SQLiteQuery(Node):
    def __init__(self, query=None):
        self.out_edge = Edge()
        self.query = query

    @gen.coroutine
    def run(self):
        if self.query is None:
            self.out_edge.data = sqlite.records_iter()
        elif self.query["operator"] == "fm":
            if self.query["type"] == "chem":
                self.out_edge.data = sqlite.chem_first_match(self.query)
            else:
                self.out_edge.data = sqlite.first_match(self.query)
        else:
            if self.query["type"] == "chem":
                self.out_edge.data = sqlite.chem_find_all(self.query)
            else:
                self.out_edge.data = sqlite.find_all(self.query)

    def out_edges(self):
        return (self.out_edge,)
