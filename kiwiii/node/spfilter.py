#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.node.node import Node, Edge


class SPFilter(Node):
    def __init__(self, func, in_edge):
        self.func = func
        self.in_edge = in_edge
        self.out_edge = Edge()

    @gen.coroutine
    def run(self):
        self.out_edge.data = map(self.func, self.in_edge.data)
        yield

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
