#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.node import Node


class Apply(Node):
    def __init__(self, func, in_edge):
        super().__init__(in_edge)
        self.func = func

    def run(self):
        self.out_edge.records = map(self.func, self.in_edge.records)
        self.on_finish()