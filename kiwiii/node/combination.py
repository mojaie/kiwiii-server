#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from itertools import combinations

from kiwiii.core.node import Node


class Combination(Node):
    def __init__(self, in_edge, r=2):
        super().__init__(in_edge)
        self.r = r

    def run(self):
        self.out_edge.records = combinations(self.in_edge.records, self.r)
        self.on_finish()
