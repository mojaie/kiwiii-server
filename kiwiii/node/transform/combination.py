#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import Node


class Combination(Node):
    def __init__(self, in_edge, r=2):
        super().__init__(in_edge)
        self.r = r

    def on_submitted(self):
        comb = itertools.combinations(self.in_edge.records, self.r)
        main, counter = itertools.tee(comb)
        self.out_edge.records = main
        self.out_edge.task_count = len(list(counter))
        self.out_edge.fields.merge(self.in_edge.fields)
