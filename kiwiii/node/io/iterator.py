#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
from kiwiii.core.node import Node


class IteratorInput(Node):
    def __init__(self, iter_input, fields=None):
        super().__init__()
        self.iter_input = iter_input
        self.fields = fields

    def on_submitted(self):
        main, counter = itertools.tee(self.iter_input)
        self.out_edge.records = main
        if self.fields:
            self.out_edge.fields.extend(self.fields)
        self.out_edge.task_count = len(list(counter))

    def in_edges(self):
        return tuple()
