#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import operator

from kiwiii.core.node import Node


class FilterRecords(Node):
    def __init__(self, in_edge, key, value, operator=operator.eq):
        super().__init__(in_edge)
        if fields is not None:
            self.fields.merge(fields)

    def on_submitted(self):
        self.out_edge.records = filter(self.func, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)
