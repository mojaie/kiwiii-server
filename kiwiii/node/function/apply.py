#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.node import Node


class Apply(Node):
    def __init__(self, in_edge, func, fields=None, params=None):
        super().__init__(in_edge)
        self.func = func
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self.out_edge.records = map(self.func, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)
        self.out_edge.params.update(self.params)
