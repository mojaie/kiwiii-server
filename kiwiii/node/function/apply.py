#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.node import SyncNode


class Apply(SyncNode):
    def __init__(self, func, fields=None, params=None):
        super().__init__()
        self.func = func
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        super().on_submitted()
        self._out_edge.records = map(self.func, self._in_edge.records)
