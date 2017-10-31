#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.core.node import Node, AsyncNode

INDEX_FIELD = {"key": "_index", "name": "Index", "sortType": "numeric"}


class Number(Node):
    def __init__(self, in_edge, field=None):
        super().__init__(in_edge)
        if field is None:
            self.fields = [INDEX_FIELD]
        else:
            self.fields = [field]

    def run(self):
        cnt = itertools.count()
        for in_ in self.in_edge.records:
            out = {self.fields[0]["key"]: next(cnt)}
            out.update(in_)
            self.out_edge.records.append(out)
        self.on_finish()


class AsyncNumber(AsyncNode):
    def __init__(self, in_edge, field=None):
        super().__init__(in_edge)
        if field is None:
            self.fields = [INDEX_FIELD]
        else:
            self.fields = [field]

    @gen.coroutine
    def _get_loop(self):
        cnt = itertools.count()
        while 1:
            in_ = yield self.in_edge.get()
            out = {self.fields[0]["key"]: next(cnt)}
            out.update(in_)
            yield self.out_edge.put(out)
            self.out_edge.done_count = self.in_edge.done_count
