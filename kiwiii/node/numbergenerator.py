#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.core.node import Node, AsyncNode


class NumberGenerator(Node):
    def __init__(self, in_edge, name="_index"):
        super().__init__(in_edge)
        self.name = name

    def run(self):
        cnt = itertools.count()
        for in_ in self.in_edge.records:
            out = {self.name: next(cnt)}
            out.update(in_)
            self.out_edge.records.append(out)
        self.on_finish()

    def info(self, specs):
        specs["columns"].insert(0, {
            "key": "_index", "name": "Index", "sort": "numeric"
        })


class AsyncNumberGenerator(AsyncNode):
    def __init__(self, in_edge, name="_index"):
        super().__init__(in_edge)
        self.name = name

    @gen.coroutine
    def _get_loop(self):
        cnt = itertools.count()
        while 1:
            in_ = yield self.in_edge.get()
            out = {self.name: next(cnt)}
            out.update(in_)
            yield self.out_edge.put(out)

    def info(self, specs):
        specs["columns"].insert(0, {
            "key": "_index", "name": "Index", "sort": "numeric"
        })
