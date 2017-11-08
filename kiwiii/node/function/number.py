#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from tornado import gen
from kiwiii.core.node import Node, AsyncNode


def number(name, zipped):
    row, count = zipped
    new_row = {name: count}
    new_row.update(row)
    return new_row


class Number(Node):
    def __init__(self, in_edge, name="_index", counter=itertools.count,
                 field=None, params=None):
        super().__init__(in_edge)
        self.counter = counter
        if field is None:
            field = {"key": name}
        self.fields.merge([field])
        self.func = functools.partial(number, name)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        zipped = zip(self.in_edge.records, self.counter())
        self.out_edge.records = map(self.func, zipped)
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)
        self.out_edge.params.update(self.params)


class AsyncNumber(AsyncNode):
    def __init__(self, in_edge, name="_index", counter=itertools.count,
                 field=None, params=None):
        super().__init__(in_edge)
        self.counter = counter
        if field is None:
            field = {"key": name}
        self.fields.merge([field])
        self.func = functools.partial(number, name)
        if params is not None:
            self.params.update(params)

    @gen.coroutine
    def _get_loop(self):
        cnt = self.counter()
        while 1:
            in_ = yield self.in_edge.get()
            yield self.out_edge.put(self.func((in_, next(cnt))))
            self.out_edge.done_count = self.in_edge.done_count
