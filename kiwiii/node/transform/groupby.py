#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from kiwiii.core.node import SyncNode
from kiwiii.util import lod


def groupby(key, rows, row):
    found = lod.find(key, row[key], rows)
    if found:
        found.update(row)
        return rows
    else:
        rows.append(row)
        return rows


class GroupBy(SyncNode):
    def __init__(self, key, params=None):
        super().__init__()
        self.key = key
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        func = functools.partial(groupby, self.key)
        grouped = functools.reduce(func, self._in_edge.records, [])
        main, counter = itertools.tee(grouped)
        self._out_edge.records = main
        self._out_edge.task_count = sum(1 for i in counter)
        self._out_edge.fields.merge(self._in_edge.fields)
        self._out_edge.fields.merge(self.fields)
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)
