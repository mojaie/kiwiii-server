#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from kiwiii.core.node import Node
from kiwiii.util import lod


def groupby(key, rows, row):
    found = lod.find(key, row[key], rows)
    if found:
        found.update(row)
        return rows
    else:
        rows.append(row)
        return rows


class GroupBy(Node):
    def __init__(self, key, in_edge):
        super().__init__(in_edge)
        self.key = key

    def on_submitted(self):
        func = functools.partial(groupby, self.key)
        grouped = functools.reduce(func, self.in_edge.records, [])
        main, counter = itertools.tee(grouped)
        self.out_edge.records = main
        self.out_edge.task_count = len(list(counter))
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.params.update(self.in_edge.params)
