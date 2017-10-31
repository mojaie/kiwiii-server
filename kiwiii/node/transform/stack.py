#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import Node


def stack(row, keys):
    for nk in set(row.keys()) - set(keys):
        d = {k: v for k, v in row.items() if k in keys}
        d["field"] = nk
        d["value"] = row[nk]
        yield d


class Stack(Node):
    def __init__(self, keys, in_edge):
        super().__init__(in_edge)
        self.keys = keys

    def on_submitted(self):
        stacked = itertools.chain.from_iterable(
            stack(rcd, self.keys) for rcd in self.in_edge.records)
        main, counter = itertools.tee(stacked)
        self.out_edge.records = main
        self.out_edge.task_count = len(list(counter))
