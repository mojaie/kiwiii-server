#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import SyncNode


def stack(row, keys):
    for nk in set(row.keys()) - set(keys):
        d = {k: v for k, v in row.items() if k in keys}
        d["_field"] = nk
        d["_value"] = row[nk]
        yield d


class Stack(SyncNode):
    def __init__(self, keys, params=None):
        super().__init__()
        self.keys = keys
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        stacked = itertools.chain.from_iterable(
            stack(rcd, self.keys) for rcd in self._in_edge.records)
        main, counter = itertools.tee(stacked)
        self._out_edge.records = main
        self._out_edge.task_count = sum(1 for i in counter)
        fs = filter(lambda x: x["key"] in self.keys, self._in_edge.fields)
        self._out_edge.fields.merge(fs)
        self._out_edge.fields.merge([{"key": "_field"}, {"key": "_value"}])
        self._out_edge.params.update(self._in_edge.params)
        self._out_edge.params.update(self.params)
