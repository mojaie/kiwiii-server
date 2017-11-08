#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import Node


def unstack(row, keys):
    for nk in set(row.keys()) - set(keys):
        d = {k: v for k, v in row.items() if k in keys}
        d["_field"] = nk
        d["_value"] = row[nk]
        yield d


# TODO: GroupBy equivalent?
class Unstack(Node):
    def __init__(self, in_edge, field_key="_field", value_key="_value",
                 params=None):
        super().__init__(in_edge)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        stacked = itertools.chain.from_iterable(
            unstack(rcd, self.keys) for rcd in self.in_edge.records)
        main, counter = itertools.tee(stacked)
        self.out_edge.records = main
        self.out_edge.task_count = sum(1 for i in counter)
        fs = filter(lambda x: x["key"] in self.keys, self.in_edge.fields)
        self.out_edge.fields.merge(fs)
        self.out_edge.fields.merge([{"key": "_field"}, {"key": "_value"}])
        self.out_edge.params.update(self.in_edge.params)
        self.out_edge.params.update(self.params)
