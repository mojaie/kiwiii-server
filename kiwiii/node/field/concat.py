#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.core.node import Node


def concat(old_keys, new_field, separator, row):
    row[new_field] = separator.join(row[f] for f in old_keys)
    for f in old_keys:
        del row[f]
    return row


class ConcatFields(Node):
    def __init__(self, in_edge, old_keys, new_field, separator="_"):
        super().__init__(in_edge)
        self.field = new_field
        self.old_keys = old_keys
        self.func = functools.partial(concat, old_keys, new_field, separator)

    def on_submitted(self):
        self.out_edge.records = map(self.func, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count
        for f in self.old_keys:
            self.out_edge.fields.remove(f)
        self.out_edge.fields.add(self.fields, dupkey="replace")
        self.out_edge.params.update(self.in_edge.params)
