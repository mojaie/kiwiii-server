#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.core.node import Node


def rename(updates, row):
    for old, new in updates.items():
        row[new] = row[old]
        del row[old]
    return row


class UpdateFields(Node):
    def __init__(self, in_edge, mapping, params=None):
        super().__init__(in_edge)
        self.fields = mapping.values()
        self.key_updates = {}
        for old_key, field in mapping.items():
            if old_key != field["key"]:
                self.key_updates[old_key] = field["key"]
        if self.key_updates:
            self.func = functools.partial(rename, self.key_updates)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        if hasattr(self, "func"):
            self.out_edge.records = map(self.func, self.in_edge.records)
        else:
            self.out_edge.records = self.in_edge.records
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)
        self.out_edge.params.update(self.params)
        for k in self.key_updates.keys():
            self.out_edge.fields.delete("key", k)
