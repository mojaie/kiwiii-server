#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.core.node import SyncNode


def rename(updates, row):
    for old, new in updates.items():
        row[new] = row[old]
        del row[old]
    return row


class UpdateFields(SyncNode):
    def __init__(self, mapping, params=None):
        super().__init__()
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
        super().on_submitted()
        for k in self.key_updates.keys():
            self._out_edge.fields.delete("key", k)
        if hasattr(self, "func"):
            self._out_edge.records = map(self.func, self._in_edge.records)
        else:
            self._out_edge.records = self._in_edge.records
