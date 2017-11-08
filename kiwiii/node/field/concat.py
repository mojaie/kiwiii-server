#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.node.function.apply import Apply


def concat(old_keys, new_field, separator, row):
    row[new_field] = separator.join(row[f] for f in old_keys)
    for f in old_keys:
        del row[f]
    return row


class ConcatFields(Apply):
    def __init__(self, in_edge, old_keys, new_field, separator="_",
                 params=None):
        super().__init__(
            in_edge,
            functools.partial(concat, old_keys, new_field, separator),
            fields=[new_field], params=params
        )
        self.old_keys = old_keys

    def on_submitted(self):
        super().on_submitted()
        for k in self.old_keys:
            self.out_edge.fields.delete("key", k)
