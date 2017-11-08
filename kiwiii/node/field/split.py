#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from kiwiii.node.function.apply import Apply


def split(old_key, new_fields, separator, fill, row):
    values = itertools.chain(
        row[old_key].split(separator), itertools.repeat(fill))
    new_row = {}
    for field in new_fields:
        new_row[field["key"]] = next(values)
    new_row.update(row)
    del new_row[old_key]
    return new_row


class SplitField(Apply):
    def __init__(self, in_edge, old_key, new_fields, separator, fill=None,
                 params=None):
        super().__init__(
            in_edge,
            functools.partial(split, old_key, new_fields, separator, fill),
            fields=new_fields, params=params
        )
        self.old_key = old_key

    def on_submitted(self):
        super().on_submitted()
        self.out_edge.fields.delete("key", self.old_key)
