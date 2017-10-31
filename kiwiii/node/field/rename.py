#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.core.node import Node


def rename(mapping, row):
    for old, new in mapping.items():
        row[new] = row[old]
        del row[old]
    return row


class RenameFields(Node):
    def __init__(self, in_edge, conv_fields):
        super().__init__(in_edge)
        self.conv = conv_fields
        conv_keys = {}
        for old_key, new_dict in conv_fields.items():
            if old_key != new_dict["key"]:
                conv_keys[old_key] = new_dict["key"]
        if conv_keys:
            self.func = functools.partial(rename, conv_keys)

    def on_submitted(self):
        if hasattr(self, "func"):
            self.out_edge.records = map(self.func, self.in_edge.records)
        else:
            self.out_edge.records = self.in_edge.records
        self.out_edge.task_count = self.in_edge.task_count
        for f in self.in_edge.fields:
            if f["key"] in self.conv.keys():
                self.out_edge.fields.append(self.conv[f["key"]])
            else:
                self.out_edge.fields.append(f)
        self.out_edge.fields.extend(self.fields)
