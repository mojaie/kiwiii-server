#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii.core.node import Node

INDEX_FIELD = {"key": "_index", "name": "Index", "sortType": "numeric"}


class RenameFields(Node):
    def __init__(self, in_edge, mapping=None):
        super().__init__(in_edge)

    def run(self):
        cnt = itertools.count()
        for in_ in self.in_edge.records:
            out = {self.fields[0]["key"]: next(cnt)}
            out.update(in_)
            self.out_edge.records.append(out)
        self.on_finish()
