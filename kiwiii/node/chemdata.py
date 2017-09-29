#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
from chorus.model.graphmol import Compound
from tornado import gen

from kiwiii import static
from kiwiii.core.node import Node, AsyncNode

FIELDS = [
    {"key": "_structure", "name": "Structure", "sort": "none"},
    {"key": "_mw", "name": "MW", "sort": "numeric"},
    {"key": "_formula", "name": "Formula", "sort": "text"},
    {"key": "_logp", "name": "WCLogP", "sort": "numeric"},
    {"key": "_nonH", "name": "Non-H atom count", "sort": "numeric"}
]


def chem_data(row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    record = {k: v(mol) for k, v in static.CHEM_FUNCTIONS.items()}
    record.update(row)
    return record


class ChemData(Node):
    def __init__(self, in_edge):
        super().__init__(in_edge)
        self.fields = FIELDS

    def run(self):
        self.out_edge.records = map(chem_data, self.in_edge.records)
        self.on_finish()


class AsyncChemData(AsyncNode):
    def __init__(self, in_edge):
        super().__init__(in_edge)
        self.fields = FIELDS

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            out = chem_data(in_)
            yield self.out_edge.put(out)
