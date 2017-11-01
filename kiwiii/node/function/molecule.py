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


def chem_data(row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    record = {k: v(mol) for k, v in static.CHEM_FUNCTIONS.items()}
    record.update(row)
    return record


class Molecule(Node):
    def __init__(self, in_edge):
        super().__init__(in_edge)
        self.fields = static.CHEM_FIELDS

    def on_submitted(self):
        self.out_edge.records = map(chem_data, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)


class AsyncMolecule(AsyncNode):
    def __init__(self, in_edge):
        super().__init__(in_edge)
        self.fields = static.CHEM_FIELDS

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            out = chem_data(in_)
            self.out_edge.done_count = self.in_edge.done_count
            yield self.out_edge.put(out)
