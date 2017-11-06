#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json
import pickle

from chorus.model.graphmol import Compound
from tornado import gen

from kiwiii import static
from kiwiii.core.node import Node, AsyncNode


def chem_data(chem_calcs, pickle_mol, row):
    mol = Compound(json.loads(row["_molobj"]))
    rcd = {k: v(mol) for k, v in chem_calcs.items()}
    rcd.update(row)
    if pickle_mol:
        rcd["_molobj"] = pickle.dumps(json.loads(row["_molobj"]), protocol=4)
    return rcd


class Molecule(Node):
    def __init__(self, in_edge, fields=None, chem_calcs=None,
                 pickle_mol=False):
        super().__init__(in_edge)
        if fields is None:
            self.fields.merge(static.CHEM_FIELDS)
        else:
            self.fields.merge(fields)
        if chem_calcs is None:
            chem_calcs = static.CHEM_FUNCTIONS
        self.func = functools.partial(chem_data, chem_calcs, pickle_mol)

    def on_submitted(self):
        self.out_edge.records = map(self.func, self.in_edge.records)
        self.out_edge.task_count = self.in_edge.task_count
        self.out_edge.fields.merge(self.in_edge.fields)
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)


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
