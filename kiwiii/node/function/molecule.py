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
from kiwiii.core.node import AsyncNode
from kiwiii.node.function.apply import Apply


def chem_data(chem_calcs, pickle_mol, row):
    mol = Compound(json.loads(row["_molobj"]))
    rcd = {k: v(mol) for k, v in chem_calcs.items()}
    rcd.update(row)
    if pickle_mol:
        rcd["_molobj"] = pickle.dumps(json.loads(row["_molobj"]), protocol=4)
    return rcd


class Molecule(Apply):
    def __init__(self, in_edge, chem_calcs=None, pickle_mol=False,
                 fields=None, params=None):
        super().__init__(in_edge, None, fields=fields, params=params)
        if fields is None:
            self.fields.merge(static.CHEM_FIELDS)
        if chem_calcs is None:
            chem_calcs = static.CHEM_FUNCTIONS
        self.func = functools.partial(chem_data, chem_calcs, pickle_mol)


class AsyncMolecule(AsyncNode):
    def __init__(self, in_edge, chem_calcs=None, pickle_mol=False,
                 fields=None, params=None):
        super().__init__(in_edge)
        if fields is None:
            self.fields.merge(static.CHEM_FIELDS)
        else:
            self.fields.merge(fields)
        if chem_calcs is None:
            chem_calcs = static.CHEM_FUNCTIONS
        self.func = functools.partial(chem_data, chem_calcs, pickle_mol)

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            out = self.func(in_)
            self.out_edge.done_count = self.in_edge.done_count
            yield self.out_edge.put(out)
