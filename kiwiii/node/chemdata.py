#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle

from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound
from chorus import molutil
from chorus import wclogp
from tornado import gen

from kiwiii.definition import molobj
from kiwiii.node.node import Node, Edge, AsyncQueueEdge


def chem_data(row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    record[molobj["key"]] = json.dumps(mol.jsonized())
    record["_structure"] = SVG(mol).contents()
    record["_mw"] = molutil.mw(mol)
    record["_formula"] = molutil.formula(mol)
    record["_nonH"] = molutil.non_hydrogen_count(mol)
    record["_logp"] = wclogp.wclogp(mol)
    del row[molobj["key"]]
    record.update(row)
    return record


class ChemData(Node):
    def __init__(self, in_edge):
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.out_edge.records = map(chem_data, self.in_edge.records)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class AsyncChemData(Node):
    def __init__(self, func, in_edge):
        self.func = func
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        if isinstance(self.in_edge, AsyncQueueEdge):
            while self.in_edge.status != "done":
                in_record = yield self.in_edge.get()
                yield self.out_edge.put(chem_data(in_record))
        else:
            self.out_edge.records = map(chem_data, self.in_edge.records)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
