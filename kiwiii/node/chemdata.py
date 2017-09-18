#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle
from chorus.model.graphmol import Compound
from kiwiii.definition import molobj
from kiwiii.node.node import Node, Edge


def chem_data(row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    record[molobj["key"]] = json.dumps(mol.jsonized())
    record.update(row)
    del record[molobj["key"]]
    return record


class ChemData(Node):
    def __init__(self, in_edge):
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.out_edge.records = list(map(chem_data, self.in_edge.records))

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
