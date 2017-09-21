#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
from chorus.model.graphmol import Compound
from tornado import gen

from kiwiii import static
from kiwiii.node.node import Node, Edge, AsyncQueueEdge


def chem_data(row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    record = {k: v(mol) for k, v in static.CHEM_FUNCTIONS.items()}
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
    def __init__(self, in_edge):
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
