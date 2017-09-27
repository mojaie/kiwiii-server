#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
from chorus.model.graphmol import Compound
from tornado import gen

from kiwiii import static
from kiwiii.task import Node, AsyncNode, Edge, AsyncQueueEdge


def chem_data(row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    record = {k: v(mol) for k, v in static.CHEM_FUNCTIONS.items()}
    record.update(row)
    return record


class ChemData(Node):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.out_edge.records = map(chem_data, self.in_edge.records)
        self.on_finish()

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)


class AsyncChemData(AsyncNode):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def _dispatch(self):
        while 1:
            in_ = yield self.in_edge.get()
            out = chem_data(in_)
            yield self.out_edge.put(out)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
