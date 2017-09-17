#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import uuid

from tornado import gen
from kiwiii.node.node import Node, Edge


class JSONResponse(Node):
    def __init__(self, in_edge, params=None):
        self.in_edge = in_edge
        self.out_edge = Edge()
        self.params = params
        id_ = str(uuid.uuid4())
        ts = time.time()
        self.out_edge.data = {
            "id": id_,
            "name": id_[:8],
            "columns": [],
            "records": [],
            "status": "Completed"
        }

    @gen.coroutine
    def run(self):
        while self.in_edge.status != "done":
            in_record = yield self.in_edge.get()
            yield self.out_edge.data["records"].append(in_record)

    def in_edges(self):
        return (self.in_edge,)

    def out_edges(self):
        return (self.out_edge,)
