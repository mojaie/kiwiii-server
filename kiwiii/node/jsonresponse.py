#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from tornado import gen
from kiwiii.node.node import Node


class JSONResponse(Node):
    def __init__(self, in_edge, params=None):
        self.in_edge = in_edge
        self.response = {
            "columns": [],
            "records": []
        }
        self.params = params

    @gen.coroutine
    def run(self):
        while self.in_edge.status != "done":
            in_record = yield self.in_edge.get()
            yield self.response["records"].append(in_record)
