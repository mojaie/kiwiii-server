#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json

from chorus import v2000reader, v2000writer
from chorus import molutil
from chorus.draw import calc2dcoords

from kiwiii import static
from kiwiii.core.node import Node


def sdfile_records(contents, params):
    mols = v2000reader.mols_from_text(contents)
    for m in mols:
        row = {}
        if params["implh"]:
            m = molutil.make_Hs_implicit(m)
        if params["recalc"]:
            calc2dcoords.calc2dcoords(m)
        row[static.MOLOBJ_KEY] = json.dumps(m.jsonized())
        for p in params["fields"]:
            row[p] = m.data.get(p, "")
        yield row


class SDFileInput(Node):
    def __init__(self, contents, params):
        super().__init__()
        self.contents = contents
        self.params = params

    def on_submitted(self):
        self.out_edge.records = sdfile_records(self.contents, self.params)
        self.out_edge.task_count = v2000reader.inspect_text(self.contents)[1]

    def in_edges(self):
        return tuple()
