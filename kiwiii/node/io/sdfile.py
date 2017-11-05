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


def sdfile_text_records(contents, params):
    mols = v2000reader.mols_from_text(contents)
    for m in mols:
        row = {}
        if params.get("implh", False):
            m = molutil.make_Hs_implicit(m)
        if params.get("recalc", False):
            calc2dcoords.calc2dcoords(m)
        row[static.MOLOBJ_KEY] = json.dumps(m.jsonized())
        for p in params.get("fields", []):
            row[p] = m.data.get(p, "")
        yield row


def sdfile_records(in_file, params):
    mols = v2000reader.mols_from_file(in_file)
    for m in mols:
        row = {}
        if params.get("implh", False):
            m = molutil.make_Hs_implicit(m)
        if params.get("recalc", False):
            calc2dcoords.calc2dcoords(m)
        row[static.MOLOBJ_KEY] = json.dumps(m.jsonized())
        for p in params.get("fields", []):
            row[p] = m.data.get(p, "")
        yield row


class SDFileLinesInput(Node):
    def __init__(self, contents, fields=None, params=None):
        super().__init__()
        self.contents = contents
        if fields is not None:
            self.fields.merge(
                {"key": q, "name": q, "sortType": "text"} for q in fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self.out_edge.records = sdfile_text_records(self.contents, self.params)
        self.out_edge.task_count = v2000reader.inspect_text(self.contents)[1]
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.in_edge.params)

    def in_edges(self):
        return tuple()


class SDFileInput(Node):
    def __init__(self, in_file, fields=None, params=None):
        super().__init__()
        self.in_file = in_file
        if fields is not None:
            self.fields.merge(
                {"key": q, "name": q, "sortType": "text"} for q in fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        self.out_edge.records = sdfile_records(self.in_file, self.params)
        self.out_edge.task_count = v2000reader.inspect_file(self.in_file)[1]
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)

    def in_edges(self):
        return tuple()
