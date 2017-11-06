#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json

from chorus import v2000reader  # , v2000writer
from chorus import molutil
from chorus.draw import calc2dcoords

from kiwiii.core.node import Node


class SDFileInputBase(Node):
    def __init__(self, fields=None, implicit_hydrogen=False,
                 recalc_coords=False, params=None):
        super().__init__()
        self.implicit_hydrogen = implicit_hydrogen
        self.recalc_coords = recalc_coords
        if fields is not None:
            self.fields.merge(
                {"key": q, "name": q, "sortType": "text"} for q in fields)
        if params is not None:
            self.params.update(params)

    def records_iter(self):
        for mol in self.contents:
            row = {}
            if self.implicit_hydrogen:
                mol = molutil.make_Hs_implicit(mol)
            if self.recalc_coords:
                calc2dcoords.calc2dcoords(mol)
            row["_molobj"] = json.dumps(mol.jsonized())
            for p in self.fields:
                row[p["key"]] = mol.data.get(p["key"], "")
            yield row

    def on_submitted(self):
        self.out_edge.records = self.records_iter()
        self.out_edge.task_count = self.row_count
        self.out_edge.fields.merge(self.fields)
        self.out_edge.params.update(self.params)

    def in_edges(self):
        return tuple()


class SDFileInput(SDFileInputBase):
    def __init__(self, in_file, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_file(in_file)
        self.row_count = v2000reader.inspect_file(in_file)[1]


class SDFileLinesInput(SDFileInputBase):
    def __init__(self, lines, **kwargs):
        super().__init__(**kwargs)
        self.contents = v2000reader.mols_from_text(lines)
        self.row_count = v2000reader.inspect_text(lines)[1]
