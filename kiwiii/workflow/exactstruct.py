#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle
from chorus.model.graphmol import Compound
from chorus import substructure
from chorus import molutil
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.mpfilter import MPFilter
from kiwiii.node.numbergenerator import NumberGenerator
from kiwiii.node.jsonresponse import JSONResponse


def exact_filter(qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    if substructure.exact(mol, qmol):
        record[molobj["key"]] = json.dumps(mol.jsonized())
        record.update(row)
        del record[molobj["key"]]
        return record


class ExactStruct(TaskTree):
    def __init__(self, query):
        super().__init__()
        mw_filter = {
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "value": molutil.mw(query["mol"])
        }
        e1, = self.add_node(SQLiteQuery(mw_filter))
        e2, = self.add_node(MPFilter(exact_filter, e1))
        e3, = self.add_node(NumberGenerator(e2))
        self.response = self.add_node(JSONResponse(e3))
