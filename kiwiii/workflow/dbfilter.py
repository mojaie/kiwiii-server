#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import pickle
from chorus.model.graphmol import Compound
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.numbergenerator import NumberGenerator
from kiwiii.node.jsonresponse import JSONResponse


def chem_records(qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    record[molobj["key"]] = json.dumps(mol.jsonized())
    record.update(row)
    del record[molobj["key"]]
    return record


class DBFilter(TaskTree):
    def __init__(self, query):
        super().__init__()
        e1, = self.add_node(SQLiteQuery(query))
        e2, = self.add_node(NumberGenerator(e1))
        self.response, = self.add_node(JSONResponse(e2))
