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
from kiwiii import sqliteconnection as sqlite
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree


def exact_filter(qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    if substructure.exact(mol, qmol):
        record[molobj["key"]] = json.dumps(mol.jsonized())
        record.update(row)
        del record[molobj["key"]]
        return record


def reindex(row, count):
    result = {"_index": count}
    result.update(row)
    return result


class ExactStruct(TaskTree):
    def __init__(self, query):
        super().__init__()
        # MW filter
        source = sqlite.chem_find_all({
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "value": molutil.mw(query["mol"])
        })
        n1 = Filter(exact_filter, args=source, mp=query["mp"])
        e1 = self.add_node(n1)
        n2 = NumberGenerator(pred=e1)
        e2 = self.add_node(n2)
        self.output = JsonStore(pred=e2)

    def result(self):
        return self.tasks[self.output_id]["output"]
