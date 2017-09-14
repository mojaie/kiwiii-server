#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
import json
import pickle
from chorus.model.graphmol import Compound
from chorus import substructure
from kiwiii import sqliteconnection as sqlite
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree


def substr_filter(qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    if substructure.substructure(mol, qmol):
        record[molobj["key"]] = json.dumps(mol.jsonized())
        record.update(row)
        del record[molobj["key"]]
        return record


def reindex(row, count):
    result = {"_index": count}
    result.update(row)
    return result


class SubstructFilter(TaskTree):
    def __init__(self, query):
        super().__init__()
        source = sqlite.records_iter(query)
        t1 = self.put_task(substr_filter, args=source, mp=query["mp"])
        t2 = self.put_task(reindex, args=itertools.count, parents=(t1,))
        self.output_id = t2

    def result(self):
        return self.tasks[self.output_id]["output"]
