#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
import json
import pickle
from chorus.model.graphmol import Compound
from kiwiii import sqliteconnection as sqlite
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree


def chem_records(qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[molobj["key"]]))
    record[molobj["key"]] = json.dumps(mol.jsonized())
    record.update(row)
    del record[molobj["key"]]
    return record


def reindex(row, count):
    result = {"_index": count}
    result.update(row)
    return result


class DBSearch(TaskTree):
    def __init__(self, query):
        super().__init__()
        source = sqlite.chem_first_match(query)
        t1 = self.put_task(chem_records, args=source)
        t2 = self.put_task(reindex, args=itertools.count, parents=(t1,))
        self.output_id = t2

    def result(self):
        return self.tasks[self.output_id]["output"]
