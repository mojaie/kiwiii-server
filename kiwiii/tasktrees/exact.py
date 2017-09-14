#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools
import json
import pickle
from chorus.chem.model import Compound
from kiwiii import sqliteconnection as sqlite
from kiwiii.definition import MOL
from kiwiii.tasktrees.tasktree import TaskTree


def substr_search(func, qmol, row):
    record = {}
    mol = Compound(pickle.loads(row[MOL.key]))
    if func(mol, qmol):
        record[MOL.key] = json.dumps(mol.jsonized())
        record.update(row)
        del record[MOL.key]
        return record


def reindex(row, count):
    result = {"_index": count}
    result.update(row)
    return result


class ExactStructSearch(TaskTree):
    def __init__(self, query):
        super().__init__()
        source = sqlite.chem_find_all({query["targets"], "eq", "_mw_wo_sw"})
        t1 = self.put_task(substr_search, args=source, mp=query["mp"])
        t2 = self.put_task(reindex, args=itertools.count, parents=(t1,))
        self.output_id = t2

    def result(self):
        return self.tasks[self.output_id]["output"]
