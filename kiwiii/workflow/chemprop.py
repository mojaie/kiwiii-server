#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
import functools

from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.chemdata import AsyncChemData
from kiwiii.node.filter import AsyncFilter
from kiwiii.node.numbergenerator import AsyncNumberGenerator
from kiwiii.node.jsonresponse import AsyncJSONResponse


def prop_filter(func, qmol, row):
    if func(row[molobj["key"]], qmol):
        return row


class ChemProp(TaskTree):
    def __init__(self, query):
        super().__init__()
        query.update({"type": "all"})
        func = functools.partial(prop_filter, query["func"], query["qmol"])
        e1, = self.add_node(SQLiteQuery(query))
        e2, = self.add_node(AsyncFilter(func, e1))
        e3, = self.add_node(AsyncChemData(e2))
        e4, = self.add_node(AsyncNumberGenerator(e3))
        res = AsyncJSONResponse(e4)
        self.response = res.response
        self.add_node(res)
