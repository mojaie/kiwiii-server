#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from chorus import substructure
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree
from kiwiii.node import sqlitehelper as helper
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.filter import AsyncFilter
from kiwiii.node.numbergenerator import AsyncNumberGenerator
from kiwiii.node.jsonresponse import AsyncJSONResponse


def substr_filter(qmol, row):
    if substructure.substructure(row[molobj["key"]], qmol):
        return row


def supstr_filter(qmol, row):
    if substructure.substructure(qmol, row[molobj["key"]]):
        return row


class Substructure(TaskTree):
    def __init__(self, query):
        super().__init__()
        func = {
            "substr": functools.partial(substr_filter,
                                        helper.query_mol(query)),
            "supstr": functools.partial(supstr_filter,
                                        helper.query_mol(query))
        }[query["type"]]
        e1, = self.add_node(SQLiteQuery("all", query))
        e2, = self.add_node(AsyncFilter(func, e1))
        e3, = self.add_node(AsyncNumberGenerator(e2))
        res = AsyncJSONResponse(e3)
        self.response = res.response
        self.add_node(res)
