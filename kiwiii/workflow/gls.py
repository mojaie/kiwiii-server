#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from chorus import substructure
from kiwiii.definition import molobj
from kiwiii.workflow.tasktree import TaskTree
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.filter import AsyncFilter
from kiwiii.node.numbergenerator import AsyncNumberGenerator
from kiwiii.node.jsonresponse import AsyncJSONResponse


def gls_filter(qmol, row):
    if substructure.substructure(row[molobj["key"]], qmol):
        return row


class GLS(TaskTree):
    def __init__(self, query):
        super().__init__()
        e1, = self.add_node(SQLiteQuery())
        e2, = self.add_node(AsyncFilter(gls_filter, e1))
        e3, = self.add_node(AsyncNumberGenerator(e2))
        res = AsyncJSONResponse(e3)
        self.response = res.response
        self.add_node(res)
