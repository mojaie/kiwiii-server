#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.task import Workflow
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.numbergenerator import NumberGenerator
from kiwiii.node.jsonresponse import JSONResponse


class DBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        e1, = self.add_node(SQLiteQuery("search", query))
        e2, = self.add_node(NumberGenerator(e1))
        res = JSONResponse(e2, self)
        self.response = res.response
        self.add_node(res)
