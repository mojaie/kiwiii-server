#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.workflow import Workflow
from kiwiii.node.sqlitequery import SQLiteQuery, resource_fields
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.node.jsonresponse import JSONResponse


class DBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        self.fields.extend(resource_fields(query["tables"]))
        e1, = self.add_node(SQLiteQuery("filter", query))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))


class DBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        self.fields.extend(resource_fields(query["tables"]))
        e1, = self.add_node(SQLiteQuery("search", query))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))
