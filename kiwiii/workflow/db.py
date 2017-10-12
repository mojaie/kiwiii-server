#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.workflow import Workflow
from kiwiii.node import sqliteio
from kiwiii.node.chemdata import ChemData, STRUCT_FIELD
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.node.jsonresponse import JSONResponse
from kiwiii.sqlitehelper import SQLITE_HELPER as sq


class DBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        e1, = self.add_node(sqliteio.SQLiteFilterInput(query))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))


class DBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        e1, = self.add_node(sqliteio.SQLiteSearchInput(query))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))


class ChemDBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        e1, = self.add_node(sqliteio.SQLiteFilterInput(query))
        e2, = self.add_node(ChemData(e1))
        e3, = self.add_node(NumberGenerator(e2))
        self.add_node(JSONResponse(e3, self))


class ChemDBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        e1, = self.add_node(sqliteio.SQLiteSearchInput(query))
        e2, = self.add_node(ChemData(e1))
        e3, = self.add_node(NumberGenerator(e2))
        self.add_node(JSONResponse(e3, self))
