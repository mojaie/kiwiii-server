#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.workflow import Workflow
from kiwiii.node.function.molecule import Molecule
from kiwiii.node.function.number import Number
from kiwiii.node.io import sqlite
from kiwiii.node.io.json import JSONResponse


class DBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1, = self.add_node(sqlite.SQLiteFilterInput(query))
        e2, = self.add_node(Number(e1))
        self.add_node(JSONResponse(e2, self))


class DBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1, = self.add_node(sqlite.SQLiteSearchInput(query))
        e2, = self.add_node(Number(e1))
        self.add_node(JSONResponse(e2, self))


class ChemDBFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1, = self.add_node(sqlite.SQLiteFilterInput(query))
        e2, = self.add_node(Molecule(e1))
        e3, = self.add_node(Number(e2))
        self.add_node(JSONResponse(e3, self))


class ChemDBSearch(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1, = self.add_node(sqlite.SQLiteSearchInput(query))
        e2, = self.add_node(Molecule(e1))
        e3, = self.add_node(Number(e2))
        self.add_node(JSONResponse(e3, self))
