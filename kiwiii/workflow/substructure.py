#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import substructure
from chorus import molutil
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.function.molecule import AsyncMolecule, STRUCT_FIELD
from kiwiii.node.record.filter import MPFilterRecords
from kiwiii.node.io.json import AsyncJSONResponse
from kiwiii.node.function.number import AsyncNumber, INDEX_FIELD
from kiwiii.node.io import sqlite
from kiwiii.sqlitehelper import SQLITE_HELPER as sq


def exact_filter(qmol, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    if substructure.equal(mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def substr_filter(qmol, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    if substructure.substructure(
            mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


def supstr_filter(qmol, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    if substructure.substructure(
            qmol, mol, ignore_hydrogen=params["ignoreHs"]):
        return row


class ExactStruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        qmol = sq.query_mol(query["queryMol"])
        mw_filter = {
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "values": (molutil.mw(qmol),),
        }
        func = functools.partial(exact_filter, qmol, query["params"])
        e1, = self.add_node(sqlite.SQLiteFilterInput(mw_filter))
        e2, = self.add_node(MPFilterRecords(func, e1))
        e3, = self.add_node(AsyncMolecule(e2))
        e4, = self.add_node(AsyncNumber(e3))
        self.add_node(AsyncJSONResponse(e4, self))


class Substruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(substr_filter, qmol, query["params"])
        e1, = self.add_node(sqlite.SQLiteInput(query))
        e2, = self.add_node(MPFilterRecords(func, e1))
        e3, = self.add_node(AsyncMolecule(e2))
        e4, = self.add_node(AsyncNumber(e3))
        self.add_node(AsyncJSONResponse(e4, self))


class Superstruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(supstr_filter, qmol, query["params"])
        e1, = self.add_node(sqlite.SQLiteInput(query))
        e2, = self.add_node(MPFilterRecords(func, e1))
        e3, = self.add_node(AsyncMolecule(e2))
        e4, = self.add_node(AsyncNumber(e3))
        self.add_node(AsyncJSONResponse(e4, self))
