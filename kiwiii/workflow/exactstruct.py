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

from kiwiii import sqlitehelper as helper
from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.chemdata import AsyncChemData, STRUCT_FIELD
from kiwiii.node.filter import MPFilter
from kiwiii.node.jsonresponse import AsyncJSONResponse
from kiwiii.node.numbergenerator import AsyncNumberGenerator, INDEX_FIELD
from kiwiii.node.sqlitequery import SQLiteQuery, resource_fields


def exact_filter(qmol, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    if substructure.equal(mol, qmol, ignore_hydrogen=params["ignoreHs"]):
        return row


class ExactStruct(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(resource_fields(query["tables"]))
        qmol = helper.query_mol(query["queryMol"])
        mw_filter = {
            "tables": query["tables"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "values": molutil.mw(qmol)
        }
        func = functools.partial(exact_filter, qmol, query["params"])
        e1, = self.add_node(SQLiteQuery("filter", mw_filter))
        e2, = self.add_node(MPFilter(func, e1))
        e3, = self.add_node(AsyncChemData(e2))
        e4, = self.add_node(AsyncNumberGenerator(e3))
        self.add_node(AsyncJSONResponse(e4, self))
