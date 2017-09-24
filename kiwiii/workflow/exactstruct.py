#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
import json

from chorus import substructure
from chorus import molutil
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.task import Workflow
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.filter import AsyncFilter
from kiwiii.node.numbergenerator import AsyncNumberGenerator
from kiwiii.node.jsonresponse import AsyncJSONResponse


def exact_filter(qmol, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    if substructure.exact(mol, qmol):
        return row


class ExactStruct(Workflow):
    def __init__(self, query):
        super().__init__()
        mw_filter = {
            "targets": query["targets"],
            "key": "_mw_wo_sw",
            "operator": "eq",
            "value": molutil.mw(query["mol"])
        }
        e1, = self.add_node(SQLiteQuery("filter", mw_filter))
        e2, = self.add_node(AsyncFilter(exact_filter, e1))
        e3, = self.add_node(AsyncNumberGenerator(e2))
        res = AsyncJSONResponse(e3, self)
        self.response = res.response
        self.add_node(res)
