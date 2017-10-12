#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json
import traceback

from chorus import rdkit
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.chemdata import AsyncChemData, STRUCT_FIELD
from kiwiii.node.filter import MPFilter
from kiwiii.node.jsonresponse import AsyncJSONResponse
from kiwiii.node.numbergenerator import AsyncNumberGenerator, INDEX_FIELD
from kiwiii.node.sqliteio import SQLiteInput
from kiwiii.sqlitehelper import SQLITE_HELPER as sq


def rdmorgan_filter(qmol, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    try:
        score = rdkit.morgan_sim(mol, qmol, 4)
    except:
        print(traceback.format_exc())
        return
    thld = float(params["threshold"])
    if score >= thld:
        row["_morgan_sim"] = score
        return row


class RDKitMorgan(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        self.fields.append({
            "key": "_morgan_sim", "name": "Fingerprint similarity",
            "sortType": "numeric"})
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdmorgan_filter, qmol, query["params"])
        e1, = self.add_node(SQLiteInput(query))
        e2, = self.add_node(MPFilter(func, e1))
        e3, = self.add_node(AsyncChemData(e2))
        e4, = self.add_node(AsyncNumberGenerator(e3))
        self.add_node(AsyncJSONResponse(e4, self))
