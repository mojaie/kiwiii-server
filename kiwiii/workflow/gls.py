#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import mcsdr
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.chemdata import AsyncChemData, STRUCT_FIELD
from kiwiii.node.filter import MPFilter
from kiwiii.node.jsonresponse import AsyncJSONResponse
from kiwiii.node.numbergenerator import AsyncNumberGenerator, INDEX_FIELD
from kiwiii.node.sqliteio import SQLiteInput
from kiwiii.sqlitehelper import SQLITE_HELPER as sq


def mcsdr_filter(qmolarr, params, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    type_ = {"sim": "local_sim", "edge": "mcsdr_edges"}
    if len(mol) > params["molSizeCutoff"]:  # mol size filter
        return
    try:
        arr = mcsdr.comparison_array(
            mol, params["diameter"], params["maxTreeSize"])
    except ValueError:
        return
    sm, bg = sorted([arr[1], qmolarr[1]])
    thld = float(params["threshold"])
    if params["measure"] == "sim" and sm < bg * thld:
        return  # threshold filter
    if params["measure"] == "edge" and sm < thld:
        return  # fragment size filter
    res = mcsdr.local_sim(arr, qmolarr)
    if res[type_[params["measure"]]] >= thld:
        row["_local_sim"] = res["local_sim"]
        row["_mcsdr"] = res["mcsdr_edges"]
        return row


class GLS(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(sq.resource_fields(query["targets"]))
        self.fields.extend([
            {"key": "_mcsdr", "name": "MCS-DR size", "sortType": "numeric"},
            {"key": "_local_sim", "name": "GLS", "sortType": "numeric"}
        ])
        qmol = sq.query_mol(query["queryMol"])
        qmolarr = mcsdr.comparison_array(
            qmol, query["params"]["diameter"], query["params"]["maxTreeSize"])
        func = functools.partial(mcsdr_filter, qmolarr, query["params"])
        e1, = self.add_node(SQLiteInput(query))
        e2, = self.add_node(MPFilter(func, e1))
        e3, = self.add_node(AsyncChemData(e2))
        e4, = self.add_node(AsyncNumberGenerator(e3))
        self.add_node(AsyncJSONResponse(e4, self))
