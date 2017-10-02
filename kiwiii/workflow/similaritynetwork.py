#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import mcsdr
from chorus import molutil
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.combination import Combination
from kiwiii.node.filter import MPFilter
from kiwiii.node.jsonresponse import AsyncJSONResponse
from kiwiii.node.simpleio import IteratorInput


def gls_filter(params, pair):
    (idx1, arr1), (idx2, arr2) = pair
    thld = int(params["threshold"])
    sm, bg = sorted([arr1[1], arr2[1]])
    if sm < bg * thld:  # threshold filter
        return False
    sim = mcsdr.local_sim(arr1, arr2)
    if sim >= thld:
        row = {
            "source": idx1,
            "target": idx2,
            "weight": sim
        }
        if "canceled" in result:
            row["timeout"] = result["canceled"]
        return row


def gls_array(rcd, params):
    mol = Compound(json.loads(rcd[static.MOLOBJ_KEY]))
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(mol)
    diam = int(params["diameter"])
    tree = int(params["maxTreeSize"])
    arr = mcsdr.comparison_array(mol, diam, tree)
    return {"_index": rcd["_index"], "array": arr}


class GLSSimilarityNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.format = "networkedges"
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"}
        ]
        arrs = map(gls_array, contents["records"])
        filter_ = functools.partial(gls_filter, params)
        e1, = self.add_node(IteratorInput(arrs))
        e2, = self.add_node(Combination(e1))
        e3, = self.add_node(MPFilter(filter_, e2))
        self.add_node(AsyncJSONResponse(e3, self))
