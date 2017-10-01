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
from kiwiii.node.filter import MPFilter
from kiwiii.node.simpleio import ListInput


def gls_filter(func, param, row):
    arr1, arr2 = row
    result = func(arr1[1], arr2[1])
    if result[param] >= thld:
        row["source"] = arr1[0]
        row["target"] = arr2[0]
        row["weight"] = result[param]
        if "canceled" in result:
            row["timeout"] = result["canceled"]
        return True
    return False


class SimilarityNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"}
        ]
        mols = (Compound(json.loads(rcd[static.MOLOBJ_KEY]))
                for rcd in contents["records"])
        diam = int(params["diameter"])
        tree = int(params["maxTreeSize"])
        if params["measure"] == "gls":
            mols = (mcsdr.comparison_array(m, diam, tree) for m in mols)
        func = {
            "gls": functools.partial(mcsdr_filter, qmolarr, params)
        }[params["measure"]]
        e1, = self.add_node(ListInput(mols))
        e2, = self.add_node(MatrixSupplier(e1))
        e3, = self.add_node(MPFilter(func, e2))
        self.add_node(AsyncNetworkEdgesOutput(e3, self))
