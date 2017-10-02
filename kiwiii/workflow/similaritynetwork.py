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


def gls_matrix_filter(param, pair):
    mol1, arr2 = pair
    result = func(arr1[1], arr2[1])
    if result[param] >= thld:
        row["source"] = arr1[0]
        row["target"] = arr2[0]
        row["weight"] = result[param]
        if "canceled" in result:
            row["timeout"] = result["canceled"]
        return True
    return False


def chemobj(rcd):
    return Compound(json.loads(rcd[static.MOLOBJ_KEY]))


class GLSSimilarityNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"}
        ]
        diam = int(params["diameter"])
        tree = int(params["maxTreeSize"])
        comparr = functools.partial(mcsdr.comparison_array, diam, tree)
        filter_ = functools.partial(gls_matrix_filter, params)
        e1, = self.add_node(ListInput(contents["records"]))
        e2, = self.add_node(Apply(chemobj, e1))
        e3, = self.add_node(Apply(comparr, e2))
        e4, = self.add_node(MatrixSupplier(e3))
        e5, = self.add_node(MPFilter(filter_, e4))
        self.add_node(AsyncGraphOutput(e5, self))
