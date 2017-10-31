#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import json

from chorus import mcsdr
from chorus import molutil
from chorus import rdkit
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.io.json import AsyncJSONResponse
from kiwiii.node.io.iterator import IteratorInput
from kiwiii.node.record.filter import MPFilterRecords
from kiwiii.node.transform.combination import Combination


def gls_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    sm, bg = sorted((row1["array"][1], row2["array"][1]))
    if sm < bg * thld:  # threshold filter
        return
    res = mcsdr.local_sim(row1["array"], row2["array"])
    if res["local_sim"] >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": res["local_sim"]
        }
        return row


def morgan_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    sim = rdkit.morgan_sim(row1["mol"], row2["mol"], radius=2)
    if sim >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": sim
        }
        return row


def fmcs_filter(params, pair):
    row1, row2 = pair
    thld = float(params["threshold"])
    res = rdkit.fmcs(row1["mol"], row2["mol"], timeout=params["timeout"])
    if res["similarity"] >= thld:
        row = {
            "source": row1["index"],
            "target": row2["index"],
            "weight": res["similarity"],
            "canceled": res["canceled"]
        }
        return row


def gls_array(params, rcd):
    mol = Compound(json.loads(rcd[static.MOLOBJ_KEY]))
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(mol)
    diam = int(params["diameter"])
    tree = int(params["maxTreeSize"])
    arr = mcsdr.comparison_array(mol, diam, tree)
    return {"index": rcd["_index"], "array": arr}


def rdkit_mol(params, rcd):
    mol = Compound(json.loads(rcd[static.MOLOBJ_KEY]))
    if params["ignoreHs"]:
        mol = molutil.make_Hs_implicit(mol)
    return {"index": rcd["_index"], "mol": mol}


class GLSNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"},
            {"key": "weight"}
        ]
        arrgen = functools.partial(gls_array, params)
        arrs = map(arrgen, contents["records"])
        filter_ = functools.partial(gls_filter, params)
        e1, = self.add_node(IteratorInput(arrs))
        e2, = self.add_node(Combination(e1))
        e3, = self.add_node(MPFilterRecords(filter_, e2))
        self.add_node(AsyncJSONResponse(e3, self))

    def on_submitted(self):
        super().on_submitted()
        self.response["nodesID"] = self.nodesid


class RDKitMorganNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"},
            {"key": "weight"}
        ]
        mols = map(functools.partial(rdkit_mol, params), contents["records"])
        filter_ = functools.partial(morgan_filter, params)
        e1, = self.add_node(IteratorInput(mols))
        e2, = self.add_node(Combination(e1))
        e3, = self.add_node(MPFilterRecords(filter_, e2))
        self.add_node(AsyncJSONResponse(e3, self))

    def on_submitted(self):
        super().on_submitted()
        self.response["nodesID"] = self.nodesid


class RDKitFMCSNetwork(Workflow):
    def __init__(self, contents, params):
        super().__init__()
        self.datatype = "edges"
        self.nodesid = contents["id"]
        self.query = params
        self.fields = [
            {"key": "source"},
            {"key": "target"},
            {"key": "weight"}
        ]
        mols = map(functools.partial(rdkit_mol, params), contents["records"])
        filter_ = functools.partial(fmcs_filter, params)
        e1, = self.add_node(IteratorInput(mols))
        e2, = self.add_node(Combination(e1))
        e3, = self.add_node(MPFilterRecords(filter_, e2))
        self.add_node(AsyncJSONResponse(e3, self))

    def on_submitted(self):
        super().on_submitted()
        self.response["nodesID"] = self.nodesid
