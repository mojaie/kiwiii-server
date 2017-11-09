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

from kiwiii.core.workflow import Workflow
from kiwiii.node.function.filter import MPFilter
from kiwiii.node.function.molecule import AsyncMolecule
from kiwiii.node.function.number import AsyncNumber
from kiwiii.node.io.json import AsyncJSONResponse
from kiwiii.node.io.sqlite import SQLiteInput
from kiwiii.sqlitehelper import SQLITE_HELPER as sq


def rdmorgan_filter(qmol, params, row):
    mol = Compound(json.loads(row["_molobj"]))
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
        self.fields.add({
            "key": "_morgan_sim", "name": "Fingerprint similarity",
            "sortType": "numeric"})
        qmol = sq.query_mol(query["queryMol"])
        func = functools.partial(rdmorgan_filter, qmol, query["params"])
        sq_in = SQLiteInput(query)
        mpf = MPFilter(func)
        molecule = AsyncMolecule()
        number = AsyncNumber()
        response = AsyncJSONResponse(self)
        self.connect(sq_in, mpf)
        self.connect(mpf, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
