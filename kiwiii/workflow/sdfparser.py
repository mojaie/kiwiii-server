#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.workflow import Workflow
from kiwiii.node.function.molecule import Molecule
from kiwiii.node.function.number import Number
from kiwiii.node.io.json import JSONResponse
from kiwiii.node.io.sdfile import SDFileInput


class SDFParser(Workflow):
    def __init__(self, contents, query):
        super().__init__()
        self.query = query
        self.fields.merge([
            {"key": q, "name": q, "sortType": "text"}
            for q in query["params"]["fields"]
        ])
        sdf_in = SDFileInput(contents, query["params"])
        molecule = Molecule()
        number = Number()
        response = JSONResponse(self)
        self.connect(sdf_in, molecule)
        self.connect(molecule, number)
        self.connect(number, response)
