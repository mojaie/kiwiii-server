#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import static

from kiwiii.core.workflow import Workflow
from kiwiii.node.sdfileio import SDFileInput
from kiwiii.node.chemdata import ChemData, STRUCT_FIELD
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.node.jsonresponse import JSONResponse


class SDFParser(Workflow):
    def __init__(self, contents, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD, STRUCT_FIELD]
        self.fields.extend(static.CHEM_FIELDS)
        e1, = self.add_node(SDFileInput(contents, query["params"]))
        e2, = self.add_node(ChemData(e1))
        e3, = self.add_node(NumberGenerator(e2))
        self.add_node(JSONResponse(e3, self))
