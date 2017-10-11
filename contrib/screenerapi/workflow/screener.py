#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii.core.workflow import Workflow
from kiwiii.node.basicio import IteratorInput
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.node.jsonresponse import JSONResponse

from contrib.screenerapi import request


class CompoundProfile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        results = request.compound(
            query["compoundID"], auth_header)
        e1, = self.add_node(IteratorInput(results))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))


class PlateValues(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        results = request.plate_values(
            query["QCSRefID"], query["layerIndex"], auth_header)
        e1, = self.add_node(IteratorInput(results))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))


class PlateStats(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        results = request.plate_stats(
            query["QCSRefID"], query["layerIndices"], auth_header)
        e1, = self.add_node(IteratorInput(results))
        e2, = self.add_node(NumberGenerator(e1))
        self.add_node(JSONResponse(e2, self))
