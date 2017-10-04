#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.jsonresponse import JSONResponse
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.node import sqliteio

""" TODO: result """

result = {
    "chem": {
        "fields": [
            {"key": "key"},
            {"key": "value"}
        ],
        "records": [
            {"key": "id", "value": "DB00172"},
            {"key": "name", "value": "hoge"}
        ]
    },
    "aliases": {
        "fields": [
            {"key": "id"},
            {"key": "source"}
        ],
        "records": [
            {"id": "DB00172", "source": "DRUGBANKFDA"}
        ]
    },
    "assays": {
        "fields": [
            {"key": "field"},
            {"key": "source"},
            {"key": "tags"},
            {"key": "dataType"},
            {"key": "value"},
        ],
        "records": [
            {"field": "test1", "source": "assay.sqlite3",
             "tags": ["hoge", "fuga"], "dataType": "type"}
        ]
    }
}


class Profile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        self.fields.extend(sqliteio.resource_fields(query["tables"]))
        e1s = []
        for r in static.RESOURCES:
            if r["resourceType"] == "sqlite":
                sq = {
                    "type": "filter",
                    "tables": resources,
                    "resourceFile": r,
                    "key": "id", "operator": "eq", "value": query[""]
                }
                e1, = self.add_node(sqliteio.SQLiteFilterInput(sq))
            if r["resourceType"] == "api":
                sq = {
                    "type": "filter",
                    "tables": resources,
                    "resourceURL": r,
                    "key": "id", "operator": "eq", "value": query[""]
                }
                e1, = self.add_node(httpio.HTTPResourceFilterInput(sq))
            e1s.append(e1)
        e2, = self.add_node(Append(e1s))
        e3, = self.add_node(Splitter(, e2))
        e3, = self.add_node(Apply(formatter, e2))
        e4, = self.add_node(NumberGenerator(e3))
        self.add_node(JSONResponse(e4, self))
