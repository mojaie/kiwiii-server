#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import lod
from kiwiii import sqlitehelper as helper
from kiwiii.core.workflow import Workflow
from kiwiii.node.io import sqlite
from kiwiii.node.field.concat import ConcatFields
from kiwiii.node.function.number import Number
from kiwiii.node.io.json import JSONResponse
from kiwiii.node.record.filter import FilterRecords
from kiwiii.node.record.merge import MergeRecords
from kiwiii.node.transform.unstack import Unstack


class FieldFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        # SQLite
        targets = lod.filtered(helper.SQLITE_RESOURCES, "domain", "activity")
        target_ids = lod.valuelist(targets, "id")
        q = {
            "type": "filter",
            "targets": target_ids,
            "key": "compoundID", "operator": "in", "values": query["values"]
        }
        sq_in = sqlite.SQLiteFilterInput(q)
        filter1 = FilterRecords("assayID", query["targetAssays"])
        filter2 = FilterRecords("field", query["targetFields"])
        self.connect(sq_in, filter1)
        self.connect(filter1, filter2)
        """
        if r["resourceType"] == "api":
            sq = {
                "type": "filter",
                "targets": resources,
                "resourceURL": r,
                "key": "id", "operator": "eq", "value": query[""]
            }
            e1, = self.add_node(httpio.HTTPResourceFilterInput(sq))
        """
        merge = MergeRecords()
        self.connect(filter2, merge)
        concat = ConcatFields(
            ("assayID", "field"), {"key": "_field"}, separator=":")
        unstack = Unstack("id", field_key="_field", value_key="value")
        number = Number()
        response = JSONResponse(self)
        self.connect(merge, concat)
        self.connect(concat, unstack)
        self.connect(unstack, number)
        self.connect(number, response)
