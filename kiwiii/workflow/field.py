#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqlitehelper
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
        es = []
        # SQLite
        targets = list(sqlitehelper.SQLITE_RESOURCES.filter(
            "domain", "activity").values("id"))
        q = {
            "type": "filter",
            "targets": targets,
            "key": "compoundID", "operator": "in", "values": query["values"]
        }
        e1, = self.add_node(sqlite.SQLiteFilterInput(q))
        e2, = self.add_node(
            FilterRecords(e1, "assayID", query["targetAssays"]))
        e3, = self.add_node(
            FilterRecords(e2, "field", query["targetFields"]))
        es.append(e3)
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
        es1, = self.add_node(MergeRecords(es))
        es2, = self.add_node(ConcatFields(
            es1, ("assayID", "field"), delimiter="_",
            field="field"))
        es3, = self.add_node(Unstack(
            "id", es2, field_key="field", value_key="value"))
        es4, = self.add_node(Number(es3))
        self.add_node(JSONResponse(es4, self))
