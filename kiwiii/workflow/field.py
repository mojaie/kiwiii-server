#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from kiwiii import sqlitehelper
from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.io import sqlite
from kiwiii.node.function.number import Number, INDEX_FIELD
from kiwiii.node.io.json import JSONResponse
from kiwiii.node.record.merge import MergeRecords
from kiwiii.node.transform.groupby import GroupBy
from kiwiii.util import lod


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["field"]])
    del row["key"]
    return row


class FieldFilter(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [INDEX_FIELD]
        rsrc_fields = list(itertools.chain.from_iterable(
            r["fields"] for r in static.RESOURCES))
        fs = [lod.find("key", i, rsrc_fields) for i in query["targetFields"]]
        self.fields.extend(fs)
        e1s = []
        # SQLite
        sq_rsrcs = list(lod.filter_(
            "domain", "activity", sqlitehelper.SQLITE_RESOURCES))
        q = {
            "type": "filter",
            "targets": list(lod.values("id", sq_rsrcs)),
            "key": "id", "operator": "in", "values": query["values"],
            "fields": ["id"] + query["targetFields"]
        }
        e1, = self.add_node(sqlite.SQLiteFilterInput(q))
        e1s.append(e1)
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
        e2, = self.add_node(MergeRecords(e1s))
        e3, = self.add_node(GroupBy("id", e2))
        e4, = self.add_node(Number(e3))
        self.add_node(JSONResponse(e4, self))
