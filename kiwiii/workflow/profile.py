#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii import sqlitehelper
from kiwiii.core.workflow import Workflow
from kiwiii.node.io import sqlite
from kiwiii.node.function.apply import Apply
from kiwiii.node.function.number import Number
from kiwiii.node.io.json import JSONResponse
from kiwiii.node.record.merge import MergeRecords
from kiwiii.node.transform.stack import Stack
from kiwiii.lod import LOD


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["_field"]])
    del row["key"]
    return row


class Profile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1s = []
        ac = LOD(sqlitehelper.SQLITE_RESOURCES.filter("domain", "activity"))
        fs = LOD()
        for vs in ac.values("fields"):
            fs.merge(vs)
        fields_dict = {f["key"]: f for f in fs}
        sq = {
            "type": "filter",
            "targets": list(ac.values("id")),
            "key": "id", "operator": "eq", "values": (query["id"],)
        }
        e1, = self.add_node(sqlite.SQLiteFilterInput(sq))
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
        e3, = self.add_node(Stack(('id',), e2))
        func = functools.partial(add_rsrc_fields, fields_dict)
        e4, = self.add_node(Apply(func, e3))
        e5, = self.add_node(Number(e4))
        self.add_node(JSONResponse(e5, self))
