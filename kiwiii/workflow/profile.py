#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

from kiwiii import sqlitehelper
from kiwiii.core.workflow import Workflow
from kiwiii.node.io import sqlite
from kiwiii.node.function.number import Number
from kiwiii.node.io.json import JSONResponse
from kiwiii.node.record.merge import MergeRecords


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["_field"]])
    del row["key"]
    return row


class Profile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        e1s = []
        targets = list(sqlitehelper.SQLITE_RESOURCES.filter(
            "domain", "activity").values("id"))
        sq = {
            "type": "filter",
            "targets": targets,
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
        e3, = self.add_node(Number(e2))
        self.add_node(JSONResponse(e3, self))
