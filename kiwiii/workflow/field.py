#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools
import itertools

from kiwiii import sqlitehelper
from kiwiii.core.workflow import Workflow
from kiwiii.node import sqliteio
from kiwiii.node.apply import Apply
from kiwiii.node.jsonresponse import JSONResponse
from kiwiii.node.merge import Merge
from kiwiii.node.reshape import Stack
from kiwiii.node.numbergenerator import NumberGenerator, INDEX_FIELD
from kiwiii.util import lod


def add_rsrc_fields(fields_dict, row):
    row.update(fields_dict[row["field"]])
    del row["key"]
    return row


class Profile(Workflow):
    def __init__(self, query):
        super().__init__()
        self.query = query
        self.fields = [
            INDEX_FIELD,
            {"key": "id"},
            {"key": "field"},
            {"key": "value"},
        ]
        e1s = []
        ac = list(lod.filter_(
            "domain", "activity", sqlitehelper.SQLITE_RESOURCES))
        fs = lod.unique(
            itertools.chain.from_iterable(lod.values("fields", ac)))
        fields_dict = {f["key"]: f for f in fs}
        sq = {
            "type": "filter",
            "targets": list(lod.values("id", ac)),
            "key": "id", "operator": "eq", "values": (query["id"],)
        }
        e1, = self.add_node(sqliteio.SQLiteFilterInput(sq))
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
        e2, = self.add_node(Merge(e1s))
        e3, = self.add_node(Stack(('id',), e2))
        func = functools.partial(add_rsrc_fields, fields_dict)
        e4, = self.add_node(Apply(func, e3))
        e5, = self.add_node(NumberGenerator(e4))
        self.add_node(JSONResponse(e5, self))
