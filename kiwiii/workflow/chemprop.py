#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#
import functools
import json
import operator
import re

from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.core.workflow import Workflow
from kiwiii.node.sqlitequery import SQLiteQuery
from kiwiii.node.chemdata import AsyncChemData
from kiwiii.node.filter import MPFilter
from kiwiii.node.numbergenerator import AsyncNumberGenerator
from kiwiii.node.jsonresponse import AsyncJSONResponse
from kiwiii.util import lod


def prop_filter(func, op, val, row):
    mol = Compound(json.loads(row[static.MOLOBJ_KEY]))
    try:
        valid = op(func(mol), val)
    except TypeError as e:
        print(e, row["ID"], val)
    else:
        if valid:
            return row


def like_operator(a, b):
    """ regexp implementation of sqlite LIKE operator """
    return re.match(b.replace("%", ".*?").replace("_", "[\w ]"), a) is not None


class ChemProp(Workflow):
    def __init__(self, query):
        super().__init__()
        sortfunc = {"numeric": float, "text": str}
        col = lod.find("key", query["key"], static.CHEM_COLUMNS)
        vs = [sortfunc[col["sort"]](v) for v in query["values"]]
        v = {True: vs, False: vs[0]}[query["operator"] == "in"]
        opfunc = {
            "eq": operator.eq, "gt": operator.gt, "lt": operator.lt,
            "ge": operator.ge, "le": operator.le,
            "lk": like_operator, "in": lambda a, b: a in b
        }[query["operator"]]
        func = functools.partial(
            prop_filter,
            static.CHEM_FUNCTIONS[query["key"]],
            opfunc, v
        )
        e1, = self.add_node(SQLiteQuery("all", query))
        e2, = self.add_node(MPFilter(func, e1))
        e3, = self.add_node(AsyncChemData(e2))
        e4, = self.add_node(AsyncNumberGenerator(e3))
        self.add_node(AsyncJSONResponse(e4, self))
