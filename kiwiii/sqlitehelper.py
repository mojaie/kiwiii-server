#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import os
import pickle

from chorus import molutil, smilessupplier, v2000reader
from chorus.draw import calc2dcoords
from chorus.model.graphmol import Compound

from kiwiii import sqliteconnection as sqlite
from kiwiii import static
from kiwiii.util import lod


SQLITE_RESOURCES = list(lod.filter_("resourceType", "sqlite",
                                    static.RESOURCES))


class SQLiteHelper(object):
    def __init__(self):
        self.dbs = {}

    def origins_iter(self, rsrc_ids):
        for r in rsrc_ids:
            found = lod.find("id", r, SQLITE_RESOURCES)
            self.dbs.setdefault(r, found["resourceFile"])
            conn = sqlite.Connection(
                os.path.join(static.SQLITE_BASE_DIR, self.dbs[r]))
            yield r, found["table"], conn

    def record_count(self, rsrc_ids):
        count = 0
        for _, tbl, conn in self.origins_iter(rsrc_ids):
            count += conn.rows_count(tbl)
        return count

    def resource_fields(self, rsrc_ids):
        results = []
        for r in rsrc_ids:
            found = lod.find("id", r, SQLITE_RESOURCES)
            results.extend(found["fields"])
        return lod.unique(results)

    def records_iter(self, rsrc_ids):
        for r, tbl, conn in self.origins_iter(rsrc_ids):
            for res in conn.rows_iter(tbl):
                row = dict(res)
                mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
                row["source"] = r
                yield row

    def search(self, rsrc_ids, key, value):
        for r, tbl, conn in self.origins_iter(rsrc_ids):
            key_exists = lod.find("key", key, self.resource_fields(rsrc_ids))
            if key_exists is None:
                continue
            res = conn.find_first(key, (value,), tbl)
            if res is not None:
                row = dict(res)
                if static.MOLOBJ_KEY in row:
                    mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                    row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
                    row["source"] = r
                yield row
                break
        else:
            if static.MOLOBJ_KEY in row:
                null_record = {key: value}
                null_record[static.MOLOBJ_KEY] = json.dumps(
                    molutil.null_molecule().jsonized())
                yield null_record
            else:
                yield {key: value}

    def find_all(self, rsrc_ids, key, values, op):
        op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
              "lk": "LIKE", "in": "IN"}[op]
        for r, tbl, conn in self.origins_iter(rsrc_ids):
            key_exists = lod.find("key", key, self.resource_fields(rsrc_ids))
            if key_exists is None:
                continue
            for res in conn.find_iter(key, values, tbl, op):
                row = dict(res)
                if static.MOLOBJ_KEY in row:
                    mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                    row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
                    row["source"] = r
                yield row

    def query_mol(self, query):
        if query["format"] == "smiles":
            try:
                qmol = smilessupplier.smiles_to_compound(query["value"])
                calc2dcoords.calc2dcoords(qmol)
            except (ValueError, StopIteration):
                raise TypeError()
        elif query["format"] == "molfile":
            try:
                qmol = v2000reader.mol_from_text(query["value"])
            except (ValueError, StopIteration):
                raise TypeError()
        elif query["format"] == "dbid":
            res = next(self.search((query["source"],), "id", query["value"]))
            if res is None:
                raise ValueError()
            qmol = Compound(json.loads(res[static.MOLOBJ_KEY]))
        return qmol


SQLITE_HELPER = SQLiteHelper()
