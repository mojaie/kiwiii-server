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


def resource_fields(tables):
    results = []
    for tbl in tables:
        found = lod.find("table", tbl, SQLITE_RESOURCES)
        fields = found["fields"]
        results.extend(fields)
    return lod.unique(results)


def record_count(tables, filename):
    conn = sqlite.Connection(os.path.join(static.SQLITE_BASE_DIR, filename))
    count = 0
    for tbl in tables:
        count += conn.rows_count(tbl)
    return count


def records_iter(tables, filename):
    conn = sqlite.Connection(os.path.join(static.SQLITE_BASE_DIR, filename))
    for tbl in tables:
        for res in conn.rows_iter(tbl):
            row = dict(res)
            mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
            row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
            yield row


def first_match(tables, filename, key, values):
    conn = sqlite.Connection(os.path.join(static.SQLITE_BASE_DIR, filename))
    for val in values:
        for tbl in tables:
            key_exists = lod.find("key", key, resource_fields((tbl,)))
            if key_exists is None:
                continue
            res = conn.find_first(key, (val,), tbl)
            if res is not None:
                row = dict(res)
                if static.MOLOBJ_KEY in row:
                    mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                    row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
                yield row
                break
        else:
            if static.MOLOBJ_KEY in row:
                null_record = {key: val}
                null_record[static.MOLOBJ_KEY] = json.dumps(
                    molutil.null_molecule().jsonized())
                yield null_record
            else:
                yield {key: val}


def find_all(tables, filename, key, values, op):
    conn = sqlite.Connection(os.path.join(static.SQLITE_BASE_DIR, filename))
    op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
          "lk": "LIKE", "in": "IN"}[op]
    for tbl in tables:
        key_exists = lod.find("key", key, resource_fields((tbl,)))
        if key_exists is None:
            continue
        for res in conn.find_iter(key, values, tbl, op):
            row = dict(res)
            if static.MOLOBJ_KEY in row:
                mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
            yield row


def query_mol(query):
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
        res = next(first_match((query["table"],), query["resourceFile"],
                               "id", (query["value"],)))
        if res is None:
            raise ValueError()
        qmol = Compound(json.loads(res[static.MOLOBJ_KEY]))
    return qmol
