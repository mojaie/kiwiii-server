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


def resources_iter(targets):
    dbs = {}
    for rsrc in static.RESOURCES:
        if rsrc["entity"] not in targets:
            continue
        db, table_key = rsrc["entity"].split(':')
        if db in dbs:
            continue
        path = os.path.join(static.SQLITE_BASE_DIR, "{}.sqlite3".format(db))
        dbs[db] = sqlite.Connection(path)
        yield dbs[db], rsrc


def resource_fields(targets):
    fields = []
    for rsrc in static.RESOURCES:
        fields.extend(rsrc["columns"])
    return lod.unique(fields)


def record_count(targets):
    count = 0
    for conn, rsrc in resources_iter(targets):
        table_key = rsrc["entity"].split(':')[1]
        count += conn.rows_count((table_key,))
    return count


def records_iter(query):
    for conn, rsrc in resources_iter(query["targets"]):
        table_key = rsrc["entity"].split(':')[1]
        for res in conn.rows_iter((table_key,)):
            row = dict(res)
            mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
            row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
            yield row


def first_match(query):
    for val in query["values"]:
        is_chem = []
        for conn, rsrc in resources_iter(query["targets"]):
            if rsrc["domain"] == "chemical":
                is_chem.append(1)
            key_exists = lod.find("key", query["key"], rsrc["columns"])
            if key_exists is None:
                continue
            table_key = rsrc["entity"].split(':')[1]
            res = conn.find_first(query["key"], (val,), (table_key,))
            if res is not None:
                row = dict(res)
                if static.MOLOBJ_KEY in row:
                    mol = Compound(pickle.loads(row[static.MOLOBJ_KEY]))
                    row[static.MOLOBJ_KEY] = json.dumps(mol.jsonized())
                yield row
                break
        else:
            if sum(is_chem):
                null_record = {query["key"]: val}
                null_record[static.MOLOBJ_KEY] = json.dumps(
                    molutil.null_molecule().jsonized())
                yield null_record
            else:
                yield {query["key"]: val}


def find_all(query):
    op = {"eq": "=", "gt": ">", "lt": "<", "ge": ">=", "le": "<=",
          "lk": "LIKE", "in": "IN"}[query["operator"]]
    for conn, rsrc in resources_iter(query["targets"]):
        key_exists = lod.find("key", query["key"], rsrc["columns"])
        if key_exists is None:
            continue
        table_key = rsrc["entity"].split(':')[1]
        for res in conn.find_iter(query["key"], query["values"],
                                  (table_key,), op):
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
        res = next(first_match({
            "targets": (query["source"],),
            "key": "ID",
            "values": (query["value"],)
        }))
        if res is None:
            raise ValueError()
        qmol = Compound(json.loads(res[static.MOLOBJ_KEY]))
    return qmol
