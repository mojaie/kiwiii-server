
import glob
import os
import yaml

from kiwiii import sqliteconnection as sqlite
from kiwiii import static
from kiwiii.lod import ListOfDict


def resource_format(data):
    for rsrc in data["resources"]:
        rsrc["domain"] = data["domain"]
        rsrc["resourceType"] = data["type"]
        rsrc["resourceFile"] = data["file"]
        fields = ListOfDict(rsrc["fields"])
        if rsrc["domain"] == "chemical":
            fields.merge(static.CHEM_FIELDS)
            fields.delete("key", "_molobj")
        for field in fields:
            if "name" not in field:
                field["name"] = field["key"]
        rsrc["fields"] = fields


def screener_format(data):
    for rsrc in data["resources"]:
        rsrc["domain"] = data["domain"]
        rsrc["resourceType"] = data["type"]
        rsrc["resourceURL"] = data["url"]


def resources():
    resources = ListOfDict()
    # SQLite databases
    for filename in glob.glob(
            os.path.join(static.SQLITE_BASE_DIR, "*.sqlite3")):
        conn = sqlite.Connection(filename)
        doc = conn.document()
        resource_format(doc)
        resources.extend(doc["resources"])
    # API schema
    for filename in glob.glob(os.path.join(static.API_BASE_DIR, "*.yaml")):
        with open(filename) as f:
            doc = yaml.load(f.read())
            screener_format(doc)
            resources.extend(doc["resources"])
    return resources


def templates():
    templates = ListOfDict()
    for tm in glob.glob(os.path.join(static.REPORT_TEMPLATE_DIR, "*.csv")):
        templates.append({
            "name": os.path.splitext(os.path.basename(tm))[0],
            "sourceFile": os.path.basename(tm)
        })
    return templates


try:
    RESOURCES = resources()
    TEMPLATES = templates()
except TypeError:
    RESOURCES = []
    TEMPLATES = []

SCHEMA = {
    "resources": RESOURCES,
    "templates": TEMPLATES
}
