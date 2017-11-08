
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
        if rsrc["domain"] == "chemical":
            rsrc["fields"].extend(static.CHEM_FIELDS)
        for field in rsrc["fields"]:
            if field["key"] == "_molobj":
                continue
            if "name" not in field:
                field["name"] = field["key"]
            if "request" not in field:
                field["request"] = "search"
            if "valueType" not in field:
                field["valueType"] = "numeric"
            if field["valueType"] in ["text", "compound_id"]:
                field["sortType"] = "text"
            else:
                field["sortType"] = "numeric"


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
