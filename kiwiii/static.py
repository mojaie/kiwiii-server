
import glob
import os
import yaml

from kiwiii import sqliteconnection as sqlite
from kiwiii import tablecolumn as tc
from tornado import process


""" Option module availability """

from chorus import mcsdr

CYTHON_AVAILABLE = mcsdr.CYTHON_AVAILABLE
NUMEXPR_AVAILABLE = mcsdr.NUMEXPR_AVAILABLE

try:
    from chorus import rdkit
    RDK_AVAILABLE = True
    print("RDKit is available")
except ImportError:
    RDK_AVAILABLE = False
    print("RDKit is not available")


""" Server status """

PROCESSES = process.cpu_count()


""" Settings """

try:
    with open("server_config.yaml") as f:
        config = yaml.load(f.read())
except FileNotFoundError:
    """ use server_config stub"""
    config = {
        "web_home": "",
        "basic_auth_realm": "",
        "user": {},
    }

USERS = config["user"]
WEB_HOME = config["web_home"]
BASIC_AUTH_REALM = config["basic_auth_realm"]


def user_passwd_matched(user, passwd):
    return user in USERS and passwd == USERS[user]["password"]


SQLITE_BASE_DIR = config.get("sqlite_base_dir", "")
API_BASE_DIR = config.get("api_base_dir", "")
REPORT_TEMPLATE_DIR = config.get("report_template_dir", "")


def resource_format(data):
    for tbl in data["tables"]:
        if data["domain"] == "chemical":
            tc.add_calc_cols(tbl)
        tbl["domain"] = data["domain"]
        for col in tbl["columns"]:
            if "name" not in col:
                col["name"] = col["key"]
            if "request" not in col:
                col["request"] = "search"
            if "valueType" in col:
                col["sort"] = "numeric"
            elif "sort" not in col:
                col["sort"] = "text"


def screener_format(data):
    for rsrc in data["resources"]:
        rsrc["domain"] = data["domain"]
        rsrc["url"] = data["url"]
        rsrc["entity"] = "{}:{}".format(rsrc["qcsRefId"], rsrc["layerIndex"])


def resources():
    resources = []
    # SQLite databases
    for filename in glob.glob(os.path.join(SQLITE_BASE_DIR, "*.sqlite3")):
        conn = sqlite.Connection(filename)
        doc = conn.document()
        resource_format(doc)
        resources.extend(doc["tables"])
    # API schema
    for filename in glob.glob(os.path.join(API_BASE_DIR, "*.yaml")):
        with open(filename) as f:
            doc = yaml.load(f.read())
            screener_format(doc)
            resources.extend(doc["resources"])
    return resources


def templates():
    templates = []
    for tm in glob.glob(os.path.join(REPORT_TEMPLATE_DIR, "*.csv")):
        templates.append({
            "name": os.path.splitext(os.path.basename(tm))[0],
            "sourceFile": os.path.basename(tm)
        })
    return templates


RESOURCES = resources()
TEMPLATES = templates()
SCHEMA = {
    "resources": RESOURCES,
    "templates": TEMPLATES
}


# dummy
def sqlite_path(db):
    return
