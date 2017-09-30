
import glob
import os
import yaml

from tornado import process
from chorus import molutil, wclogp
from chorus.draw.svg import SVG

from kiwiii import sqliteconnection as sqlite
from kiwiii import tablecolumn as tc


""" Option module availability """

try:
    from chorus import rdkit
    RDK_AVAILABLE = True
except ImportError:
    RDK_AVAILABLE = False
try:
    import cython
    NUMERIC_MODULE = "Cython"
except ImportError:
    try:
        import numexpr
        NUMERIC_MODULE = "NumExpr"
    except ImportError:
        NUMERIC_MODULE = "Numpy"


""" Server status """

PROCESSES = process.cpu_count()


""" Settings """

try:
    with open("server_config.yaml") as f:
        config = yaml.load(f.read())
except FileNotFoundError:
    """ use server_config stub"""
    config = {}


USERS = config.get("user")
WEB_HOME = config.get("web_home")
BASIC_AUTH_REALM = config.get("basic_auth_realm")


def user_passwd_matched(user, passwd):
    return user in USERS and passwd == USERS[user]["password"]


SQLITE_BASE_DIR = config.get("sqlite_base_dir")
API_BASE_DIR = config.get("api_base_dir")
REPORT_TEMPLATE_DIR = config.get("report_template_dir")


def mol_to_svg(mol):
    return SVG(mol).contents()


CHEM_FIELDS = [
    {"key": "_mw", "name": "MW", "sort": "numeric"},
    {"key": "_formula", "name": "Formula", "sort": "text"},
    {"key": "_logp", "name": "WCLogP", "sort": "numeric"},
    {"key": "_nonH", "name": "Non-H atom count", "sort": "numeric"}
]

CHEM_FUNCTIONS = {
    "_structure": mol_to_svg,
    "_mw": molutil.mw,
    "_formula": molutil.formula,
    "_logp": wclogp.wclogp,
    "_nonH": molutil.non_hydrogen_count
}

MOLOBJ_KEY = "_mol"


def resource_format(data):
    for tbl in data["tables"]:
        if data["domain"] == "chemical":
            tbl["columns"].extend(CHEM_FIELDS)
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


# dummy
def sqlite_path(db):
    return
