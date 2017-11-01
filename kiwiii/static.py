
import glob
import os
import yaml

from tornado import process
from chorus import molutil, wclogp
from chorus.draw.svg import SVG

from kiwiii import sqliteconnection as sqlite
from kiwiii.lod import ListOfDict


SCHEMA_VERSION = 0.8

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


CHEM_FIELDS = ListOfDict([
    {"key": "_structure", "name": "Structure", "sortType": "none"},
    {"key": "_mw", "name": "MW", "sortType": "numeric"},
    {"key": "_mw_wo_sw", "name": "MW w/o salt and water",
     "sortType": "numeric"},
    {"key": "_formula", "name": "Formula", "sortType": "text"},
    {"key": "_logp", "name": "WCLogP", "sortType": "numeric"},
    {"key": "_nonH", "name": "Non-H atom count", "sortType": "numeric"}
])

CHEM_FUNCTIONS = {
    "_structure": mol_to_svg,
    "_mw": molutil.mw,
    "_mw_wo_sw": molutil.mw_wo_sw,
    "_formula": molutil.formula,
    "_logp": wclogp.wclogp,
    "_nonH": molutil.non_hydrogen_count
}

MOLOBJ_KEY = "_mol"


def resource_format(data):
    for rsrc in data["resources"]:
        rsrc["domain"] = data["domain"]
        rsrc["resourceType"] = data["type"]
        rsrc["resourceFile"] = data["file"]
        if rsrc["domain"] == "chemical":
            rsrc["fields"].extend(CHEM_FIELDS)
        for field in rsrc["fields"]:
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
    for filename in glob.glob(os.path.join(SQLITE_BASE_DIR, "*.sqlite3")):
        conn = sqlite.Connection(filename)
        doc = conn.document()
        resource_format(doc)
        resources.extend(doc["resources"])
    # API schema
    for filename in glob.glob(os.path.join(API_BASE_DIR, "*.yaml")):
        with open(filename) as f:
            doc = yaml.load(f.read())
            screener_format(doc)
            resources.extend(doc["resources"])
    return resources


def templates():
    templates = ListOfDict()
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


EXTERNALS = [
    "contrib.screenerapi"
]


# dummy
def sqlite_path(db):
    return
