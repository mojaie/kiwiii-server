
import yaml

from tornado import process
from chorus import molutil, wclogp
from chorus.draw.svg import SVG

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
    {"key": "_molobj"},
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

EXTERNALS = [
    "contrib.screenerapi"
]
