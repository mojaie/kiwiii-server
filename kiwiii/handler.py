#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import time

from chorus.draw.svg import SVG
from chorus.model.graphmol import Compound
from chorus import v2000writer
from chorus.util.text import decode

from tornado import web, gen
from tornado.ioloop import IOLoop
from tornado.options import define, options, parse_command_line

from kiwiii import excelexporter
from kiwiii import static
from kiwiii import screenerapi
from kiwiii import auth
from kiwiii import sqlitehelper
from kiwiii.core.jobqueue import JobQueue
from kiwiii.workflow import db
from kiwiii.workflow import similaritynetwork as simnet
from kiwiii.workflow import substructure as substr
from kiwiii.workflow.chemprop import ChemProp
from kiwiii.workflow.gls import GLS
from kiwiii.workflow.rdkitfmcs import RDKitFMCS
from kiwiii.workflow.rdkitmorgan import RDKitMorgan
from kiwiii.workflow.sdfparser import SDFParser


class BaseHandler(web.RequestHandler):
    pass
    # def initialize(self):
    #    self.created = time.strftime("%X %x %Z")

    # def get_current_user(self):
    #     return self.get_secure_cookie("user")


class SchemaHandler(BaseHandler):
    @auth.basic_auth
    def get(self):
        """Responds with resource schema JSON

        :statuscode 200: no error
        """
        self.write(static.SCHEMA)


class WorkflowHandler(BaseHandler):
    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Runs calculation job and immediately responds"""
        query = json.loads(self.get_argument("query"))
        workflows = {
            "search": db.DBSearch,
            "filter": db.DBFilter,
            "chemsearch": db.ChemDBSearch,
            "chemfilter": db.ChemDBFilter,
            "exact": substr.ExactStruct
        }
        wf = workflows[query["type"]](query)
        yield wf.submit()
        self.write(wf.response)


class AsyncWorkflowHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Submits calculation job"""
        query = json.loads(self.get_argument("query"))
        workflows = {
            "substr": substr.Substruct,
            "supstr": substr.Superstruct,
            "prop": ChemProp,
            "gls": GLS,
            "rdfmcs": RDKitFMCS,
            "rdmorgan": RDKitMorgan
        }
        wf = workflows[query["type"]](query)
        yield self.jq.put(wf)
        self.write(wf.response)


class ResultHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @auth.basic_auth
    @gen.coroutine
    def get(self):
        """Fetch calculation results"""
        query = json.loads(self.get_argument("query"))
        try:
            wf = self.jq.get(query["id"])
        except KeyError:
            self.write({
                "id": query["id"],
                "status": "failure",
                "message": "job not found"
            })
        else:
            if query["command"] == "abort":
                yield self.jq.abort(query["id"])
            self.write(wf.response)


class SimilarityNetworkHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @auth.basic_auth
    def post(self):
        """Submit similarity network calculation job"""
        contents = json.loads(self.request.files['json'][0]['body'].decode())
        params = json.loads(self.get_argument("params"))
        workflows = {
            "gls": simnet.GLSNetwork,
            "rdmorgan": simnet.RDKitMorganNetwork,
            "rdfmcs": simnet.RDKitFMCSNetwork,
        }
        wf = workflows[params["type"]](contents, params)
        yield self.jq.put(wf)
        self.write(wf.response)


class StructurePreviewHandler(BaseHandler):
    @auth.basic_auth
    def get(self):
        """Structure image preview"""
        query = json.loads(self.get_argument("query"))
        try:
            qmol = sqlitehelper.query_mol(query)
        except TypeError:
            response = '<span class="msg_warn">Format Error</span>'
        except ValueError:
            response = '<span class="msg_warn">Not found</span>'
        else:
            response = SVG(qmol).contents()
        self.write(response)


class SDFileParserHandler(BaseHandler):
    def post(self):
        """Responds with datatable JSON made of query SDFile"""
        filename = self.request.files['contents'][0]['filename']
        contents = decode(self.request.files['contents'][0]['body'])
        params = json.loads(self.get_argument("params"))
        query = {
            "sourceFile": filename,
            "params": params
        }
        wf = SDFParser(contents, query)
        yield wf.submit()
        self.write(wf.response)


class SDFileExportHandler(BaseHandler):
    def post(self):
        contents = json.loads(self.request.files['json'][0]['body'].decode())
        cols = [c["key"] for c in contents["fields"]
                if c["visible"] and c["sort"] != "none"]
        mols = []
        for rcd in contents["records"]:
            mol = Compound(json.loads(rcd[static.MOLOBJ_KEY]))
            for col in cols:
                mol.data[col] = rcd[col]
            mols.append(mol)
        text = v2000writer.mols_to_text(mols)
        self.set_header("Content-Type", 'text/plain; charset="utf-8"')
        self.write(text)


class ExcelExportHandler(BaseHandler):
    def post(self):
        js = json.loads(self.request.files['json'][0]['body'].decode())
        data = {"tables": [js]}
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class ReportPreviewHandler(BaseHandler):
    def get(self):
        auth_header = self.request.headers.get('Authorization')
        # TODO: auth_header is local servers one
        # auth_decoded = base64.b64decode(auth_header[6:]).decode('utf-8')
        # user, passwd = auth_decoded.split(':')
        # print(user, passwd)
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        qcs = screenerapi.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = auth.TemplateMatcher(tmpl_file, "Preview", 320)
        arrays = screenerapi.get_all_layer_values(qcsid, 0, auth_header)
        for i in layer_idxs:
            tmpl.add_array(arrays[i], layer_name[i])
        self.write(tmpl.to_json())


class ReportHandler(BaseHandler):
    def get(self):
        auth_header = self.request.headers.get('Authorization')
        qcsid = self.get_argument("qcsid")
        tmpl_file = self.get_argument("template")
        layer_idxs = [int(i) for i in self.get_arguments("vsel")]
        stat_idxs = [int(i) for i in self.get_arguments("ssel")]
        qcs = screenerapi.get_qcs_info((qcsid,), auth_header)[0]
        layer_name = {y["layerIndex"]: y["name"] for y in qcs["layers"]}
        tmpl = auth.TemplateMatcher(tmpl_file, "Results")
        for i in layer_idxs:
            array = screenerapi.get_all_plate_values(qcsid, i, auth_header)
            tmpl.add_array(array, layer_name[i])
        data = {"tables": [tmpl.to_json()]}
        for i in stat_idxs:
            stat = screenerapi.get_all_plate_stats(qcsid, i, auth_header)
            stat["name"] = layer_name[i]
            data["tables"].append(stat)
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


"""
class LoginHandler(BaseHandler):
    def post(self):
        user = self.get_argument("name")
        pw = self.get_argument("pass")
        if user in config["user"] and pw == config["user"][user]["password"]:
            self.set_secure_cookie("user", user)
            self.write({"authenticated": True})
        else:
            self.write({"authenticated": False})
"""


class ServerStatusHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq
        self.instance = instance

    @auth.basic_auth
    def get(self):
        js = {
            "totalTasks": len(self.jq),
            "queuedTasks": self.jq.queue.qsize(),
            "instance": self.instance,
            "processors": static.PROCESSES,
            "rdkit": static.RDK_AVAILABLE,
            "numericModule": static.NUMERIC_MODULE,
            "calc": {
                "columns": [
                    {"key": "id"},
                    {"key": "size"},
                    {"key": "status"},
                    {"key": "created"},
                    {"key": "expires"}
                ],
                "records": []
            }
        }
        for task, expires in self.jq.tasks_iter():
            js["calc"]["records"].append({
                "id": task.id,
                "size": task.size(),
                "status": task.status,
                "created": time.strftime(
                    "%X %x %Z", time.localtime(task.created)),
                "expires": time.strftime(
                    "%X %x %Z", time.localtime(expires)),
            })
        self.write(js)


def run():
    define("port", default=8888, help="run on the given port", type=int)
    define("debug", default=False, help="run in debug mode")
    parse_command_line()
    store = {
        "jq": JobQueue(),
        "instance": time.strftime("%X %x %Z", time.localtime(time.time()))
    }
    app = web.Application(
        [
            (r"/schema", SchemaHandler),
            (r"/run", WorkflowHandler),
            (r"/async", AsyncWorkflowHandler, store),
            (r"/res", ResultHandler, store),
            (r"/simnet", SimilarityNetworkHandler, store),
            (r"/strprev", StructurePreviewHandler),
            (r"/sdfin", SDFileParserHandler),
            (r"/sdfout", SDFileExportHandler),
            (r"/xlsx", ExcelExportHandler),
            (r"/report", ReportHandler),
            (r"/reportprev", ReportPreviewHandler),
            (r"/server", ServerStatusHandler, store),
            (r'/(.*)', web.StaticFileHandler, {"path": static.WEB_HOME})
        ],
        debug=options.debug,
        compress_response=True,
        cookie_secret="_TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE_"
    )
    app.listen(options.port)
    try:
        print("Server started")
        IOLoop.instance().start()
    except KeyboardInterrupt:
        print("Server stopped")
