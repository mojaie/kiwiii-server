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
from kiwiii import handlerutil as hu
from kiwiii import tablebuilder as tb
from kiwiii import tablefilter as tf
from kiwiii import tablecolumn as tc
from kiwiii.core.jobqueue import JobQueue
from kiwiii.workflow.exactstruct import ExactStruct
from kiwiii.workflow.chemdbfilter import ChemDBFilter
from kiwiii.workflow.chemdbsearch import ChemDBSearch
from kiwiii.workflow.chemprop import ChemProp
from kiwiii.workflow.dbfilter import DBFilter
from kiwiii.workflow.dbsearch import DBSearch
from kiwiii.workflow.gls import GLS
from kiwiii.workflow.rdkitfmcs import RDKitFMCS
from kiwiii.workflow.rdkitmorgan import RDKitMorgan
from kiwiii.workflow.substructure import Substructure


class BaseHandler(web.RequestHandler):
    pass
    # def initialize(self):
    #    self.created = time.strftime("%X %x %Z")

    # def get_current_user(self):
    #     return self.get_secure_cookie("user")


class SchemaHandler(BaseHandler):
    @hu.basic_auth
    def get(self):
        """Responds with resource schema JSON

        :statuscode 200: no error
        """
        self.write(static.SCHEMA)


class DBSearchHandler(BaseHandler):
    @hu.basic_auth
    @gen.coroutine
    def get(self):
        """Search database

        :form query: JSON encoded query

        :<json string type: query type
        :<json object targets: list of resource entity

        for sql query
        :<json string key: query column key
        :<json object values: list of query value
        :<json string operator: ``eq``, ``gt``, ``lt``, ``ge``, ``le``,
            ``in``, ``lk``

        for structure query
        :<json string format: one of ``smiles``, ``molfile``, ``dbid``
        :<json string value: query value
        :<json string source: resource of the query value(for format=dbid)
        :<json boolean ignoreHs: if true, ignore explicit hydrogens

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        workflows = {
            "search": DBSearch,
            "filter": DBFilter,
            "chemsearch": ChemDBSearch,
            "chemfilter": ChemDBFilter,
            "exact": ExactStruct
        }
        wf = workflows[query["type"]](query)
        yield wf.submit()
        self.write(wf.response)


class SubmitJobHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @hu.basic_auth
    @gen.coroutine
    def get(self):
        """Search database

        :form query: JSON encoded query

        :<json string type: query type
        :<json object targets: list of resource entity

        for sql query
        :<json string key: query column key
        :<json object values: list of query value
        :<json string operator: ``eq``, ``gt``, ``lt``, ``ge``, ``le``,
            ``in``, ``lk``

        for structure query
        :<json string format: one of ``smiles``, ``molfile``, ``dbid``
        :<json string value: query value
        :<json string querySource: resource of the query
        :<json boolean ignoreHs: if true, ignore explicit hydrogens

        similarity search options
        :<json float threshold:
        :<json int diameter: MCS-DR parameter
        :<json int maxTreeSize: MCS-DR parameter
        :<json int molSizeCutoff: MCS-DR parameter

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        workflows = {
            "substr": Substructure,
            "prop": ChemProp,
            "gls": GLS,
            "rdfmcs": RDKitFMCS,
            "rdmorgan": RDKitMorgan
        }
        wf = workflows[query["type"]](query)
        yield self.jq.put(wf)
        self.write(wf.response)


class JobResultHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @hu.basic_auth
    @gen.coroutine
    def get(self):
        """Fetch calculation results

        :form query: JSON encoded query

        :<json string id: datatable id
        :<json string command: ``fetch`` | ``abort``

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        try:
            wf = self.jq.get(query["id"])
        except KeyError:
            self.write({
                "id": query["id"],
                "status": "Failure",
                "message": "Temporary table not found."
            })
        else:
            if query["command"] == "abort":
                yield self.jq.abort(query["id"])
            self.write(wf.response)


class StructurePreviewHandler(BaseHandler):
    @hu.basic_auth
    def get(self):
        """Structure image preview

        :form query: JSON encoded query

        :<json string format: one of ``smiles``, ``molfile``, ``dbid``
        :<json string value: query value
        :<json string source: resource of the query value

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        try:
            qmol = tf.parse_chem_query(
                query["value"], query["format"], query.get("source"))
        except TypeError:
            response = '<span class="msg_warn">Format Error</span>'
        except ValueError:
            response = '<span class="msg_warn">Not found</span>'
        else:
            response = SVG(qmol).contents()
        self.write(response)


class SdfHandler(BaseHandler):
    def post(self):
        """Responds with datatable JSON made of query SDFile

        :form contents: request file object
        :form columns: list of SDF option attribute to show
        :form implh: if 1, make hydrogens implicit
        :form recalc: if 1, recalculate 2D coordinates

        :statuscode 200: no error
        """
        filename = self.request.files['contents'][0]['filename']
        contents = decode(self.request.files['contents'][0]['body'])
        query = json.loads(self.get_argument("query"))
        builder = tb.TableBuilder()
        builder.data["query"] = {"sourceFile": filename}
        builder.add_filter(tf.SdfFilter(contents, query))
        builder.add_filter(tc.IndexColumn())
        builder.add_filter(tc.StructureColumn())
        builder.add_filter(tc.CalcColumnGroup())
        tc.add_calc_cols(builder.data)
        # builder.add_filter(tc.ChemAliaseColumn(config["default_chemlib"]))
        self.write(builder.flush())


class GraphHandler(BaseHandler):
    def initialize(self, jq, instance):
        super().initialize()
        self.jq = jq

    @hu.basic_auth
    def post(self):
        """Submit graph connection calculation job

        :form query: JSON encoded query

        :<json string nodeTableId:
        :<json object indices: list of index
        :<json object molecules: list of molecule object
        :<json string measure: ``gls`` | ``morgan``
        :<json float threshold: graph connection score threshold
        :<json boolean ignoreHs: if true, ignore explicit hydrogens
        :<json int diameter: MCS-DR parameter
        :<json int maxTreeSize: MCS-DR parameter
        :<json int molSizeCutoff: MCS-DR parameter

        :statuscode 200: no error
        """
        query = json.loads(self.get_argument("query"))
        builder = tb.TableBuilder()
        matrix_filter = {
            "gls": tf.GlsMatrixFilter,
            "rdmorgan": tf.RdMorganMatrixFilter,
            "rdfmcs": tf.RdFmcsMatrixFilter
        }
        builder.add_filter(matrix_filter[query["measure"]](query))
        builder.queue(self)


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
        tmpl = hu.TemplateMatcher(tmpl_file, "Preview", 320)
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
        tmpl = hu.TemplateMatcher(tmpl_file, "Results")
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


class ExcelExportHandler(BaseHandler):
    def post(self):
        js = json.loads(self.request.files['json'][0]['body'].decode())
        data = {"tables": [js]}
        buf = excelexporter.json_to_xlsx(data)
        self.set_header("Content-Type", 'application/vnd.openxmlformats-office\
                        document.spreadsheetml.sheet; charset="utf-8"')
        self.write(buf.getvalue())


class SDFileExportHandler(BaseHandler):
    def post(self):
        MOL = tc.MolObjectColumn()
        table = json.loads(self.request.files['json'][0]['body'].decode())
        cols = [c["key"] for c in table["columns"]
                if c["visible"] and c["sort"] != "none"]
        mols = []
        for rcd in table["records"]:
            mol = Compound(json.loads(rcd[MOL.key]))
            for col in cols:
                mol.data[col] = rcd[col]
            mols.append(mol)
        text = v2000writer.mols_to_text(mols)
        self.set_header("Content-Type", 'text/plain; charset="utf-8"')
        self.write(text)


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

    @hu.basic_auth
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
            (r"/search", DBSearchHandler),
            (r"/job", SubmitJobHandler, store),
            (r"/jobres", JobResultHandler, store),
            (r"/graph", GraphHandler, store),
            (r"/sdf", SdfHandler),
            (r"/xlsx", ExcelExportHandler),
            (r"/exportsdf", SDFileExportHandler),
            (r"/strprev", StructurePreviewHandler),
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
