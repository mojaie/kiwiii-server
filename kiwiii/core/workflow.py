#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen

from kiwiii import static
from kiwiii.util import graph
from kiwiii.core.node import Asynchronizer
from kiwiii.core.task import Task


class Workflow(Task):
    def __init__(self, verbose=True):
        super().__init__()
        self.nodes = []
        self.preds = {}
        self.succs = {}
        self.order = None
        self.interval = 0.5
        self.query = {}
        self.datatype = "nodes"
        # TODO: verbose output
        self.verbose = verbose

    @gen.coroutine
    def submit(self):
        self.on_submitted()
        yield self.run()

    @gen.coroutine
    def run(self):
        self.on_start()
        for node_id in self.order:
            self.nodes[node_id].run()
        while any(n.status == "running" for n in self.nodes):
            if self.status == "aborted":
                return
            yield gen.sleep(self.interval)
        self.on_finish()

    def add_node(self, node):
        """parallel computation
        returns worker id
        """
        node_id = len(self.nodes)
        self.nodes.append(node)
        self.preds[node_id] = {}
        self.succs[node_id] = {}
        for in_ in node.in_edges():
            self.preds[node_id][in_.source] = True
            self.succs[in_.source][node_id] = True
        for out in node.out_edges():
            out.source = node_id
        return node.out_edges()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        # TODO: Only an Asynchronizer might be running which will be stopped
        # TODO: sqlitewriter should allow interruption
        fs = [
            n.interrupt() for n in self.nodes if isinstance(n, Asynchronizer)]
        while not all(f.done() for f in fs):
            yield gen.sleep(self.interval)
        self.on_aborted()

    def on_submitted(self):
        # TODO: catch exceptions on submit (ex. file path error, format error)
        self.order = graph.topological_sort(self.succs, self.preds)
        for node_id in self.order:
            self.nodes[node_id].on_submitted()

    def output(self):
        # TODO:
        data = {
            "id": self.id,
            "name": self.id[:8],
            "dataType": self.datatype,
            "schemaVersion": static.SCHEMA_VERSION,
            "revision": 0,
            "created": time.strftime("%X %x %Z",
                                     time.localtime(self.creation_time)),
            "status": self.status
        }
        if self.start_time is not None:
            data["execTime"] = round(time.time() - self.start_time, 1)
        if hasattr(self, "query"):
            data["query"] = self.query
        if hasattr(self, "fields"):
            data["fields"] = self.fields
        if hasattr(self, "result_records"):
            data["records"] = self.result_records
        if hasattr(self, "result_count"):
            data["resultCount"] = self.result_count
        if hasattr(self, "task_count") and hasattr(self, "done_count"):
            data["taskCount"] = self.task_count
            data["doneCount"] = self.done_count
            try:
                p = round(self.done_count / self.task_count * 100, 1)
            except ZeroDivisionError:
                p = None
            # TODO: mpfilter should send doneCount even if the row is filtered out
            if self.status == "done":
                data["progress"] = 100
            else:
                data["progress"] = p
        if hasattr(self, "nodesid"):
            data["nodesID"] = self.nodesid
        return data
