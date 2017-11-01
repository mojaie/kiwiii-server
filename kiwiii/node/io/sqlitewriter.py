#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import sqlite3

from tornado import gen

from kiwiii.core.node import Synchronizer


class SQLiteWriter(Synchronizer):
    def __init__(self, in_edge, wf):
        super().__init__(in_edge)
        self.wf = wf
        self.records = []

    @gen.coroutine
    def _get_loop(self):
        while 1:
            in_ = yield self.in_edge.get()
            self.records.append(in_)
            self.wf.resultCount += 1
            self.wf.doneCount = self.in_edge.done_count

    @gen.coroutine
    def run(self):
        self.on_start()
        self._get_loop()
        while 1:
            if self.in_edge.status == "aborted":
                self.out_edge.status = "aborted"
                self.on_aborted()
                return
            if self.in_edge.status == "done":
                self.out_edge.status = "done"
                break
            yield gen.sleep(self.interval)
        try:
            # TODO: insert records
            pass
        except sqlite3.Error:
            self.on_aborted()
        else:
            self.on_finish()

    def out_edges(self):
        return tuple()

    def on_submitted(self):
        self.wf.fields.merge(self.in_edge.fields)
        self.wf.resultCount = 0
        self.wf.taskCount = self.in_edge.task_count
        self.wf.doneCount = 0

    def interrupt(self):
        # TODO: core.workflow will call this when interrupted
        # TODO: conn.interrupt()
        pass
