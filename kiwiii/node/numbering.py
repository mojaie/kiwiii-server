#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.node import Node
from kiwiii.util import graph





class SQLiteInput(Node):
    def __init__(self):
        self.tasks = []
        self.preds = {}
        self.succs = {}

    @gen.coroutine
    def run(self):
        order = graph.topological_sort(self.succs, self.preds)
        for flow_id in order:
            print("Started task {}".format(flow_id))
            task = self.tasks[flow_id]
            args = []
            if task["parents"] is not None:
                for parent in task["parents"]:
                    args.append(self.tasks[parent]["output"])
            args.append(task["args"])
            if "mp" in task:
                worker = ResponseWorker(zip(args), self.tasks[flow_id])
                yield worker.run()
            else:
                res = itertools.starmap(task["function"], *zip(args))
                self.tasks[flow_id]["output"] = res
        print("Finished task {}".format(flow_id))
