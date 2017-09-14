#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.worker import Worker
from kiwiii.util import graph


class ResponseWorker(Worker):
    def __init__(self, args, task):
        super().__init__(args, task["function"])
        self.task = task

    def on_task_done(self, res):
        self.task["output"].append(res)


class TaskTree(Worker):
    def __init__(self):
        self.tasks = []
        self.preds = {}
        self.succs = {}

    @gen.coroutine
    def run(self):
        order = graph.topological_sort(self.succs, self.preds)
        for flow_id in order:
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

    def put_task(self, function, args, parents=None, mp=False):
        """parallel computation
        returns worker id
        """
        task = {
            "function": function, "args": args, "mp": mp, "parents": parents
        }
        flow_id = len(self.tasks)
        self.tasks.append(task)
        self.preds[flow_id] = {}
        self.succs[flow_id] = {}
        if parents is not None:
            for p in parents:
                self.preds[flow_id][parents] = True
                self.succs[parents][flow_id] = True
        return flow_id
