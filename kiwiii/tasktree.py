#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

class TaskTree(Worker):
    def __init__(self, worker):
        self.tasks = [worker]
        self.pred = {}
        # TODO: avoid loop

    @gen.coroutine
    def run(self):
        while tasks:
            pass

    def put_worker(self, worker, parent):
        """parallel computation
        returns node id
        """
        self.tasks.append(worker)

    def put_task(self, task, parent):
        """simple task (function and args)
        both command(destructive) and query(non-destr) tasks are acceptable
        returns node id
        """
        pass
