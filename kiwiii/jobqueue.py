#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time

from tornado import gen
from tornado.queues import Queue


class ResultStore(object):
    """Temporary data store for calculation result fetcher"""
    def __init__(self):
        self.container = []
        self.max_age = 86400 * 7  # Time(sec)

    def register(self, task, now=time.time()):
        self.container.append(task)
        # remove expired data
        alive = []
        for task in self.container:
            if task.created + self.max_age > now:
                alive.append(task)
        self.container = alive

    def get(self, id_):
        for task in self.container:
            if task.id == id_:
                return task
        raise ValueError('Table not found')

    def tasks_iter(self):
        for task in self.store.container:
            expires = task.created + self.max_age
            yield task, expires


class JobQueue(object):
    def __init__(self, store=ResultStore):
        self.queue = Queue()
        self.store = store()
        self.current_task_id = None
        self.current_task = None
        self.queued_ids = []
        self.aborted_ids = []
        self._dispatcher()

    def __len__(self):
        return len(self.store.container)

    def put(self, task):
        """ Put a job to the queue """
        print("put {}".format(task.id))
        self.store.register(task)
        self.queued_ids.append(task.id)
        self.queue.put_nowait(task)

    def get_result(self, id_):
        return self.store.get(id_)

    def abort(self, id_):
        if id_ in self.queued_ids:
            self.aborted_ids.append(id_)
        elif id_ == self.current_task_id:
            self.current_task.interrupt()

    def status(self, id_):
        if id_ in self.queued_ids:
            return "Queued"
        elif id_ == self.current_task_id:
            return self.current_task.status
        else:
            return "Completed"

    @gen.coroutine
    def _dispatcher(self):
        while 1:
            task = yield self.queue.get()
            self.queued_ids.remove(task.id)
            if task.id in self.aborted_ids:
                self.aborted_ids.remove(task.id)
                continue
            self.current_task_id = task.id
            self.current_task = task
            yield self.current_task.run()
            self.current_task_id = None
            self.current_task = None

    def tasks_iter(self):
        return self.store.tasks_iter()
