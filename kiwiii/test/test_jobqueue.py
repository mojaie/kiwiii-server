#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import time
import unittest

from kiwiii import jobqueue
from kiwiii.task import Task

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test


class IdleTask(Task):
    @gen.coroutine
    def run(self):
        while 1:
            if self.interruption_requested:
                break
            yield gen.sleep(0.2)


class FlashTask(Task):
    @gen.coroutine
    def run(self):
        pass


class TestResultStore(unittest.TestCase):
    def test_resultstore(self):
        store = jobqueue.ResultStore()
        task1 = Task()
        task2 = Task()
        task3 = Task()
        store.register(task1)
        store.register(task2)
        task3.created = time.time() - store.max_age - 100
        store.register(task3)
        self.assertEqual(len(store.container), 2)
        self.assertEqual(store.get(task2.id).id, task2.id)
        with self.assertRaises(ValueError):
            store.get("invalidKey")


class TestJobQueue(AsyncTestCase):
    @gen_test
    def test_jobqueue(self):
        jq = jobqueue.JobQueue()
        task1 = FlashTask()
        yield jq.put(task1)


if __name__ == '__main__':
    unittest.main()
