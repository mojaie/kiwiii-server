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


class FlashTask(Task):
    @gen.coroutine
    def run(self):
        pass


class IdleTask(Task):
    @gen.coroutine
    def run(self):
        self.on_start()
        while 1:
            if self.status == "interrupted":
                self.on_aborted()
                return
            yield gen.sleep(0.5)
        self.on_finish()

    @gen.coroutine
    def interrupt(self):
        self.status = "interrupted"
        while self.status != "aborted":
            yield gen.sleep(0.5)


class TestJobQueue(AsyncTestCase):
    @gen_test
    def test_store(self):
        jq = jobqueue.JobQueue()
        task1 = FlashTask()
        task2 = FlashTask()
        task3 = FlashTask()
        yield jq.put(task1)
        yield jq.put(task2)
        task3.created = time.time() - jq.task_lifetime - 100
        yield jq.put(task3)
        self.assertEqual(len(jq.store), 2)
        self.assertEqual(jq.get(task2.id).id, task2.id)
        with self.assertRaises(ValueError):
            jq.get("invalidKey")

    @gen_test
    def test_queue(self):
        jq = jobqueue.JobQueue()
        task1 = IdleTask()
        task2 = IdleTask()
        yield jq.put(task1)
        yield jq.put(task2)
        yield gen.sleep(0.01)
        self.assertEqual(task1.status, "running")
        self.assertEqual(task2.status, "ready")
        yield jq.abort(task1.id)
        self.assertEqual(task1.status, "aborted")
        self.assertEqual(task2.status, "running")
        yield jq.abort(task2.id)
        self.assertEqual(task2.status, "aborted")
        self.assertEqual(len(jq.store), 2)


if __name__ == '__main__':
    unittest.main()
