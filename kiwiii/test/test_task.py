#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from kiwiii.task import MPWorker, AsyncQueueEdge


def twice(dict_):
    return {"value": dict_["value"] * 2}


class MPWorkerResults(MPWorker):
    def __init__(self, *args):
        super().__init__(*args)
        self.results = []

    def on_task_done(self, res):
        self.results.append(res)


class TestTask(AsyncTestCase):
    @gen_test
    def test_mpworker(self):
        args = [{"value": i} for i in range(10)]
        worker = MPWorkerResults(args, twice)
        yield worker.run()
        total = sum(a["value"] for a in worker.results)
        self.assertEqual(total, 90)

    @gen_test
    def test_mpinterrupt(self):
        args = [{"value": i} for i in range(100)]
        worker = MPWorkerResults(args, twice)
        worker.run()
        yield worker.interrupt()
        self.assertEqual(worker.status, "aborted")

    @gen_test
    def test_asyncedge(self):
        edge = AsyncQueueEdge()
        in_ = 123
        yield edge.put(in_)
        out = yield edge.get()
        self.assertEqual(out, 123)
        yield edge.done()
        self.assertEqual(edge.status, "done")


if __name__ == '__main__':
    unittest.main()
