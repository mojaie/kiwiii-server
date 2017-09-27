#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from kiwiii.test.node.util import twice, MPWorkerResults, FlashNode, IdleNode
from kiwiii.task import Edge, AsyncQueueEdge, Workflow


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

    @gen_test
    def test_workflow(self):
        wf = Workflow()
        e0 = Edge()
        e1 = wf.add_node(FlashNode(e0))
        wf.add_node(FlashNode(e1))
        yield wf.run()
        self.assertTrue(all(n.status == "done" for n in wf.nodes))

    @gen_test
    def test_asyncworkflow(self):
        wf = Workflow()
        e0 = AsyncQueueEdge()
        e1 = wf.add_node(IdleNode(e0))
        wf.add_node(IdleNode(e1))
        wf.run()
        yield wf.interrupt()
        self.assertTrue(all(n.status == "aborted" for n in wf.nodes))


if __name__ == '__main__':
    unittest.main()
