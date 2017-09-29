#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.jsonresponse import JSONResponse, AsyncJSONResponse
from kiwiii.core.node import LazyNode, Asynchronizer
from kiwiii.core.edge import Edge
from kiwiii.core.workflow import Workflow


class TestJSONResponse(AsyncTestCase):
    def test_jsonresponse(self):
        wf = Workflow()
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        n = JSONResponse(in_edge, wf)
        n.run()
        self.assertEqual(len(wf.response["records"]), 10)
        self.assertEqual(wf.response["status"], "done")

    @gen_test
    def test_asyncjsonresponse(self):
        wf = Workflow()
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        n1 = Asynchronizer(in_edge)
        n2 = AsyncJSONResponse(n1.out_edges()[0], wf)
        n2.interval = 0.01
        n1.run()
        yield n2.run()
        self.assertEqual(n1.status, "done")
        self.assertEqual(n2.status, "done")
        self.assertEqual(len(wf.response["records"]), 10)
        self.assertEqual(wf.response["status"], "done")

    @gen_test
    def test_interrupt(self):
        wf = Workflow()
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(100)]
        n1 = Asynchronizer(in_edge)
        n2 = LazyNode(n1.out_edges()[0])
        n3 = AsyncJSONResponse(n2.out_edges()[0], wf)
        n2.interval = 0.01
        n3.interval = 0.01
        n1.run()
        n2.run()
        n3.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n3.status, "aborted")
        self.assertGreater(len(wf.response["records"]), 0)


if __name__ == '__main__':
    unittest.main()
