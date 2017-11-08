#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.io.json import JSONResponse, AsyncJSONResponse
from kiwiii.node.io.iterator import IteratorInput
from kiwiii.core.node import LazyNode, Asynchronizer
from kiwiii.core.workflow import Workflow


class TestJSONResponse(AsyncTestCase):
    @gen_test
    def test_jsonresponse(self):
        wf = Workflow()
        e, = wf.add_node(IteratorInput({"value": i} for i in range(10)))
        wf.add_node(JSONResponse(e, wf))
        yield wf.submit()
        self.assertEqual(len(wf.response["records"]), 10)
        self.assertEqual(wf.response["status"], "done")

    @gen_test
    def test_asyncjsonresponse(self):
        wf = Workflow()
        e1, = wf.add_node(IteratorInput({"value": i} for i in range(10)))
        e2, = wf.add_node(Asynchronizer(e1))
        n2 = AsyncJSONResponse(e2, wf)
        wf.add_node(n2)
        n2.interval = 0.01
        yield wf.submit()
        self.assertEqual(len(wf.response["records"]), 10)
        self.assertEqual(wf.response["status"], "done")

    @gen_test
    def test_interrupt(self):
        wf = Workflow()
        e1, = wf.add_node(IteratorInput({"value": i} for i in range(100)))
        e2, = wf.add_node(Asynchronizer(e1))
        n3 = LazyNode(e2)
        e3, = wf.add_node(n3)
        n4 = AsyncJSONResponse(e3, wf)
        wf.add_node(n4)
        n3.interval = 0.01
        n4.interval = 0.01
        wf.submit()
        yield wf.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n3.status, "aborted")
        self.assertGreater(len(wf.response["records"]), 0)


if __name__ == '__main__':
    unittest.main()
