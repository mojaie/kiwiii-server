#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from kiwiii.core.node import FlashNode, LazyConsumer, EagerProducer
from kiwiii.core.edge import Edge
from kiwiii.core.workflow import Workflow


def twice(dict_):
    return {"value": dict_["value"] * 2}


class TestWorkflow(AsyncTestCase):
    @gen_test
    def test_workflow(self):
        wf = Workflow()
        n1 = FlashNode(Edge())
        n1.in_edges = lambda: tuple()
        e1, = wf.add_node(n1)
        wf.add_node(FlashNode(e1))
        wf.on_submitted()
        yield wf.run()
        self.assertTrue(all(n.status == "done" for n in wf.nodes))

    @gen_test
    def test_asyncworkflow(self):
        wf = Workflow()
        n1 = EagerProducer()
        e1, = wf.add_node(n1)
        n2 = LazyConsumer(e1)
        wf.add_node(n2)
        wf.on_submitted()
        self.assertEqual(n1.status, "ready")
        self.assertEqual(e1.status, "ready")
        self.assertEqual(n2.status, "ready")
        wf.run()
        self.assertEqual(n1.status, "running")
        self.assertEqual(e1.status, "ready")
        self.assertEqual(n2.status, "running")
        yield wf.interrupt()
        self.assertEqual(n1.status, "aborted")
        self.assertEqual(e1.status, "aborted")
        self.assertEqual(n2.status, "aborted")


if __name__ == '__main__':
    unittest.main()
