#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.core.node import (
    Synchronizer, Asynchronizer, AsyncNode, LazyConsumer, EagerProducer)
from kiwiii.core.edge import Edge


class TestNode(AsyncTestCase):
    @gen_test
    def test_synchronizer(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        n1 = Asynchronizer(in_edge)
        n2 = AsyncNode(n1.out_edges()[0])
        n3 = Synchronizer(n2.out_edges()[0])
        n2.interval = 0.01
        n3.interval = 0.01
        n1.run()
        n2.run()
        yield n3.run()
        self.assertEqual(n1.status, "done")
        self.assertEqual(n2.status, "done")
        self.assertEqual(n3.status, "done")

    @gen_test
    def test_interrupt(self):
        n1 = EagerProducer()
        n2 = AsyncNode(n1.out_edges()[0])
        n3 = LazyConsumer(n2.out_edges()[0])
        n2.interval = 0.01
        n3.interval = 0.01
        n1.run()
        n2.run()
        n3.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n3.status, "aborted")


if __name__ == '__main__':
    unittest.main()
