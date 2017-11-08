#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.function.number import Number, AsyncNumber
from kiwiii.core.node import LazyConsumer, Synchronizer, Asynchronizer
from kiwiii.core.edge import Edge


class TestNumber(AsyncTestCase):
    def test_number(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        f = Number(in_edge, name="num")
        f.run()
        total = sum(a["num"] for a in f.out_edges()[0].records)
        self.assertEqual(total, 45)
        self.assertEqual(f.status, "done")

    @gen_test
    def test_asyncnumber(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        n1 = Asynchronizer(in_edge)
        n2 = AsyncNumber(n1.out_edges()[0], name="num")
        n3 = Synchronizer(n2.out_edges()[0])
        n2.interval = 0.01
        n3.interval = 0.01
        n1.run()
        n2.run()
        yield n3.run()
        self.assertEqual(n1.status, "done")
        self.assertEqual(n2.status, "done")
        self.assertEqual(n3.status, "done")
        total = sum(r["num"] for r in n3.out_edges()[0].records)
        self.assertEqual(total, 45)

    @gen_test
    def test_interrupt(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(100)]
        n1 = Asynchronizer(in_edge)
        n2 = AsyncNumber(n1.out_edges()[0], name="num")
        n3 = LazyConsumer(n2.out_edges()[0])
        n2.interval = 0.01
        n3.interval = 0.01
        n1.run()
        n2.run()
        n3.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n3.status, "aborted")
        self.assertGreater(len(n3.records), 0)


if __name__ == '__main__':
    unittest.main()
