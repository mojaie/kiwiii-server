#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.filter import Filter, MPFilter
from kiwiii.core.node import Synchronizer, LazyConsumer
from kiwiii.core.edge import Edge


def twice(dict_):
    return {"value": dict_["value"] * 2}


class TestFilter(AsyncTestCase):
    def test_filter(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        f = Filter(twice, in_edge)
        f.run()
        total = sum(a["value"] for a in f.out_edges()[0].records)
        self.assertEqual(total, 90)
        self.assertEqual(f.status, "done")

    @gen_test
    def test_mpfilter(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        n1 = MPFilter(twice, in_edge)
        n2 = Synchronizer(n1.out_edges()[0])
        n2.interval = 0.01
        n1.run()
        self.assertEqual(n1.status, "running")
        yield n2.run()
        total = sum(a["value"] for a in n2.out_edges()[0].records)
        self.assertEqual(total, 90)
        self.assertEqual(n2.status, "done")

    @gen_test
    def test_interrupt(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(100)]
        n1 = MPFilter(twice, in_edge)
        n2 = LazyConsumer(n1.out_edges()[0])
        n2.interval = 0.01
        n1.run()
        n2.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n2.status, "aborted")
        self.assertGreater(len(n2.out_edges()[0].records), 0)


if __name__ == '__main__':
    unittest.main()
