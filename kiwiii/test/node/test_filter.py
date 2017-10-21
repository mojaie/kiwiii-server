#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.filter import Filter, MPFilter
from kiwiii.core.node import AsyncNode, LazyConsumer
from kiwiii.core.edge import Edge


def odd(dict_):
    if dict_["value"] % 2:
        return {"value": dict_["value"]}


class TestFilter(AsyncTestCase):
    def test_filter(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        f = Filter(odd, in_edge)
        f.on_submitted()
        f.run()
        total = sum(a["value"] for a in f.out_edges()[0].records)
        self.assertEqual(total, 25)
        self.assertEqual(f.status, "done")

    @gen_test
    def test_mpfilter(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(100)]
        in_edge.task_count = 100
        n1 = MPFilter(odd, in_edge)
        n2 = AsyncNode(n1.out_edges()[0])
        n2.interval = 0.01
        n1.on_submitted()
        n2.on_submitted()
        self.assertEqual(n2.out_edges()[0].task_count, 100)
        n1.run()
        self.assertEqual(n1.status, "running")
        n2.run()
        res = []
        while n2.in_edges()[0].status != "done":
            r = yield n2.out_edges()[0].get()
            res.append(r)
        self.assertEqual(n2.out_edges()[0].done_count, 100)
        self.assertEqual(sum(a["value"] for a in res), 2500)

    @gen_test
    def test_interrupt(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(100)]
        n1 = MPFilter(odd, in_edge)
        n2 = LazyConsumer(n1.out_edges()[0])
        n2.interval = 0.01
        n1.run()
        n2.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n2.status, "aborted")
        self.assertGreater(len(n2.records), 0)


if __name__ == '__main__':
    unittest.main()
