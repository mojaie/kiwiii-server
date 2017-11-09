#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.function.filter import Filter, MPFilter
from kiwiii.node.io.iterator import IteratorInput
from kiwiii.core.node import AsyncNode, LazyConsumer


def odd(dict_):
    if dict_["value"] % 2:
        return {"value": dict_["value"]}


class TestFilter(AsyncTestCase):
    def test_filter(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        iter_in.submit()
        filter_ = Filter(odd)
        filter_.add_in_edge(iter_in.out_edge())
        filter_.submit()
        total = sum(a["value"] for a in filter_.out_edge().records)
        self.assertEqual(total, 25)
        self.assertEqual(filter_.status, "done")

    @gen_test
    def test_mpfilter(self):
        iter_in = IteratorInput({"value": i} for i in range(10))
        mpf = MPFilter(odd)
        asyn = AsyncNode()
        mpf.interval = 0.01
        iter_in.submit()
        mpf.submit()
        asyn.submit()
        self.assertEqual(mpf.out_edge().task_count, 100)
        iter_in.run()
        self.assertEqual(iter_in.status, "running")
        mpf.run()
        res = []
        while mpf.in_edges.status != "done":
            r = yield n2.out_edge().get()
            res.append(r)
        self.assertEqual(n2.out_edge().done_count, 100)
        self.assertEqual(sum(a["value"] for a in res), 2500)

    @gen_test
    def test_interrupt(self):
        n = IteratorInput({"value": i} for i in range(100))
        n1 = MPFilter(odd, n.out_edges()[0])
        n2 = LazyConsumer(n1.out_edges()[0])
        n2.interval = 0.01
        n.on_submitted()
        n1.on_submitted()
        n2.on_submitted()
        n.run()
        n1.run()
        n2.run()
        yield n1.interrupt()
        yield gen.sleep(0.1)
        self.assertEqual(n2.status, "aborted")
        self.assertGreater(len(n2.records), 0)


if __name__ == '__main__':
    unittest.main()
