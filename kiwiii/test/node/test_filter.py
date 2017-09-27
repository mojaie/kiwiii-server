#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.filter import Filter, MPFilter
from kiwiii.node.synchronizer import Synchronizer
from kiwiii.task import Edge


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
        f = MPFilter(twice, in_edge)
        sync = Synchronizer(f.out_edges()[0])
        sync.interval = 0.05
        f.run()
        self.assertEqual(f.status, "running")
        yield sync.run()
        total = sum(a["value"] for a in sync.out_edges()[0].records)
        self.assertEqual(total, 90)
        self.assertEqual(f.status, "done")


if __name__ == '__main__':
    unittest.main()
