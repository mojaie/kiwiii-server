#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado import gen
from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.numbergenerator import NumberGenerator, AsyncNumberGenerator
from kiwiii.node.synchronizer import Synchronizer, Asynchronizer
from kiwiii.task import Edge


def twice(dict_):
    return {"value": dict_["value"] * 2}


@unittest.skip("")
class TestNumberGenerator(AsyncTestCase):
    def test_numbergenerator(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        f = NumberGenerator(in_edge, name="num")
        f.run()
        total = sum(a["num"] for a in f.out_edges()[0].records)
        self.assertEqual(total, 45)
        self.assertEqual(f.status, "done")

    @gen_test
    def test_asyncnumbergenerator(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        a = Asynchronizer(in_edge)
        g = AsyncNumberGenerator(a.out_edges()[0], name="num")
        sync = Synchronizer(g.out_edges()[0])
        a.run()
        g.run()
        sync.run()
        while 1:
            print(a.status, g.status, sync.status)
            yield gen.sleep(0.2)
        total = sum(r["num"] for r in sync.out_edges()[0].records)
        self.assertEqual(total, 45)
        self.assertEqual(g.status, "done")


if __name__ == '__main__':
    unittest.main()
