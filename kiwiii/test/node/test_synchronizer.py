#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase, gen_test

from kiwiii.node.synchronizer import Synchronizer, Asynchronizer
from kiwiii.task import Edge


class TestSynchronizer(AsyncTestCase):
    @gen_test
    def test_synchronizer(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        a = Asynchronizer(in_edge)
        s = Synchronizer(a.out_edges()[0])
        a.run()
        self.assertEqual(a.status, "running")
        self.assertEqual(s.status, "ready")
        yield s.run()
        self.assertEqual(a.status, "done")
        self.assertEqual(s.status, "done")


if __name__ == '__main__':
    unittest.main()
