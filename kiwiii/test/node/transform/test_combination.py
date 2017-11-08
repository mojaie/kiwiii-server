#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase

from kiwiii.core.edge import Edge
from kiwiii.node.transform.combination import Combination


def odd(dict_):
    if dict_["value"] % 2:
        return {"value": dict_["value"]}


class TestCombination(AsyncTestCase):
    def test_combination(self):
        in_edge = Edge()
        in_edge.records = [{"value": i} for i in range(10)]
        f = Combination(in_edge)
        f.on_submitted()
        self.assertEqual(f.out_edges()[0].task_count, 45)
        f.run()
        self.assertEqual(len(list(f.out_edges()[0].records)), 45)


if __name__ == '__main__':
    unittest.main()
