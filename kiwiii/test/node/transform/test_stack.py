#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase

from kiwiii.node.io.iterator import IteratorInput
from kiwiii.node.transform.stack import Stack


RECORDS = [
    {"id": 1, "type": "a", "value": 12.4},
    {"id": 2, "type": "b", "value": 54.23},
    {"id": 3, "type": "c", "value": 111},
    {"id": 4, "type": "a", "value": 98.0},
    {"id": 5, "type": "c", "value": None}
]


class TestStack(AsyncTestCase):
    def test_stack(self):
        n = IteratorInput(RECORDS)
        f = Stack(('id',), n.out_edges()[0])
        f.on_submitted()
        self.assertEqual(f.out_edges()[0].task_count, 10)
        f.run()
        self.assertEqual(len(list(f.out_edges()[0].records)), 10)


if __name__ == '__main__':
    unittest.main()
