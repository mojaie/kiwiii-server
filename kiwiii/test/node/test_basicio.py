#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import unittest

from tornado.testing import AsyncTestCase

from kiwiii.node.basicio import IteratorInput


class TestBasicIO(AsyncTestCase):
    def test_iterator_input(self):
        f = IteratorInput(range(100))
        f.run()
        self.assertEqual(f.out_edges()[0].task_count, 100)
        self.assertEqual(sum(f.out_edges()[0].records), 4950)


if __name__ == '__main__':
    unittest.main()
