#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import json
import unittest

from chorus.demo import MOL
from chorus import v2000reader as reader
from chorus.model.graphmol import Compound

from kiwiii import static
from kiwiii.node.chemdata import chem_data


class TestChemdata(unittest.TestCase):
    def test_chemdata(self):
        in_ = reader.mol_from_text(MOL["demo"])
        row = {
            static.MOLOBJ_KEY: json.dumps(in_.jsonized())
        }
        record = chem_data(row)
        out = Compound(json.loads(record[static.MOLOBJ_KEY]))
        self.assertEqual(str(out), str(in_))
        self.assertAlmostEqual(record["_mw"], 754.7)


if __name__ == '__main__':
    unittest.main()
