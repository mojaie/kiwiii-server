#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import operator

from kiwiii.node.function.filter import Filter


class FilterRecords(Filter):
    def __init__(self, in_edge, key, value, filter_operator=operator.eq,
                 params=None):
        super().__init__(
            in_edge,
            lambda x: filter_operator(x[key], value),
            params=params
        )
