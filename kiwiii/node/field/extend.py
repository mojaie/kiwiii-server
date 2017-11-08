#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import functools

from kiwiii.node.function.apply import Apply


def extend(name, key, func, row):
    new_row = {name: func(key)}
    new_row.update(row)
    return new_row


class Extend(Apply):
    def __init__(self, in_edge, name, create_from, apply_func=lambda x: x,
                 field=None, params=None):
        if field is None:
            field = {"key": name}
        super().__init__(
            in_edge,
            functools.partial(extend, name, create_from, apply_func),
            fields=[field], params=params)
