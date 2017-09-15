#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import itertools

from tornado import gen
from kiwiii.node import Node
from kiwiii.util import graph


class MPFilter(Node):
    def __init__(self, func, parent, tasktree):
        self.tasktree = tasktree
        self.func = func
        self.parent = parent
        self.children = []
        self.output = []

    def parent_output(self):
        pass

    @gen.coroutine
    def run(self):
        args = self.parent_output()
        worker = Worker(zip(args), self.func)
        yield worker.run()
