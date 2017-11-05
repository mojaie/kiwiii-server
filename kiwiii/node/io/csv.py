#
# (C) 2014-2017 Seiji Matsuoka
# Licensed under the MIT License (MIT)
# http://opensource.org/licenses/MIT
#

import csv
from kiwiii.core.node import Node


class CSVFileInput(Node):
    def __init__(self, in_file, delimiter=",", fields=None, params=None):
        super().__init__()
        self.in_file = in_file
        self.delimiter = delimiter
        if fields is not None:
            self.fields.merge(fields)
        if params is not None:
            self.params.update(params)

    def on_submitted(self):
        with open(self.in_file, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            self.out_edge.records = reader
            if not self.fields:
                for field in reader.fieldnames:
                    self.out_edge.fields.merge(
                        {"key": f, "name": f, "sortType": "text"}
                        for f in field)
            self.out_edge.task_count = sum(1 for _ in f.readlines()) - 1
            self.out_edge.params.update(self.in_edge.params)

    def in_edges(self):
        return tuple()
