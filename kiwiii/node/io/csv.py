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

    def inspect(self):
        with open(self.in_file, newline="") as f:
            count = sum(1 for _ in f.readlines()) - 1
        with open(self.in_file, newline="") as f:
            reader = csv.DictReader(f, delimiter=self.delimiter)
            try:
                next(reader)
            except StopIteration:
                pass
            fieldnames = reader.fieldnames
        return fieldnames, count

    def reader(self):
        with open(self.in_file, newline="") as f:
            for row in csv.DictReader(f, delimiter=self.delimiter):
                yield row

    def on_submitted(self):
        # TODO: a bit tricky
        self.out_edge.records = self.reader()
        fnames, count = self.inspect()
        if not self.fields:
            self.out_edge.fields.merge(
                {"key": f, "name": f, "sortType": "text"} for f in fnames)
        else:
            self.out_edge.fields.merge(self.fields)
        self.out_edge.task_count = count
        self.out_edge.params.update(self.params)

    def in_edges(self):
        return tuple()
