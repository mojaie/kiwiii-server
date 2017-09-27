
from tornado import gen

from kiwiii.task import MPWorker, Node, AsyncNode, Edge, AsyncQueueEdge


def twice(dict_):
    return {"value": dict_["value"] * 2}


class MPWorkerResults(MPWorker):
    def __init__(self, *args):
        super().__init__(*args)
        self.results = []

    def on_task_done(self, res):
        self.results.append(res)


class FlashNode(Node):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = Edge()

    def run(self):
        self.on_finish()

    def out_edges(self):
        return (self.out_edge,)


class IdleNode(AsyncNode):
    def __init__(self, in_edge):
        super().__init__()
        self.in_edge = in_edge
        self.out_edge = AsyncQueueEdge()

    @gen.coroutine
    def run(self):
        self.on_start()
        while 1:
            if self.status == "interrupted":
                self.on_aborted()
                break
            yield gen.sleep(self.interval)

    def out_edges(self):
        return (self.out_edge,)
