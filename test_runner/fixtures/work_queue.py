import queue
import threading
from typing import Callable, Generic, List, Optional, TypeVar

T = TypeVar("T")
U = TypeVar("U")
V = TypeVar("V")


def do(nthreads: int, inputs: List[T], work_fn: Callable[[T], U]) -> List[U]:
    class Item(Generic[V]):
        _item: V

        def __init__(self, item: V):
            self._item = item

    # duplicate the tenant in remote storage
    def worker(input_queue: queue.Queue[Item[T]], output_queue: queue.Queue[U]):
        while True:
            item = input_queue.get()
            if item is None:
                return
            output = work_fn(item._item)
            output_queue.put(Item(output))

    input_queue: queue.Queue[Optional["Item[T]"]] = queue.Queue()
    output_queue: queue.Queue[Optional["Item[U]"]] = queue.Queue()
    for t in inputs:
        input_queue.put(Item(t))
    workers = []
    for _ in range(0, nthreads):
        w = threading.Thread(target=worker, args=[input_queue, output_queue])
        workers.append(w)
        w.start()
        input_queue.put(None)
    for w in workers:
        w.join()

    outputs = []
    while True:
        try:
            output = output_queue.get(block=False)
            outputs.append(output)
        except queue.Empty:
            break
    return outputs
