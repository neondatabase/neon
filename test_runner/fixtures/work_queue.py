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

    work_fn_raised = threading.Event()

    # duplicate the tenant in remote storage
    def worker(input_queue: queue.Queue[Item[T]], output_queue: queue.Queue[U]):
        while True:
            item = input_queue.get()
            if item is None:
                return
            try:
                output = work_fn(item._item)
            except Exception:
                work_fn_raised.set()
                raise
            output_queue.put(output)

    input_queue: queue.Queue[Optional["Item[T]"]] = queue.Queue()
    output_queue: queue.Queue[U] = queue.Queue()
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

    if work_fn_raised.is_set():
        raise Exception("one of the work_fn's raised an exception, don't do that")

    outputs = []
    while True:
        try:
            output = output_queue.get(block=False)
            outputs.append(output)
        except queue.Empty:
            break
    return outputs
