# TODO

- If the key space can be perfectly partitioned at some key, perform planning on each
  partition separately. For example, if we are compacting a level with layers like this:

  ```
              :
  +--+ +----+ :  +------+
  |  | |    | :  |      |
  +--+ +----+ :  +------+
              :
  +-----+ +-+ : +--------+
  |     | | | : |        |
  +-----+ +-+ : +--------+
              :
  ```

  At the dotted line, there is a natural split in the key space, such that all
  layers are either on the left or the right of it. We can compact the
  partitions separately.  We could choose to create image layers for one
  partition but not the other one, for example.

- All the layers don't have to be exactly the same size, we can choose to cut a
  layer short or stretch it a little larger than the target size, if it helps
  the overall system. We can help perfect partitions (see previous bullet point)
  to happen more frequently, by choosing the cut points wisely. For example, try
  to cut layers at boundaries of underlying image layers. And "snap to grid",
  i.e. don't cut layers at any key, but e.g. only when key % 10000 = 0.

- Avoid rewriting layers when we'd just create an identical layer to an input
  layer.

- Parallelism. The code is already split up into planning and execution, so that
  we first split up the compaction work into "Jobs", and then execute them.
  It would be straightforward to execute multiple jobs in parallel.

- Materialize extra pages in delta layers during compaction. This would reduce
  read amplification. There has been the idea of partial image layers. Materializing
  extra pages in the delta layers achieve the same goal, without introducing a new
  concept.

## Simulator

- Expand the simulator for more workloads
- Automate a test suite that runs the simluator with different workloads and
  spits out a table of results
- Model read amplification
- More sanity checking. One idea is to keep a reference count of each
  MockRecord, i.e. use Arc<MockRecord> instead of plain MockRecord, and panic if
  a MockRecord that is newer than PITR horizon is completely dropped. That would
  indicate that the record was lost.
