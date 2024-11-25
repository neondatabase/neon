The specifications, models and results of running of them of the compute <->
safekeepers consensus algorithm for committing WAL on the fleet of safekeepers.
Following Paxos parlance, compute which writes WAL is called (WAL) proposer here
and safekeepers which persist it are called (WAL) acceptors.

Directory structure:
- Use modelcheck.sh to run TLC.
- MC*.tla contains bits of TLA+ needed for TLC like constraining the state space, and models/ actual models.
- Other .tla files are the actual specs.

Structure is partially borrowed from
[logless-reconfig](https://github.com/will62794/logless-reconfig), thanks to it.
