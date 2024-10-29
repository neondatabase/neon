#!/bin/bash

CFG=models/MCProposerAcceptorStatic_p2_a3_t2_l2.cfg
SPEC=MCProposerAcceptorStatic.tla
MEM=7G

# see
# https://lamport.azurewebsites.net/tla/current-tools.pdf
# for TLC options.
# OffHeapDiskFPSet is the optimal fingerprint set implementation
# https://docs.tlapl.us/codebase:architecture#fingerprint_sets_fpsets
# add -simulate to run in infinite simulation mode
java -Xmx$MEM -XX:MaxDirectMemorySize=$MEM -XX:+UseParallelGC -Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet \
  -cp /opt/TLA+Toolbox/tla2tools.jar tlc2.TLC $SPEC -config $CFG -workers 1 -gzip