#!/bin/bash

# Usage: ./modelcheck.sh <config_file> <spec_file>, e.g.
# ./modelcheck.sh models/MCProposerAcceptorStatic_p2_a3_t3_l3.cfg MCProposerAcceptorStatic.tla
CONFIG=$1
SPEC=$2

MEM=7G

# see
# https://lamport.azurewebsites.net/tla/current-tools.pdf
# for TLC options.
# OffHeapDiskFPSet is the optimal fingerprint set implementation
# https://docs.tlapl.us/codebase:architecture#fingerprint_sets_fpsets
#
# Add -simulate to run in infinite simulation mode.
java -Xmx$MEM -XX:MaxDirectMemorySize=$MEM -XX:+UseParallelGC -Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet \
  -cp /opt/TLA+Toolbox/tla2tools.jar tlc2.TLC $SPEC -config $CONFIG -workers auto -gzip