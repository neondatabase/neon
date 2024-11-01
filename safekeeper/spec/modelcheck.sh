#!/bin/bash

# Usage: ./modelcheck.sh <config_file> <spec_file>, e.g.
# ./modelcheck.sh models/MCProposerAcceptorStatic_p2_a3_t3_l3.cfg MCProposerAcceptorStatic.tla
CONFIG=$1
SPEC=$2

MEM=7G

mkdir -p "tlc-results"
outfilename="$SPEC-$(date --utc +%Y-%m-%d--%H-%M-%S)".log
outfile="tlc-results/$outfilename"
touch $outfile

# Save some info about the run.
GIT_REV=`git rev-parse --short HEAD`
INFO=`uname -a`

# First for Linux, second for Mac.
CPUNAMELinux=`grep -m 1 'model name' /proc/cpuinfo`
CPUCORESLinux=`nproc`
CPUNAMEMac=`sysctl -n machdep.cpu.brand_string`
CPUCORESMac=`sysctl -n machdep.cpu.thread_count`

echo "git revision: $GIT_REV" >> $outfile
echo "Platform: $INFO" >> $outfile
echo "CPU Info Linux: $CPUNAMELinux" >> $outfile
echo "CPU Cores Linux: $CPUCORESLinux" >> $outfile
echo "CPU Info Mac: $CPUNAMEMac" >> $outfile
echo "CPU Cores Mac: $CPUCORESMac" >> $outfile
echo "Spec: $SPEC" >> $outfile
echo "Config: $CONFIG" >> $outfile
echo "----" >> $outfile
cat $CONFIG >> $outfile
echo "" >> $outfile
echo "----" >> $outfile
echo "" >> $outfile

# see
# https://lamport.azurewebsites.net/tla/current-tools.pdf
# for TLC options.
# OffHeapDiskFPSet is the optimal fingerprint set implementation
# https://docs.tlapl.us/codebase:architecture#fingerprint_sets_fpsets
#
# Add -simulate to run in infinite simulation mode.
java -Xmx$MEM -XX:MaxDirectMemorySize=$MEM -XX:+UseParallelGC -Dtlc2.tool.fp.FPSet.impl=tlc2.tool.fp.OffHeapDiskFPSet \
  -cp /opt/TLA+Toolbox/tla2tools.jar tlc2.TLC $SPEC -config $CONFIG -workers auto -gzip | tee -a $outfile