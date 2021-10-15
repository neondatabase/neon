#!/bin/bash

while true; do
  echo -n "==== CURRENT TIME:" >> /tmp/test_output/netstat.stdout
  date +"%T.%N" >> /tmp/test_output/netstat.stdout
  sudo netstat -vpnoa | grep tcp | sort >> /tmp/test_output/netstat.stdout
  sleep 0.5
done
