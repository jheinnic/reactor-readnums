#!/bin/sh

# start perl runFromLists.pl "Source 1" ./dataSet01/ 258 1000000 | tee result01.log &
# sleep 6
start perl runFromLists.pl "Source 2" ./dataSet02/ 258 1000000 | tee result02.log &
# sleep 6
# start perl runFromLists.pl "Source 3" ./dataSet03/ 258 1000000 | tee result03.log &
sleep 6
start perl runFromLists.pl "Source 4" ./dataSet04/ 258 1000000 | tee result04.log &
# sleep 6
# start perl runFromLists.pl "Source 5" ./dataSet05/ 258 1000000 | tee result05.log &
