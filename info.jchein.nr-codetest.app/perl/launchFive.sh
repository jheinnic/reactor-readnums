#!/bin/sh

start perl runFromList.pl "Source 1" ../node/0 60000000 5 10000 | tee result01.log &
start perl runFromList.pl "Source 2" ../node/1 60000000 5 10000 | tee result02.log &
start perl runFromList.pl "Source 3" ../node/2 60000000 5 10000 | tee result03.log &
start perl runFromList.pl "Source 4" ../node/3 60000000 5 10000 | tee result04.log &
start perl runFromList.pl "Source 5" ../node/4 60000000 5 10000 | tee result05.log &
