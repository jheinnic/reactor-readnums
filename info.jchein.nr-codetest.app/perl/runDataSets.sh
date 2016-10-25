#!/bin/sh

for setName in dataSet002 dataSet003 dataSet004 dataSet005 dataSet006
do
	cat $setName/* | nc localhost 4000 &
	sleep 3
done

