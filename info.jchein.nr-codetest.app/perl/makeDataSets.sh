#!/bin/sh

for setDef in dataSet002:182 dataSet003:814 dataSet004:486 dataSet005:812 dataSet006:773
do
	setName=`echo $setDef | awk -F: '{print $1}'`
	setSeed=`echo $setDef | awk -F: '{print $2}'`
	# mkdir $setName
	echo "$setSeed\n5000000000\n" | perl ./logRandom.pl | (cd $setName; split -l 1000000 -a 3 - file) &
done

