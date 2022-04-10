#!/bin/bash

if [ "$1" == "--help" ] ; then
    echo "    First parameter should be the absolute path to input file."
    echo
    echo "    Second parameter should be the absolute path to output file."
    echo
    echo "    Format: ./simulateEventRate.sh <absoultePathToInputFile> <absoultePathToOutputFile>"
    exit 0
fi

pathOfInputFile=$1
pathOfOutputFile=$2
lengthOfFile=$(wc -l < $pathOfInputFile)
step=$((lengthOfFile / 10))

echo "Input File: ${pathOfInputFile}"
echo "Output File: ${pathOfOutputFile}"
echo "Length Of Input File: ${lengthOfFile}"
echo "Step: ${step}"
echo

if [ -f "$pathOfOutputFile" ] ; then
    echo fi
    #rm "$pathOfOutputFile"
fi
#touch $pathOfOutputFile

echo $pathOfOutputFile

#tail -n0 -F  | ../bin/kafka-console-producer.sh --broker-list localhost:9092 --topic 100ktopic &

echo "Current time: $(date '+%d/%m/%Y %H:%M:%S')"

for ((i=0;i<=9;i++))
do
    startLine=$((1+$(($i*$step))))
    endLine=$(($((1+$i*$step))+$step-1))
    echo "$i: $startLine -> $endLine"

    sed -n $startLine,$((endLine))p $pathOfInputFile >> $pathOfOutputFile
    currentLengthOfOutputFile=$(wc -l < $pathOfOutputFile)
    echo "Current Length Of Output File: ${currentLengthOfOutputFile}"
    echo

    sleep 0.09
done

echo "Current time: $(date '+%d/%m/%Y %H:%M:%S')"
