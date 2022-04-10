# flinkExercise
Apache Flink/Kafka assignment

Run this command to observe the file which is being written:
```
tail -n0 -F <theFileToBeObserved> | ../bin/kafka-console-producer.sh --broker-list localhost:9092 --topic 100ktopic
```

Run this command to start the script simulating the event rate:
```
./simulateEventRate.sh <absoultePathToInputFile> <absoultePathToOutputFile>
```
