#!/bin/bash
if [ -z $1 ] 
then
	echo "No consumer group name!"
	exit 1
fi
while true 
do
	./bin/windows/kafka-consumer-groups.bat --bootstrap-server localhost:9092 --group $1 --offsets --describe 
	sleep 5
done
