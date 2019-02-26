#!/bin/bash
if [ -z $1 ] 
then
	echo "Provide instance number as a parameter!"
	exit 1
fi 

CONFIG_FILE=./config/server-$1.properties
if ! [ -e ${CONFIG_FILE} ]
then 
	echo "Can't file configuration file ${CONFIG_FILE}"
	exit 2
fi 

./bin/windows/kafka-server-start.bat ${CONFIG_FILE}

pause
