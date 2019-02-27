#!/bin/bash
if [ -z $1 ] 
then
	echo "Provide instance number as a parameter!"
	exit 1
fi 

CONFIG_FILE=./config/server-$1.properties
LOG_DIR=./logs/instance$1/

if ! [ -e ${CONFIG_FILE} ]
then 
	echo "Can't file configuration file ${CONFIG_FILE}"
	exit 2
fi 

if ! [ -d ${LOG_DIR} ]
then
	echo Creating directory for log files: ${LOG_DIR}
	mkdir -p ${LOG_DIR}
fi

LOG_DIR=${LOG_DIR} ./bin/windows/kafka-server-start.bat ${CONFIG_FILE}

