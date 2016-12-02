#!/bin/bash


CLASSNAME=rabbit_test.ManagerRunner
LIB=../../lib
RABBIT=$LIB/rabbitmq-client-3.6.5.jar
BIN=../../bin
CLASS_PATH_PARAMS=$BIN:$RABBIT:$CLASSPATH

$JAVA_HOME/bin/java -cp $CLASS_PATH_PARAMS $CLASSNAME $1 $2