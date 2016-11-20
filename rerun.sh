#!/bin/bash

export CLASSPATH=$(hadoop classpath)
export HADOOP_CLASSPATH=$CLASSPATH

rm -rf /user/user01/OUT_LATIN
hadoop jar DCacheJoin.jar DCacheJoin.DCacheJoinDictionaryDriver -files /user/user01/INPUT_LATIN/latin.txt /user/user01/OUT/part-r-00000 /user/user01/OUT_LATIN
