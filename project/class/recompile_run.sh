#!/bin/bash

javac -classpath `yarn classpath` -d ../class ../src/org/myorg/distributedPrime.java
jar -cvmf distributedPrime.txt distributedPrime.jar org

hadoop fs -rm file1
hadoop fs -copyFromLocal ../input/file1 file1

hadoop fs -rm -r -f  output3

hadoop jar distributedPrime.jar file1 output3
hadoop fs -cat output3/part-r-00000