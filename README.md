# Distributed Prime Number Calculator
Hadoop version 2.5.2

#How to run
1. cd to project/class/
2. Compile: 
`javac -classpath `yarn classpath` -d ../class ../src/org/myorg/distributedPrime.java`
3. Create jar:
jar -cvmf distributedPrime.txt distributedPrime.jar org
4. Upload input File:
hadoop fs -rm file1 
hadoop fs -copyFromLocal ../input/file1 file1
5. Clear previous output:
hadoop fs -rm -r -f  output3
6. Run jar:
hadoop jar distributedPrime.jar file1 output3
hadoop fs -cat output3/part-r-00000
