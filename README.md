# Simple-Kafka-Spark-Streaming

JAVA_HOME=C:\Program Files\Java\jre1.8.0_121

SPARK_HOME=C:\somewhere\spark-2.1.0-bin-hadoop2.7

HADOOP_HOME=C:\somewhere\hadoop-2.7.3

4.3 Append below variable into "Path"

%SPARK_HOME%\bin

%HADOOP_HOME%\bin

Set hadoop environment
If you're using windows, you could download winutils from https://github.com/steveloughran/winutils

Go to c:\somewhere\spark-2.1.0-bin-hadoop2.7\bin\
Paste winutils.exe

The scenario from this repo is get data from kafka topic using Spark Streaming
