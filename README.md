# Flink-WordCount

This program consist of two types of data processing demo

`WordCount.java` uses batch processing to process word count

`StreamWordCount.java` uses stream processing to process word count as unbounded stream

Using netCat simulates real-time data stream

before running the program make sure you  have `netcat` installed then run <br/>
`nc -lk <port>`

once netcat upon running, config the application CLI arguments
if you are using IDEA, config command line arguments to `--host localhost --port portnumber` then run the application

Finally, you can type any word at terminal then see the processing result in the console of application


### Set datasource from Kafka

make sure zookeeper and kafka server is up running 

Run <br/>
`./kafka-console-producer.sh --broker-list localhost:9092 --topic sensor`

then run the application, then we can use producer console to produce data where flink is processing the data from kafka


### Other Directory 
otherthan the wordcount program, there are many example of streamApi code in the corresponding folder
