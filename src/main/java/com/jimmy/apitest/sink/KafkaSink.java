package com.jimmy.apitest.sink;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

/**
 * @ClassName: com.jimmy.apitest.sink
 * @Project: flink-demo
 * @Author: qimingchen on 9/8/21
 */
public class KafkaSink {


    public static void main(String[] args) throws Exception {

        //exec env
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //set parallelism to 1
        executionEnvironment.setParallelism(1);

        //config properteis for kafka
        Properties properties = new Properties();
        //kafka address
        properties.put("bootstrap.servers","localhost:9092");

        //consumer group even there is only one consumer
        properties.put("group.id","flink-demo");

        //zookeeper timeout interval
        properties.put("zookeeper.session.timeout.ms","400");

        //when consumer first consuming, consume from smallest offset value
        properties.put("auto.offset.reset","earliest");

        //auto commit the offset
        properties.put("auto.commit.enable","true");

        //consumer auto commit offset interval
        //this should be really important as data could be lost or duplicates
        properties.put("auto.commit.interval.ms","1000");

        //serializer
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");


        //read from Kafka producer
        DataStreamSource<String> inputStream = executionEnvironment.addSource(new FlinkKafkaConsumer<String>("sensor", new SimpleStringSchema(), properties));


        //serialize the data from producer
        SingleOutputStreamOperator<String> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2])).toString();
        });

        //write this serialized data back to kafka
        dataStream.addSink(new FlinkKafkaProducer<String>("localhost:9092","sinktest", new SimpleStringSchema()));

        //exe env
        executionEnvironment.execute();
    }
}
