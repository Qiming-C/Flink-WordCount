package com.jimmy.apitest.source;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;


/**
 * @ClassName: Source_Text3_Kafka
 * @Project: flink-demo
 * @Author: qimingchen on 8/27/21
 */
public class SourceText3_Kafka {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        //get data from kafka
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


        DataStream<String> dataStream = executionEnvironment.addSource( new FlinkKafkaConsumer<String>("sensor",new SimpleStringSchema(),properties));

        dataStream.print();

        executionEnvironment.execute();
    }

}
