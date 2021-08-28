package com.jimmy.apitest.source;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @ClassName: SourceTest1_Collection
 * @Project: flink-demo
 * @Author: qimingchen on 8/26/21
 */
public class SourceTest1_Collection {

    public static void main(String[] args) throws Exception {

        //ccreate env instance
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //read from collection
        DataStream<SensorReading> sensorReadingDataStreamSource = executionEnvironment.fromCollection(Arrays.asList(
                new SensorReading("sensor_1", 1547718199L, 35.8),
                new SensorReading("sensor_6", 1547718201L, 15.4),
                new SensorReading("sensor_10", 1547718202L, 6.7),
                new SensorReading("sensor_7", 1547718205L, 38.1)
        ));

        //read from element
        DataStream<Integer> integerDataStreamSource = executionEnvironment.fromElements(1,4,6,6654,2,2312);

        //print to std
        sensorReadingDataStreamSource.print("data");
        integerDataStreamSource.print("int");

        //execute
        executionEnvironment.execute("SensorReading");
    }
}
