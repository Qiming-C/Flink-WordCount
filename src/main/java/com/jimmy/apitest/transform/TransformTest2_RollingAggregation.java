package com.jimmy.apitest.transform;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.common.metrics.Sensor;

/**
 * @ClassName: TransformTest2_RollingAggregation
 * @Project: flink-demo
 * @Author: qimingchen on 8/29/21
 */
public class TransformTest2_RollingAggregation {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //read data from file
        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/qimingchen/CodeResource/flink-demo/src/main/resources/sensor.txt");

        //transform to sensorr
//        DataStream<SensorReading> dataStream = inputStream.map(new MapFunction<String, SensorReading>() {
//            @Override
//            public SensorReading map(String s) throws Exception {
//                String[] fields = s.split(",");
//
//                return new SensorReading(fields[0],new Long(fields[1]),new Double(fields[2]));
//
//            }
//        });

        //* above can change to lambda expression
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));

        });

        //grouping
        //! we are not able to keyBY non-tuple data
        KeyedStream<SensorReading, String> sensorReadingStringKeyedStream = dataStream.keyBy(SensorReading::getId);

        //aggregating, get max temp from group
        SingleOutputStreamOperator<SensorReading> maxTemperature = sensorReadingStringKeyedStream.max("temperature");

        //PRINT
        maxTemperature.print();

        //execute
        executionEnvironment.execute();


    }
}
