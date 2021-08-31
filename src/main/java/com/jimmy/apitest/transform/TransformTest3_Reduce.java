package com.jimmy.apitest.transform;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: com.jimmy.apitest.transform
 * @Project: flink-demo
 * @Author: qimingchen on 8/30/21
 * @Description: we want to use reduce so that the sensor no only return the meax temp but also the latest timestamp
 */
public class TransformTest3_Reduce {

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

        //reduce
        sensorReadingStringKeyedStream.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(),t1.getTimestamp(),Math.max(sensorReading.getTimestamp(),t1.getTemperature()));
            }
        });

        //or lambda expression
        SingleOutputStreamOperator<SensorReading> reduce = sensorReadingStringKeyedStream.reduce((currentState, newData) -> new SensorReading(currentState.getId(), newData.getTimestamp(), Math.max(currentState.getTemperature(), newData.getTemperature())));

        reduce.print();

        executionEnvironment.execute();

    }
}