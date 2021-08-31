package com.jimmy.apitest.transform;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @ClassName: com.jimmy.apitest.transform
 * @Project: flink-demo
 * @Author: qimingchen on 8/30/21
 * @Description: spliiting stream into splitStream by certain characteristic
 * ! here we split into high temp and low temp sensor group
 */
public class TransformTest4_Split {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //read data from file
        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/qimingchen/CodeResource/flink-demo/src/main/resources/sensor.txt");

        //convert to sensorReading type
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));

        });

        //1. split by 30 temp as threshold

        final OutputTag<SensorReading> low = new OutputTag<SensorReading>("low"){};
        final OutputTag<SensorReading> high = new OutputTag<SensorReading>("high"){};


        SingleOutputStreamOperator<Object> singleOutputStreamOperator = dataStream.process(new ProcessFunction<SensorReading, Object>() {

            @Override
            public void processElement(SensorReading sensorReading, ProcessFunction<SensorReading, Object>.Context context, Collector<Object> collector) throws Exception {

                collector.collect(sensorReading);

                if (sensorReading.getTemperature() < 30) {
                    context.output(low, sensorReading);
                } else {
                    context.output(high, sensorReading);
                }
            }
        });

        DataStream<SensorReading> lowSideOutput = singleOutputStreamOperator.getSideOutput(low);
        DataStream<SensorReading> highSideOutput = singleOutputStreamOperator.getSideOutput(high);

        lowSideOutput.print("low");
        highSideOutput.print("high");
        singleOutputStreamOperator.print("all");

        //execute
        executionEnvironment.execute();
    }
}
