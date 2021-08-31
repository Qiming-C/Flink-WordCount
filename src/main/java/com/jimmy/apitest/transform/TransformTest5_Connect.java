package com.jimmy.apitest.transform;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import scala.Tuple2;

/**
 * @ClassName: com.jimmy.apitest.transform
 * @Project: flink-demo
 * @Author: qimingchen on 8/31/21
 */
public class TransformTest5_Connect {
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

        //connect two streams into one stream
        //1. Convert high temp into tuple type,  when connect to low temp stream, output the status info
        SingleOutputStreamOperator<Tuple2<String, Double>> warningStream = highSideOutput.map(new MapFunction<SensorReading, Tuple2<String, Double>>() {


            @Override
            public Tuple2<String, Double> map(SensorReading sensorReading) throws Exception {
                return new Tuple2<>(sensorReading.getId(), sensorReading.getTemperature());
            }
        });


        ConnectedStreams<Tuple2<String, Double>, SensorReading> connectedStream = warningStream.connect(lowSideOutput);

        SingleOutputStreamOperator<Object> resultedStream = connectedStream.map(new CoMapFunction<Tuple2<String, Double>, SensorReading, Object>() {

            @Override
            public Object map1(Tuple2<String, Double> stringDoubleTuple2) throws Exception {

                return new Tuple3<>(stringDoubleTuple2._1, stringDoubleTuple2._2, "high temp warning");
            }

            @Override
            public Object map2(SensorReading sensorReading) throws Exception {

                return new Tuple2<>(sensorReading.getId(), "normal");
            }
        });

        //print
        resultedStream.print();
        //execute
        executionEnvironment.execute();
    }
}
