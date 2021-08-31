package com.jimmy.apitest.udf;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import scala.Tuple2;

/**
 * @ClassName: com.jimmy.apitest.udf
 * @Project: flink-demo
 * @Author: qimingchen on 8/31/21
 */
public class Test1_RichFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(4);

        DataStream<String> inputStream = executionEnvironment.readTextFile("/Users/qimingchen/CodeResource/flink-demo/src/main/resources/sensor.txt");

        //convert to sensor reading
        SingleOutputStreamOperator<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");

            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        DataStream<Tuple2<String,Integer>> resultStream = dataStream.map(new MyMapper());

        resultStream.print();

        executionEnvironment.execute();


    }

    private static class MyMapper extends RichMapFunction<SensorReading, Tuple2<String,Integer>> {


        @Override
        public Tuple2<String, Integer> map(SensorReading sensorReading) throws Exception {

            return new Tuple2<>(sensorReading.getId(),getRuntimeContext().getIndexOfThisSubtask());
        }

        public MyMapper() {
            super();
        }

        @Override
        public void open(Configuration parameters) throws Exception {

            //init work,  usually for defining the state or create db connection
            System.out.println("open");
        }

        @Override
        public void close() throws Exception {

            //typically close connection and clean up
            System.out.println("close");
        }
    }
}
