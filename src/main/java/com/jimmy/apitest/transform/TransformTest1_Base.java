package com.jimmy.apitest.transform;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import javax.xml.crypto.Data;

/**
 * @ClassName: TransformTest1_Base
 * @Project: flink-demo
 * @Author: qimingchen on 8/28/21
 */
public class TransformTest1_Base {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setParallelism(1);

        //get data from file
        DataStream<String> inputdataStream = executionEnvironment.readTextFile("/Users/qimingchen/CodeResource/flink-demo/src/main/resources/sensor.txt");

        //Transform Base
        //1. map   transform String type to Integer type
        DataStream<Integer> mapStream = inputdataStream.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return s.length();
            }
        });

        //2. flatmap transform split by comma
        DataStream<String> flatMapStream = inputdataStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] fields = s.split(",");

                for ( String field: fields){
                    collector.collect(field);
                }
            }
        });

        //3. filter, filter id as sensor_1X data
        //! ATTENTION: Filter can not change the type of output stream
        DataStream<String> filterStream = inputdataStream.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {

                return s.startsWith("sensor_1");
            }
        });


        //print
        mapStream.print("map");
        flatMapStream.print("flatmap");
        filterStream.print("filter");

        //execute
        executionEnvironment.execute();
    }
}
