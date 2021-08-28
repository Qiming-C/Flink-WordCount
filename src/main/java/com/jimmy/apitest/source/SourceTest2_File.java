package com.jimmy.apitest.source;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @ClassName: SourceTest2_File
 * @Project: flink-demo
 * @Author: qimingchen on 8/27/21
 */
public class SourceTest2_File {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        //if we want read in order set parallel to 1
        executionEnvironment.setParallelism(1);

        //get data from file
        DataStream<String> dataStream = executionEnvironment.readTextFile("/Users/qimingchen/CodeResource/flink-demo/src/main/resources/sensor.txt");

        //PRINT THE  data
        dataStream.print();


        //execute
        executionEnvironment.execute();

    }
}
