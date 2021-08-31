package com.jimmy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @ClassName: StreamWordCount
 * @Project: flink-demo
 * @Author: qimingchen on 8/23/21
 */
public class StreamWordCount {

    public static void main(String[] args) throws Exception {

        //config a streaming execution env
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //read data from file
//        String inputPath = "src/main/resources/hello.txt";
//        DataStreamSource<String> inputDataStream = executionEnvironment.readTextFile(inputPath);

        // using tool from flink: parametertool to get config when program start
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        //read file from socket
        DataStream<String> inputDataStream = executionEnvironment.socketTextStream(host, port);

        //bases stream compute
        //the FlatMapper is essentially the same one, so we can use the previous custom mapper
        //in stream data, there is NO GROUPBY SINCE IT IS UNBOUNDED STREAM, GROUPBY IS GROUPING SET OF DATA
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = inputDataStream.flatMap(new MyFlatMapper())
                .keyBy(value -> value.f0)
                .sum(1)
                .setParallelism(2);

        //this print will not work unless we execute the task, not like batch processing
        result.print().setParallelism(1);

        //execute the task
        executionEnvironment.execute();

    }

    //custom class, implement FlatMapFunction interface
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //split word by whitespace
            String[] words = s.split(" ");

            //traverse all word, then wrap as tuple return
            for (String word : words) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
