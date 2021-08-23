package com.jimmy.wc;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author qimingchen on 8/23/21
 * @Project flink-demo
 * @Description: Demo for batch processing data set
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        // create execution env
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();

        //read data from file
        String inputPath = "src/main/resources/hello.txt";
        //it is extending DataSet
        //DataSet<String> stringDataSource = executionEnvironment.readTextFile(inputPath);
        DataSource<String> inputDataSet = executionEnvironment.readTextFile(inputPath);

        //processing dataset, using whitespace as delimiter then transfer to (word,1) 2d array
        DataSet<Tuple2<String,Integer>> resultSet = inputDataSet.flatMap(new MyFlatMapper())
                .groupBy(0)// group by first position of word
                .sum(1); // sum the second field

        resultSet.print();
    }

    //custom class, implement FlatMapFunction interface
    public static class MyFlatMapper implements FlatMapFunction<String, Tuple2<String,Integer>>{
        @Override
        public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
            //split word by whitespace
            String[] words = s.split(" ");

            //traverse all word, then wrap as tuple return
            for (String word: words){
                collector.collect(new Tuple2<String,Integer>(word,1));
            }
        }
    }
}
