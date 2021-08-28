package com.jimmy.apitest.source;

import com.jimmy.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;

/**
 * @ClassName: SourceTest4_CustomSource
 * @Project: flink-demo
 * @Author: qimingchen on 8/28/21
 */
public class SourceTest4_CustomSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        //read data from custom source
        DataStream<SensorReading> objectDataStream = executionEnvironment.addSource(new MySensorSource());

        //print the data
        objectDataStream.print();

        //execute
        executionEnvironment.execute();

    }

    //implment custom SourceFunction
    private static class MySensorSource implements SourceFunction<SensorReading> {

        //deinfe a flag to control data produce
        private boolean running = true;

        @Override
        public void run(SourceContext<SensorReading> sourceContext) throws Exception {

            //random generator
            Random random = new Random();

            //init 10 sensor default temp
            HashMap<String, Double> sensorTempMap = new HashMap<String, Double>();

            for ( int i =0; i< 10; i++ ){
                sensorTempMap.put("sensor_"+(i+1),60+random.nextGaussian()*20);
            }


            while (running){
                    for (String  sensorId: sensorTempMap.keySet()){

                        Double newTemp = sensorTempMap.get(sensorId)+ random.nextGaussian();
                        sensorTempMap.put(sensorId,newTemp);
                        sourceContext.collect(new SensorReading(sensorId,System.currentTimeMillis(),newTemp));

                    }
                    //simulate the latency, control speed to 1sec each request
                    Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {

            running = false;

        }
    }
}
