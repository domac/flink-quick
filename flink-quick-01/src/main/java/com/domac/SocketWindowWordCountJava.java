package com.domac;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class SocketWindowWordCountJava {

    public static void main(String[] args) throws Exception {
        int port;
        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            port = parameterTool.getInt("port");
        } catch (Exception e) {
            System.err.println("No port set. use default port 9000--Java");
            port = 9000;
        }

        //获取Flink的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        String hostname = "localhost";
        String delimter = "\n";

        DataStreamSource<String> text = env.socketTextStream(hostname, port, delimter);

        DataStream<WorldWithCount> windowCounts = text.flatMap(new FlatMapFunction<String, WorldWithCount>() {
            public void flatMap(String value, Collector<WorldWithCount> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word : splits) {
                    out.collect(new WorldWithCount(word, 1L));
                }
            }
        }).keyBy("word").timeWindow(Time.seconds(2), Time.seconds(1)).sum("count");

        windowCounts.print().setParallelism(1);
        env.execute("Socket window count");
    }


    public static class WorldWithCount {

        public String word;
        public long count;

        public WorldWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public WorldWithCount() {
        }

        @Override
        public String toString() {
            return "WorldWithCount{" +
                    "word='" + word + "', coun=" + count + "}";
        }
    }
}
