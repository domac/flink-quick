package com.domac;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * @author domac
 * 实现自定义的Source
 */
public class CustomSimpleSource {

    private static final int BOUND = 100;

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<Integer, Integer>> inputStream = env.addSource(new RandomFibonacciSource());
        inputStream.map(new MyInputMap()).print();

        env.execute("custom simple source");
    }


    /**
     * 自定义的斐波那契Source
     */
    private static class RandomFibonacciSource implements SourceFunction<Tuple2<Integer, Integer>> {

        private static final long serialVersionUID = 1L;
        private Random random = new Random();
        private volatile boolean isRunning = true;
        private int counter = 0;

        @Override
        public void run(SourceContext<Tuple2<Integer, Integer>> sourceContext) throws Exception {
            while (isRunning && counter < BOUND) {
                int first = random.nextInt(BOUND / 2 - 1) + 1;
                int second = random.nextInt(BOUND / 2 - 1) + 1;
                sourceContext.collect(new Tuple2<>(first, second));
                counter++;
                Thread.sleep(50L);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    public static class MyInputMap implements MapFunction<Tuple2<Integer, Integer>, Tuple5<Integer, Integer, Integer, Integer, Integer>> {
        @Override
        public Tuple5<Integer, Integer, Integer, Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
            return new Tuple5<>(value.f0, value.f1, value.f0, value.f1, 0);
        }
    }
}
