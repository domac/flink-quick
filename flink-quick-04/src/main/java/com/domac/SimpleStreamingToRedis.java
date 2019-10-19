package com.domac;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class SimpleStreamingToRedis {

    public static void main(String[] args) throws Exception {

        String SourceServerHost = "192.168.159.130";
        int SourceServerPort = 9000;

        String RedisHost = "192.168.159.130";
        int RedisPort = 6379;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<String> text = env.socketTextStream(SourceServerHost, SourceServerPort, "\n");

        DataStream<Tuple2<String, String>> wordsData = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                return new Tuple2<>("my_words", value);
            }
        });

        //创建redis sink
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(RedisHost).setPort(RedisPort).build();
        RedisSink<Tuple2<String, String>> redisSink = new RedisSink<>(conf, new MyRedisMapper());

        wordsData.addSink(redisSink);

        env.execute("Simple Streaming To Redis");
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.LPUSH);
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}
