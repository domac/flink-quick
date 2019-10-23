package com.domac;

import com.alibaba.fastjson.JSONObject;
import lombok.Setter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class TableQueryFromKafkaToRedis {

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "edrlog";
        String kafkaBrokers = "192.168.159.130:9092";
        String kafkaGroup = "edr-group";
        String kafkaProdTopic = "resultlog";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("group.id", kafkaGroup);

        FlinkKafkaConsumer011<LogData> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(kafkaTopic, new MessageDeserializer(), properties);
        DataStream<LogData> stream = env.addSource(flinkKafkaConsumer);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Table logsTable = tableEnv.fromDataStream(stream);
        tableEnv.registerTable("edrlog", logsTable);

        String querySQL = "select count(1) as cnt from edrlog";
        Table queryResult = tableEnv.sqlQuery(querySQL);

        DataStream<Tuple2<Boolean, Long>> result = tableEnv.toRetractStream(queryResult, Long.class);


        DataStream<Long> count = result.filter(new FilterFunction<Tuple2<Boolean, Long>>() {
            @Override
            public boolean filter(Tuple2<Boolean, Long> out) throws Exception {
                return out.f0;
            }
        }).map(new MapFunction<Tuple2<Boolean, Long>, Long>() {
            @Override
            public Long map(Tuple2<Boolean, Long> out) throws Exception {
                return out.f1;
            }
        });

        count.print();
        env.execute("table query from kafka to redis");
    }

    public static class MessageDeserializer implements DeserializationSchema<LogData> {

        @Override
        public LogData deserialize(byte[] data) throws IOException {
            return JSONObject.parseObject(data, LogData.class);
        }

        @Override
        public boolean isEndOfStream(LogData logData) {
            return false;
        }

        @Override
        public TypeInformation<LogData> getProducedType() {
            return TypeExtractor.getForClass(LogData.class);
        }
    }

    public static class LogData implements Serializable {
        private static final long serialVersionUID = 3559533002594201715L;

        @Setter
        String Ver;

        @Setter
        String Mid;

        @Setter
        String Plugin;

        @Setter
        String Tag;

        @Setter
        String Time;

        @Setter
        String Rawlog;

        public LogData(String ver, String mid, String plugin, String tag, String time, String rawlog) {
            Ver = ver;
            Mid = mid;
            Plugin = plugin;
            Tag = tag;
            Time = time;
            Rawlog = rawlog;
        }

        public LogData() {
        }

        @Override
        public String toString() {
            return "LogData{" +
                    "Ver='" + Ver + '\'' +
                    ", Mid='" + Mid + '\'' +
                    ", Plugin='" + Plugin + '\'' +
                    ", Tag='" + Tag + '\'' +
                    ", Time='" + Time + '\'' +
                    ", Rawlog='" + Rawlog + '\'' +
                    '}';
        }
    }


}
