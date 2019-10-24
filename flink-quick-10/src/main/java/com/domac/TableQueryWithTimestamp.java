package com.domac;

import com.alibaba.fastjson.JSONObject;
import lombok.Setter;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

public class TableQueryWithTimestamp {

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "edrlog";
        String kafkaBrokers = "192.168.159.130:9092";
        String kafkaGroup = "edr-group";
        String kafkaProdTopic = "resultlog";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //定义kafka 消费
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("group.id", kafkaGroup);
        FlinkKafkaConsumer011<LogData> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(kafkaTopic, new MessageDeserializer(), properties);
        flinkKafkaConsumer.setStartFromLatest();
        DataStream<LogData> stream = env.addSource(flinkKafkaConsumer);

        //定义 Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table logsTable = tableEnv.fromDataStream(stream);
        tableEnv.registerTable("edrlog", logsTable);

        //处理查询
        String querySQL = "select * from edrlog";
        Table queryResult = tableEnv.sqlQuery(querySQL);
        DataStream<Tuple2<Boolean, LogData>> result = tableEnv.toRetractStream(queryResult, LogData.class);

        //结果处理
        DataStream<LogData> response = result.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Boolean, LogData>>() {
            @Override
            public long extractAscendingTimestamp(Tuple2<Boolean, LogData> out) {
                // 原始数据单位秒，将其转成毫秒
                return out.f1.Time * 1000;
            }
        }).filter(new FilterFunction<Tuple2<Boolean, LogData>>() {
            @Override
            public boolean filter(Tuple2<Boolean, LogData> out) throws Exception {
                return out.f0;
            }
        }).map(new MapFunction<Tuple2<Boolean, LogData>, LogData>() {
            @Override
            public LogData map(Tuple2<Boolean, LogData> out) throws Exception {
                return out.f1;
            }
        });
        response.print();
        env.execute("table query with timestamp");
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
        Long Time; //timestamp

        @Setter
        String Rawlog;

        public LogData(String ver, String mid, String plugin, String tag, Long time, String rawlog) {
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
