package com.domac;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * ./kafka-console-producer.sh --broker-list 192.168.159.130:9092 --topic my_test_topic
 */
public class SimpleStreamingFromKafka {

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "my_test_topic";
        String kafkaBrokers = "192.168.159.130:9092";
        String kafkaGroup = "my-test-group";

        //创建运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //配置属性
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaBrokers);
        properties.setProperty("group.id", kafkaGroup);

        //创建Kafka Source
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(
                kafkaTopic, new SimpleStringSchema(), properties);

        DataStream<String> stream = env.addSource(flinkKafkaConsumer);

        stream.print();

        env.execute("simple streaming from kafka");
    }
}
