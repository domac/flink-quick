package com.domac;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
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
        //FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<String>(kafkaTopic, new SimpleStringSchema(), properties);
        //DataStream<String> stream = env.addSource(flinkKafkaConsumer);

        //自定义Schema的方式
        FlinkKafkaConsumer<Message> flinkKafkaConsumer = new FlinkKafkaConsumer<>(kafkaTopic, new MessageDeserializer(), properties);
        DataStream<Message> stream = env.addSource(flinkKafkaConsumer);

        stream.print();
        env.execute("simple streaming from kafka");
    }

    //自定义消息格式
    public static class Message {
        String name;
        String data;

        public Message(String name, String data) {
            this.name = name;
            this.data = data;
        }

        @Override
        public String toString() {
            return "Message{" +
                    "topic='" + name + '\'' +
                    ", data='" + data + '\'' +
                    '}';
        }
    }

    public static class MessageDeserializer implements DeserializationSchema<Message> {

        @Override
        public Message deserialize(byte[] bytes) throws IOException {
            //这里只是简单的反序列化为字符串,当然你也可以反序列化为JSON
            String message = new String(bytes);
            return new Message("my_test_topic", message);
        }

        @Override
        public boolean isEndOfStream(Message message) {
            return false;
        }

        @Override
        public TypeInformation<Message> getProducedType() {
            return TypeExtractor.getForClass(Message.class);
        }
    }
}
