package com.domac;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;

import java.io.*;
import java.util.Properties;

/**
 * ./kafka-console-producer.sh --broker-list 192.168.159.130:9092 --topic my_test_topic
 * <p>
 * ./kafka-console-consumer.sh --bootstrap-server 192.168.159.130:9092 --topic your_test_topic --from-beginning
 */

/**
 * author: domacli@tencent.com
 */
public class SimpleStreamingFromKafka {

    public static void main(String[] args) throws Exception {

        String kafkaTopic = "my_test_topic";
        String kafkaBrokers = "192.168.159.130:9092";
        String kafkaGroup = "my-test-group";
        String kafkaProdTopic = "your_test_topic";

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
        FlinkKafkaConsumer011<Message> flinkKafkaConsumer = new FlinkKafkaConsumer011<>(kafkaTopic, new MessageDeserializer(), properties);
        DataStream<Message> stream = env.addSource(flinkKafkaConsumer);

        //场景一: 输出到控制台
        //stream.print();
        //场景二: 输出到kafka
        /*DataStream<String> text = stream.map(new MapFunction<Message, String>() {
            @Override
            public String map(Message message) throws Exception {
                return "{" + message.data + "}";
            }
        });
        FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<>(kafkaBrokers, kafkaProdTopic, new SimpleStringSchema());
        text.addSink(myProducer);*/

        //场景三: 自定义schema的方式
        FlinkKafkaProducer011<Message> myProducer = new FlinkKafkaProducer011<>(kafkaBrokers, kafkaProdTopic, new SimpleMessageSchema());
        stream.addSink(myProducer);

        env.execute("streaming from kafka to domac's kafka");
    }

    //自定义消息格式
    public static class Message implements Serializable {

        private static final long serialVersionUID = 3559533002594201715L;

        String name;
        String data;

        public Message(String name, String data) {
            this.name = name;
            this.data = data;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getData() {
            return data;
        }

        public void setData(String data) {
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

    //自定义消费 DeserializationSchema
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


    public static class SimpleMessageSchema implements DeserializationSchema<Message>, SerializationSchema<Message> {
        @Override
        public Message deserialize(byte[] bytes) throws IOException {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream is = new ObjectInputStream(in);
            Message obj = null;
            try {
                obj = (Message) is.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return obj;
        }

        @Override
        public boolean isEndOfStream(Message message) {
            return false;
        }

        @Override
        public byte[] serialize(Message message) {
            return message.data.getBytes();
        }

        @Override
        public TypeInformation<Message> getProducedType() {
            return TypeExtractor.getForClass(Message.class);
        }
    }

}
