package com.domac;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.util.HashMap;
import java.util.UUID;

public interface CepQuerySinkToKafka {

    public static void main(String[] args) throws Exception {

        String zk = null;
        String kafka = null;

        String inputSQL = null;

        try {
            ParameterTool parameterTool = ParameterTool.fromArgs(args);

            //--zk "192.168.159.130:2181"
            zk = parameterTool.get("zk");

            //--kafka "192.168.159.130:9092"
            kafka = parameterTool.get("kafka");

        } catch (Exception e) {
            e.printStackTrace();
        }


        if (null == zk) {
            zk = "192.168.159.130:2181";
        }

        if (null == kafka) {
            kafka = "192.168.159.130:9092";
        }


        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);


        //定义 Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka().version("0.11").topic("edrlog").startFromLatest()
                .property("zookeeper.connect", zk)
                .property("bootstrap.servers", kafka))
                .withFormat(new Json().deriveSchema())
                //数据表的schema
                .withSchema(new Schema()
                        .field("Ver", Types.STRING)
                        .field("Mid", Types.STRING)
                        .field("Plugin", Types.STRING)
                        .field("Tag", Types.STRING)
                        .field("Time", Types.LONG)
                        .field("process_timestamp", Types.SQL_TIMESTAMP)
                        .field("Type", Types.INT)
                        .field("Op", Types.INT)
                        .field("OptPid", Types.LONG)
                        .field("OptProcPath", Types.STRING)
                        .field("OptFileSize", Types.INT)
                        .field("OptModifyTime", Types.LONG)
                        .field("OptProcName", Types.STRING)
                        .field("OptCmdline", Types.STRING)
                        .field("OptPPid", Types.LONG)
                        .field("OptStdin", Types.STRING)
                        .field("OptStdout", Types.STRING)
                        .field("OptProcMd5", Types.STRING)
                        .field("OptProcUserName", Types.STRING)
                        .field("OptProcUid", Types.LONG)
                        .field("OptProcGroupName", Types.STRING)
                        .field("OptProcGid", Types.LONG)
                        .field("OptScanResult", Types.INT)
                        .field("SubFileName", Types.STRING)
                        .field("SubFilePath", Types.STRING)
                        .field("SubPid", Types.LONG)
                        .field("SubProcCmdline", Types.STRING)
                        .field("SubUserName", Types.STRING)
                        .field("SubUid", Types.LONG)
                        .field("SubGroupName", Types.STRING)
                        .field("SubGid", Types.LONG)
                        .field("SubProcStdin", Types.STRING)
                        .field("SubProcStdout", Types.STRING)
                        .field("SubFileSize", Types.LONG)
                        .field("SubModifyTime", Types.LONG)
                        .field("SubFileMd5", Types.STRING)
                        .field("SubFileType", Types.INT)
                        .field("SubScanResult", Types.INT)
                        .field("SubLIP", Types.STRING)
                        .field("SubRIP", Types.STRING)
                        .field("SubLPort", Types.INT)
                        .field("SubRPort", Types.INT)
                        .field("SubNetStatus", Types.STRING)
                        .field("SubNetProto", Types.STRING)
                        .field("LogName", Types.STRING)
                        .field("LogText", Types.STRING)
                        .field("LogOffset", Types.LONG)
                        .field("Platform", Types.INT)
                        .field("OsVersion", Types.STRING)
                        .field("Os6432", Types.INT)
                        .field("IPList", Types.STRING)
                        .field("EdrVersion", Types.STRING)
                        .field("EventId", Types.LONG)
                        .field("event_time", Types.SQL_TIMESTAMP).rowtime(new Rowtime() //定义时间时间
                                .timestampsFromField("event_timestamp")//event_timestamp 格式必须是 2019-10-24T22:18:30.000Z
                                .watermarksPeriodicBounded(60000)
                        )
                ).inAppendMode().registerTableSource("agentdata");


        inputSQL = "select Mid, SubFilePath, event_time from agentdata";
        Table result = tableEnv.sqlQuery(inputSQL);

        //获取查询的字段名称
        String[] fields = result.getSchema().getFieldNames();

        DataStream<Tuple2<Boolean, Row>> rowResult = tableEnv.toRetractStream(result, Row.class);

        DataStream<String> ds = rowResult.filter((FilterFunction<Tuple2<Boolean, Row>>) value -> value.f0).map((MapFunction<Tuple2<Boolean, Row>, String>) value -> {
            HashMap<String, Object> results = new HashMap<>();
            int index = 0;
            for (String f : fields) {
                results.put(f, value.f1.getField(index));
                index++;
            }
            results.put("notify_id", UUID.randomUUID().toString());
            results.put("notify_time", System.currentTimeMillis());
            return JSON.toJSON(results).toString();
        });

        ds.addSink(new FlinkKafkaProducer011<>(kafka, "flink_result", new SimpleStringSchema())).name("flink-connectors-kafka").setParallelism(1);
        String server_info = "zk:" + zk + "_kafka:" + kafka + "_sql=" + inputSQL;
        env.execute(server_info);
    }
}
