package com.domac;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import java.io.Serializable;

public class TableQueryWithSchema {

    public static void main(String[] args) throws Exception {

        String zkServer = "192.168.159.130:2181";
        String kafkaServer = "192.168.159.130:9092";

        String[] fieldNames = new String[]{"OptCmdline", "OptProcPath", "Op", "Type", "OptProcMd5", "OptStdin", "OptPPid", "OptFileSize", "SubFileMd5", "OptModifyTime", "SubFilePath", "SubPid", "OptProcName", "OptPid", "Time"};
        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.INT, Types.INT, Types.STRING, Types.STRING, Types.LONG, Types.LONG, Types.STRING, Types.LONG, Types.STRING, Types.LONG, Types.STRING, Types.LONG, Types.LONG};


        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        //定义 Table Environment
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.connect(new Kafka().version("0.11").topic("edrlog").startFromLatest()
                .property("zookeeper.connect", zkServer)
                .property("bootstrap.servers", kafkaServer))
                .withFormat(new Json().deriveSchema())
                //数据表的schema
                .withSchema(new Schema()
                        .field("Ver", Types.STRING)
                        .field("Mid", Types.STRING)
                        .field("Plugin", Types.STRING)
                        .field("Tag", Types.STRING)
                        .field("Time", Types.STRING)
                        .field("Rawlog", Types.POJO(RawLog.class))
                ).inAppendMode().registerTableSource("agentLogs");

        String querySQL = "select Mid, Rawlog.SubFilePath, Rawlog.Op, Rawlog.Type from agentLogs where Rawlog.Type > 1";
        Table result = tableEnv.sqlQuery(querySQL);

        tableEnv.toRetractStream(result, Row.class).print();
        env.execute("table json query");
    }

    public static class LogData implements Serializable {
        String Ver;
        String Mid;
        String Plugin;
        String Tag;
        String Time; //timestamp
        RawLog Rawlog;

        public LogData() {
        }

        public LogData(String ver, String mid, String plugin, String tag, String time, RawLog rawlog) {
            Ver = ver;
            Mid = mid;
            Plugin = plugin;
            Tag = tag;
            Time = time;
            Rawlog = rawlog;
        }

        public String getVer() {
            return Ver;
        }

        public void setVer(String ver) {
            Ver = ver;
        }

        public String getMid() {
            return Mid;
        }

        public void setMid(String mid) {
            Mid = mid;
        }

        public String getPlugin() {
            return Plugin;
        }

        public void setPlugin(String plugin) {
            Plugin = plugin;
        }

        public String getTag() {
            return Tag;
        }

        public void setTag(String tag) {
            Tag = tag;
        }

        public String getTime() {
            return Time;
        }

        public void setTime(String time) {
            Time = time;
        }

        public RawLog getRawlog() {
            return Rawlog;
        }

        @Override
        public String toString() {
            return "LogData => {" +
                    "Ver='" + Ver + '\'' +
                    ", Mid='" + Mid + '\'' +
                    ", Plugin='" + Plugin + '\'' +
                    ", Tag='" + Tag + '\'' +
                    ", Time='" + Time + '\'' +
                    ", Rawlog=" + Rawlog +
                    '}';
        }

        public void setRawlog(RawLog rawlog) {
            Rawlog = rawlog;
        }
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class RawLog implements Serializable {

        @JsonIgnoreProperties("Id")
        String id;

        @JsonProperty(value = "OptCmdline")
        String OptCmdline;

        @JsonProperty(value = "OptProcPath")
        String OptProcPath;

        @JsonProperty(value = "Op")
        int Op;

        @JsonProperty(value = "Type")
        int Type;

        @JsonProperty(value = "OptProcMd5")
        String OptProcMd5;

        @JsonProperty(value = "OptStdin")
        String OptStdin;

        @JsonProperty(value = "OptPPid")
        Long OptPPid;

        @JsonProperty(value = "OptFileSize")
        Long OptFileSize;

        @JsonProperty(value = "SubFileMd5")
        String SubFileMd5;

        @JsonProperty(value = "OptModifyTime")
        String OptModifyTime;

        @JsonProperty(value = "SubFilePath")
        String SubFilePath;

        @JsonProperty(value = "SubPid")
        Long SubPid;

        @JsonProperty(value = "OptProcName")
        String OptProcName;

        @JsonProperty(value = "OptPid")
        Long OptPid;

        @JsonProperty(value = "Time")
        Long EventTime;

        public RawLog() {
        }

        public RawLog(String id, String optCmdline, String optProcPath, int op, int type, String optProcMd5, String optStdin, Long optPPid, Long optFileSize, String subFileMd5, String optModifyTime, String subFilePath, Long subPid, String optProcName, Long optPid, Long eventTime) {
            this.id = id;
            this.OptCmdline = optCmdline;
            this.OptProcPath = optProcPath;
            this.Op = op;
            this.Type = type;
            this.OptProcMd5 = optProcMd5;
            this.OptStdin = optStdin;
            this.OptPPid = optPPid;
            this.OptFileSize = optFileSize;
            this.SubFileMd5 = subFileMd5;
            this.OptModifyTime = optModifyTime;
            this.SubFilePath = subFilePath;
            this.SubPid = subPid;
            this.OptProcName = optProcName;
            this.OptPid = optPid;
            this.EventTime = eventTime;
        }

        @Override
        public String toString() {
            return "RawLog{" +
                    "id='" + id + '\'' +
                    ", OptCmdline='" + OptCmdline + '\'' +
                    ", OptProcPath='" + OptProcPath + '\'' +
                    ", Op=" + Op +
                    ", Type=" + Type +
                    ", OptProcMd5='" + OptProcMd5 + '\'' +
                    ", OptStdin='" + OptStdin + '\'' +
                    ", OptPPid=" + OptPPid +
                    ", OptFileSize=" + OptFileSize +
                    ", SubFileMd5='" + SubFileMd5 + '\'' +
                    ", OptModifyTime='" + OptModifyTime + '\'' +
                    ", SubFilePath='" + SubFilePath + '\'' +
                    ", SubPid=" + SubPid +
                    ", OptProcName='" + OptProcName + '\'' +
                    ", OptPid=" + OptPid +
                    ", EventTime=" + EventTime +
                    '}';
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getOptCmdline() {
            return OptCmdline;
        }

        public void setOptCmdline(String optCmdline) {
            OptCmdline = optCmdline;
        }

        public String getOptProcPath() {
            return OptProcPath;
        }

        public void setOptProcPath(String optProcPath) {
            OptProcPath = optProcPath;
        }

        public int getOp() {
            return Op;
        }

        public void setOp(int op) {
            Op = op;
        }

        public int getType() {
            return Type;
        }

        public void setType(int type) {
            Type = type;
        }

        public String getOptProcMd5() {
            return OptProcMd5;
        }

        public void setOptProcMd5(String optProcMd5) {
            OptProcMd5 = optProcMd5;
        }

        public String getOptStdin() {
            return OptStdin;
        }

        public void setOptStdin(String optStdin) {
            OptStdin = optStdin;
        }

        public Long getOptPPid() {
            return OptPPid;
        }

        public void setOptPPid(Long optPPid) {
            OptPPid = optPPid;
        }

        public Long getOptFileSize() {
            return OptFileSize;
        }

        public void setOptFileSize(Long optFileSize) {
            OptFileSize = optFileSize;
        }

        public String getSubFileMd5() {
            return SubFileMd5;
        }

        public void setSubFileMd5(String subFileMd5) {
            SubFileMd5 = subFileMd5;
        }

        public String getOptModifyTime() {
            return OptModifyTime;
        }

        public void setOptModifyTime(String optModifyTime) {
            OptModifyTime = optModifyTime;
        }

        public String getSubFilePath() {
            return SubFilePath;
        }

        public void setSubFilePath(String subFilePath) {
            SubFilePath = subFilePath;
        }

        public Long getSubPid() {
            return SubPid;
        }

        public void setSubPid(Long subPid) {
            SubPid = subPid;
        }

        public String getOptProcName() {
            return OptProcName;
        }

        public void setOptProcName(String optProcName) {
            OptProcName = optProcName;
        }

        public Long getOptPid() {
            return OptPid;
        }

        public void setOptPid(Long optPid) {
            OptPid = optPid;
        }

        public Long getEventTime() {
            return EventTime;
        }

        public void setEventTime(Long eventTime) {
            EventTime = eventTime;
        }
    }
}
