package com.wmy.flink.cdc.high;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Properties;

/**
 * ClassName:Flink_CDCWithCustomerSchema
 * Package:com.wmy.flink.cdc.high
 *
 * @date:2021/7/19 11:11
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description: 自定义反序列化器
 */
public class Flink_CDCWithCustomerSchema {
    public static void main(String[] args) throws Exception {
        // 1、创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、创建Flink-MySQL-CDC的Source
        Properties props = new Properties();
        props.setProperty("debezium.snapshot.mode", "initial");
        props.setProperty("format", "debezium-json");

        // 3、采集数据
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("yaxin01")
                .port(3306)
                .username("root")
                .password("000000")
                .serverTimeZone("Asia/Shanghai")
                .databaseList("yaxin")
                .tableList("yaxin.info") // 可选配置项，如果不指定参数，一个配置下的所有表都会被监控
                .debeziumProperties(props)
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
//                .deserializer(new DebeziumDeserializationSchema<String>() { // 自定义序列化器
//                    @Override
//                    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
//                        // 获取主题信息，包括着数据库和表名
//                        String topic = sourceRecord.topic();
//                        String[] fields = topic.split("\\.");
//                        String db = fields[1];
//                        String tableName = fields[2];
//
//                        // 获取操作类型
//                        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//
//                        // 获取值信息并转换为Struct类型
//                        Struct value = (Struct) sourceRecord.value();
//
//                        // 获取变化后的数据
//                        Struct after = value.getStruct("after");
//
//                        // 创建JSON对象用于存储信息
//                        JSONObject data = new JSONObject();
//                        for (Field field : after.schema().fields()) {
//                            Object o = after.get(field);
//                            data.put(field.name(), o);
//                        }
//
//                        // 创建JSON对象用于封装最终返回数据信息
//                        JSONObject result = new JSONObject();
//                        result.put("operation", operation.toString().toLowerCase());
//                        result.put("data", data);
//                        result.put("database", db);
//                        result.put("table", tableName);
//
//                        // 发送数据至下游
//                        collector.collect(result.toJSONString());
//                    }
//
//                    @Override
//                    public TypeInformation<String> getProducedType() {
//                        return TypeInformation.of(String.class);
//                    }
//                }).build();

        // 4、将MySQL动态采集的数据实时输出到外部系统
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);
        streamSource.print();

        // 5、执行流式程序
        env.execute("Flink_CDCWithCustomerSchema >>> ");

    }
}
