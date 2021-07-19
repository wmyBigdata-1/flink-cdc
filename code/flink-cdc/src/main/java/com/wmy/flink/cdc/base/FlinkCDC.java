package com.wmy.flink.cdc.base;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.alibaba.ververica.cdc.debezium.DebeziumSourceFunction;
import com.alibaba.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

/**
 * ClassName:FlinkCDC
 * Package:com.wmy.flink.cdc.base
 *
 * @date:2021/7/18 9:26
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: Flink CDC 案例
 * flink run -c com.wmy.flink.cdc.base.FlinkCDC flink-cdc-1.0-SNAPSHOT-jar-with-dependencies.jar
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        // 1、获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2、Checkpoint
        // 2.1 开启checkpoint，每隔5秒钟做一次checkpoint
        //env.enableCheckpointing(5000L);

        // 2.2 指定checkpoint一次性语义
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 2.3 设置任务关闭保留最后一次checkpoint数据
        //env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 2.4 指定checkpoint自动重启策略
        //env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 2000L));

        // 2.5 设置状态后端
        //env.setStateBackend(new FsStateBackend("hdfs://yaxin01:9820/flink/flink-cdc/ck"));

        // 2.6 设置用户名
        //System.setProperty("HADOOP_USER_NAME", "root");

        // 3、创建MySQL CDC Source
        Properties props = new Properties();
        props.setProperty("debezium.snapshot.mode", "initial");
        DebeziumSourceFunction<String> sourceFunction = MySQLSource.<String>builder()
                .hostname("yaxin01")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("flink_big_data")
                .tableList("flink_big_data.temp") // 注意：可选配置项，必须使用库名.表名
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new StringDebeziumDeserializationSchema())
                .debeziumProperties(props)
                .build();

        // 4、读取MySQL 数据
        DataStreamSource<String> streamSource = env.addSource(sourceFunction);

        // 5、输出到外部系统
        streamSource.print("FlinkCDC >>>> ");

        // 6、执行任务
        env.execute("FlinkCDC >>>> ");
    }
}