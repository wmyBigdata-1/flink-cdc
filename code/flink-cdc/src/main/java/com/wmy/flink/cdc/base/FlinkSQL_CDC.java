package com.wmy.flink.cdc.base;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * ClassName:FlinkSQL_CDC
 * Package:com.wmy.flink.cdc.base
 *
 * @date:2021/7/18 12:22
 * @author:数仓开发工程师
 * @email:wmy_2000@163.com
 * @Description:
 */
public class FlinkSQL_CDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //2.创建Flink-MySQL-CDC的Source
        tableEnv.executeSql("CREATE TABLE user_info (" +
                "  id INT," +
                "  name STRING" +
                ") WITH (" +
                "  'connector' = 'mysql-cdc'," +
                "  'hostname' = 'yaxin01'," +
                "  'port' = '3306'," +
                "  'username' = 'root'," +
                "  'password' = '000000'," +
                "  'database-name' = 'yaxin'," +
                "  'table-name' = 'activities'" +
                ")");

        tableEnv.executeSql("select * from  user_info ").print();

        env.execute();
    }
}
