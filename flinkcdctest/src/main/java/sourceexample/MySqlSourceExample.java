package sourceexample;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
//import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Properties;

public class MySqlSourceExample {
    public static void main(String[] args) throws Exception {

        //System.setProperty("HADOOP_USER_NAME","jie");

        //TODO 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);


        //开启CK
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);


        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://10.0.0.14:8020/cdc");


        //TODO 2,使用FlinkCDC构建Source
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("localhost")
                .port(3306)
                .databaseList("test") // set captured database
                .tableList("test.role1") // set captured table
                .username("root")
                .password("1126")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .startupOptions(StartupOptions.initial())
                .build();
        //sql
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE ROLE (\n" +
                "  id int\n" +
                "  ,name VARCHAR\n" +
                "  ,PRIMARY KEY (id) NOT ENFORCED\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'hostname' = '127.0.0.1',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '1126',\n" +
                " 'database-name' = 'test',\n" +
                " 'table-name' = 'role1'\n" +
                ")";
        tableEnv.executeSql(sql);

        tableEnv.sqlQuery("select * from ROLE").execute().print();

        //TODO 3,读取数据
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "Mysql");


        //TODO 4,处理json数据
        mysqlDS.print(">>>");

        //TODO 5,启动
        //env.execute();

    }
}
