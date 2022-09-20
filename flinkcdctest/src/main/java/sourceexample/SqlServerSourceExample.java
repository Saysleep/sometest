package sourceexample;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.oracle.OracleSource;
import com.ververica.cdc.connectors.sqlserver.SqlServerSource;
import com.ververica.cdc.connectors.sqlserver.table.StartupOptions;
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

public class SqlServerSourceExample {
    public static void main(String[] args) throws Exception {

        //System.setProperty("HADOOP_USER_NAME","jie");

        //TODO 1,获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(3000);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //开启CK
        //env.enableCheckpointing(5000L);
        //env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //env.getCheckpointConfig().setCheckpointTimeout(10000L);


        //env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://10.0.0.14:8020/cdc");


        //TODO 2,使用FlinkCDC构建Source
        SourceFunction<String> sqlserverSource = SqlServerSource.<String>builder()
                .hostname("localhost")
                .port(1433)
                .database("db1")
                .username("flinkuser")
                .password("123456")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .tableList("dbo.Table3")
                .build();

        //sql
        String sql = "CREATE TABLE test (\n" +
                "    id int,\n" +
                "    name string\n" +
                ") WITH (\n" +
                "    'connector' = 'sqlserver-cdc',\n" +
                "    'hostname' = 'localhost',\n" +
                "    'port' = '1433',\n" +
                "    'username' = 'flinkuser',\n" +
                "    'password' = '123456',\n" +
                "    'database-name' = 'db1',\n" +
                "    'schema-name' = 'dbo',\n" +
                "    'table-name' = 'Table3'\n" +
                ")";
        tableEnv.executeSql(sql);
        tableEnv.sqlQuery("select * from test").execute().print();

        //TODO 3,读取数据
        DataStreamSource<String> sqlserverDS = env.addSource(sqlserverSource);


        //TODO 4,处理json数据
        //sqlserverDS.print(">>>");

        //TODO 5,启动
        env.execute();

    }
}
