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

public class OracleSourceExample {
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
        Properties properties = new Properties();
        properties.setProperty("log.mining.strategy", "online_catalog");
        properties.setProperty("log.mining.continuous.mine", "true");
        System.out.println(properties);

        SourceFunction<String> oracleSoucre = OracleSource.<String>builder()
                .hostname("localhost")
                .port(1521)
                .database("orcl") // monitor XE database
                .schemaList("flinkuser") // monitor inventory schema
                .tableList("FLINKUSER.WORKER") // monitor products table
                .username("flinkuser")
                .password("flinkpw")
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .debeziumProperties(properties)
                .startupOptions(com.ververica.cdc.connectors.oracle.table.StartupOptions.initial())
                .build();

//        DebeziumSourceFunction<String> oracleSoucre2 = OracleSource.<String>builder()
//                .hostname("172.16.10.57")
//                .port(1521)
//                .database("MESPROD") // monitor XE database
//                .schemaList("HCMS") // monitor inventory schema
//                .tableList("HCM_PROD_LINE_WKCG_REL") // monitor products table
//                .username("plsql")
//                .password("plsql")
//                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
//                .startupOptions(com.ververica.cdc.connectors.oracle.table.StartupOptions.latest())
//                .build();

        //sql
        String sql = "" +
                "CREATE TABLE test ( " +
                " ID INT , " +
                " NAME STRING " +
                ") WITH ( " +
                " 'connector' = 'oracle-cdc', " +
                " 'hostname' = 'localhost', " +
                " 'port' = '1521', " +
                " 'username' = 'flinkuser', " +
                " 'password' = 'flinkpw', " +
                " 'database-name' = 'ORCL', " +
                " 'schema-name' = 'FLINKUSER'," +
                " 'table-name' = 'WORKER', " +
                " 'scan.startup.mode' = 'initial'," +
                "'debezium.log.mining.strategy' = 'online_catalog'," +
                "'debezium.log.mining.continuous.mine' = 'true'" +
                ")";
        tableEnv.executeSql(sql);


        //TODO 3,读取数据
        DataStreamSource<String> oracleDS = env.addSource(oracleSoucre);

        //TODO 4,处理json数据
        //oracleDS.print(">>>");
        tableEnv.sqlQuery("select * from test").execute().print();

        //TODO 5,启动
        env.execute();

    }
}
