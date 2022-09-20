package sourceexample;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CDCToDoris {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取CDC的数据到临时表中 形成动态表test_flink_cdc
        String sourceSql = "CREATE TABLE test_flink_cdc (\n" +
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
        tableEnv.executeSql(sourceSql);
        //查看动态表是否存在
        //tableEnv.sqlQuery("select * from test_flink_cdc").execute().print();

        //读取doris的数据到临时表中 形成动态表 doris_test_sink
        String destinationSql = "CREATE TABLE doris_test_sink (\n" +
                "    id INT,\n" +
                "    name STRING\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.doris_test_sink',\n" +
                "       'sink.batch.size' = '2',\n" +
                "       'sink.batch.interval'='1',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(destinationSql);
        //查看动态表是否存在
        //tableEnv.sqlQuery("select * from doris_test_sink").execute().print();

        //将查询的结果插入到doris中
        tableEnv.executeSql("INSERT INTO doris_test_sink select id,name from test_flink_cdc");

    }
}
