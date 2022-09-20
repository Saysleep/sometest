package sourceexample;

import org.apache.doris.flink.cfg.DorisStreamOptions;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class DorisSourceExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //DataStream
        Properties properties = new Properties();
        properties.put("fenodes","10.0.0.50:8030");
        properties.put("username","root");
        properties.put("password","000000");
        properties.put("table.identifier","test_db.sales_records");
        env.addSource(new DorisSourceFunction(
                        new DorisStreamOptions(properties),
                        new SimpleListDeserializationSchema()
                )
        ).print();

        //SQL
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String sql = "CREATE TABLE doris_test_sink (\n" +
                "    id INT,\n" +
                "    name STRING\n" +
                "    ) \n" +
                "    WITH (\n" +
                "      'connector' = 'doris',\n" +
                "      'fenodes' = '10.0.0.50:8030',\n" +
                "      'table.identifier' = 'test_db.doris_test_sink',\n" +
                "      'username' = 'root',\n" +
                "      'password' = '000000'\n" +
                ")";
        tableEnv.executeSql(sql);

        tableEnv.sqlQuery("select * from doris_test_sink").execute().print();
        System.out.println("===================");
        //tableEnv.sqlQuery("select store_id,sum(sale_amt) from sales_records group by store_id").execute().print();
        //env.execute();
    } }
