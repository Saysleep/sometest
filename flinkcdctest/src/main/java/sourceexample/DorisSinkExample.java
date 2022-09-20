package sourceexample;

import org.apache.doris.flink.cfg.*;
import org.apache.doris.flink.datastream.DorisSourceFunction;
import org.apache.doris.flink.deserialization.SimpleListDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;

public class DorisSinkExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Properties pro = new Properties();
        pro.setProperty("format", "json");
        pro.setProperty("strip_outer_array", "true");
        env.fromElements(
                "{\"record_id\": \"1\", \"seller_id\": \"3\", \"store_id\": \"2\", \"sale_date\": \"2020-05-05\", \"sale_amt\": \"2\"}"
        ).addSink(
                DorisSink.sink(
                        DorisReadOptions.builder().build(),
                        DorisExecutionOptions.builder()
                                .setBatchSize(3)
                                .setBatchIntervalMs(0L)
                                .setMaxRetries(3)
                                .setStreamLoadProp(pro).build(),
                        DorisOptions.builder()
                                .setFenodes("10.0.0.50:8030")
                                .setTableIdentifier("test_db.sales_records")
                                .setUsername("root")
                                .setPassword("000000").build()
                ));
// .addSink(
// DorisSink.sink(
// DorisOptions.builder()
// .setFenodes("FE_IP:8030")
// .setTableIdentifier("db.table")
// .setUsername("root")
// .setPassword("").build()
// ));

        //sql 尝试将别的动态表数据插入到doris中
        //

        env.execute();
    }
}
