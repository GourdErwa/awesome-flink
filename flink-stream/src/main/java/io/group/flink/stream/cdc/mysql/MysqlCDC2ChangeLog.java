package io.group.flink.stream.cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.RowKind;

/**
 * 测试 mysql-cdc 分流写入不同的 sink table。
 * 任务以 datastream 方式启动，方便新增表(https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#scan-newly-added-tables)
 *
 * @author Li.Wei by 2022/5/19
 */
public class MysqlCDC2ChangeLog {
    public static void main(String[] args) throws Exception {
        final Configuration flinkConfiguration = new Configuration();
        flinkConfiguration.setString("rest.bind-port", "8081-8089");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfiguration);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 开启checkpoint
        env.enableCheckpointing(10000L, CheckpointingMode.EXACTLY_ONCE);

        MySqlSource<Tuple3<String, RowKind, String>> mySqlSource = MySqlSource.<Tuple3<String, RowKind, String>>builder()
            .hostname("10.0.59.xx")
            .port(3306)
            .scanNewlyAddedTableEnabled(true)
            .databaseList("example_cdc")
            .tableList("example_cdc.common_user,example_cdc.order")
            .startupOptions(StartupOptions.initial())
            .includeSchemaChanges(true)
            .username("root")
            .password("123456")
            .deserializer(new CustomerJsonDebeziumDeserializationSchema())
            .build();

        final DataStreamSource<Tuple3<String, RowKind, String>> source
            = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        new ChangelogSink().compute(source, tableEnv);

        env.execute("MySQL CDC + Changelog");
    }
}
