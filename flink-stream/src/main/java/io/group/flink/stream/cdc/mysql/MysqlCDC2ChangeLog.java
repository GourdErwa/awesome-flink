package io.group.flink.stream.cdc.mysql;

import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import io.group.flink.stream.cdc.schema.RowKindJsonDeserializationSchemaBase;
import io.group.flink.stream.cdc.schema.RowKindJsonDeserializationSchemaV2;
import io.group.flink.stream.sink.ChangelogSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * 测试 mysql-cdc 分流写入不同的 sink table。
 * 任务以 DataStream 方式启动，方便新增表(<a href="https://ververica.github.io/flink-cdc-connectors/master/content/connectors/mysql-cdc.html#scan-newly-added-tables">...</a>)
 * <p>
 * 该任务版本未调整，无法运行，请 cp 到个人项目中执行。
 * <p>
 * flink-cdc2.2.1 默认使用 flink1.13。如果项目为 flink1.14 版本，需要自己编译 flink-cdc 参考
 * <a href="https://github.com/GourdErwa/flink-cdc-connectors-release-2.2.1_flink1.14#%E6%9C%AC%E5%9C%B0%E7%BC%96%E8%AF%91%E5%86%85%E5%AE%B9-">...</a>
 *
 * <p>flink-cdc2.3.0&flink1.16 版本已经解决以上适配问题
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

        MySqlSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> mySqlSource
            = MySqlSource.<RowKindJsonDeserializationSchemaBase.TableIRowKindJson>builder()
            .hostname("10.0.59.xx")
            .port(3306)
            .scanNewlyAddedTableEnabled(true)
            .databaseList("example_cdc")
            .tableList("example_cdc.common_user,example_cdc.order")
            .startupOptions(StartupOptions.initial())
            .includeSchemaChanges(true)
            .username("root")
            .password("123456")
            // 更多扩展支持分流序列化器参考 io.group.flink.stream.cdc.schema 包内容
            .deserializer(new RowKindJsonDeserializationSchemaV2())
            .build();

        final DataStreamSource<RowKindJsonDeserializationSchemaBase.TableIRowKindJson> source
            = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");

        // sink table
        new ChangelogSink().compute(source, tableEnv);
        // sink iceberg table
        // new IcebergCdcSink().compute(source, tableEnv);

        env.execute("MySQL CDC + Changelog");
    }
}
