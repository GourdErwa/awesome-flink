package io.group.flink.stream.sink;

import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamStatementSet;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * dataStream 分流写入不同目标表。
 * <p>
 * cdc 数据源格式 source ：Tuple3<String, RowKind, String> = Tuple3<表名, RowKind, row json 序列化数据>
 *
 * @author Li.Wei by 2022/6/7
 */
@Slf4j
public class ChangelogSink {

    private final List<TableSettingConfig> tableSettingConfigs = Lists.newArrayList();

    {
        // TODO 必须该处初始化 tableSettingConfigs
    }

    public void compute(DataStream<Tuple3<String, RowKind, String>> input, StreamTableEnvironment tableEnv) {

        final StreamStatementSet statementSet = tableEnv.createStatementSet();

        for (TableSettingConfig config : tableSettingConfigs) {
            // 执行 create sql ，目的为获取 table schema
            tableEnv.executeSql(config.createTableSql);
            final ResolvedSchema sinkTableResolvedSchema = tableEnv.from(config.getTableIdentifier()).getResolvedSchema();
            final List<Column> columns = sinkTableResolvedSchema.getColumns();
            final TypeInformation<?>[] information = new TypeInformation[columns.size()];
            final String[] fieldNames = new String[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                final Column column = columns.get(i);
                fieldNames[i] = column.getName();
                information[i] = TypeConversions.fromDataTypeToLegacyInfo(column.getDataType());
            }
            final RowTypeInfo rowTypeInfo = new RowTypeInfo(information, fieldNames);
            log.info("builder source table rowTypeInfo: {}", rowTypeInfo);

            // 反序列化 json
            final JsonRowDeserializationSchema deserializationSchema =
                new JsonRowDeserializationSchema.Builder(rowTypeInfo)
                    .failOnMissingField()
                    .build();

            // 分流匹配对应的表
            final SingleOutputStreamOperator<Row> ds = input
                .filter(value -> value.f0.equals(config.getSubscriptionTableName()))
                .map(value -> {
                    final Row rowData = deserializationSchema.deserialize(value.f2.getBytes(StandardCharsets.UTF_8));
                    rowData.setKind(value.f1);
                    return rowData;
                }, (TypeInformation<Row>) TypeConversions.fromDataTypeToLegacyInfo(sinkTableResolvedSchema.toPhysicalRowDataType()))
                .name(String.format("Subscription[%s]", config.getSubscriptionTableName()));

            final Table table = tableEnv.fromChangelogStream(ds);
            table.printSchema();

            log.info("source table schema: {}", table.getResolvedSchema().toString());
            log.info("sink table schema: {}", sinkTableResolvedSchema);
            statementSet.addInsert(config.getTableIdentifier(), table);
        }
        // 该处需要使用该方式执行，如果使用 table insert table 方式，会启动不同的 flink-sql 任务，无法统一保存检查点
        statementSet.attachAsDataStream();
    }

    @Data
    @Builder
    public static class TableSettingConfig implements Serializable {
        /**
         * 写入目标表表名
         */
        private String tableIdentifier;
        /**
         * 写入目标表建表语句
         */
        private String createTableSql;
        /**
         * 写入目标表表名订阅的 cdc 中源表名称
         * 未配置默认等于 tableIdentifier 中表名
         */
        private String subscriptionTableName;
    }
}
