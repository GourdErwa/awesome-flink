package io.group.flink.stream.sink.iceberg;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.iceberg.flink.sink.FlinkSink;

import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * <a href="https://iceberg.apache.org/docs/latest/flink/">iceberg</a>
 * <p>
 * <li>cdc 数据源格式 source ：Tuple3<String, RowKind, String> = Tuple3<表名, RowKind, row json 序列化数据>
 * <li>IcebergSink： 根据配置表信息加载 catalog 信息，获取每个表的 schema、rowType 信息。
 * <li>IcebergSink： 按表名分流写入目标表：
 * source.filter(表名).map(org.apache.flink.formats.json.JsonRowDataDeserializationSchema 反序列化为
 * RowData).addSink(对应的目标表或者 flink-table)
 * <p>
 * <p>
 * 配置示例：norns-template/src/main/resources/template/connector.mysql-cdc.pipeline-job.template.conf
 *
 * @author Li.Wei by 2022/5/17
 * @see IcebergSinkOptions
 */
@Slf4j
public class IcebergCdcSink {

    /**
     * sink iceberg table 配置信息
     */
    private Set<TableLoadSchemaMsg> tableLoadSchemaMsg = Sets.newHashSet();

    public void prepare() {
        // 根据场景初始化配置信息
        tableLoadSchemaMsg = CatalogUtil.initTableLoadMsg(null);
    }

    public void compute(DataStream<Tuple3<String, RowKind, String>> input, StreamTableEnvironment tableEnv) {
        for (TableLoadSchemaMsg ss : tableLoadSchemaMsg) {
            final JsonRowDataDeserializationSchema deserializationSchema = ss.getJsonRowDataDeserializationSchema();
            final SingleOutputStreamOperator<RowData> ds = input
                .filter(value -> value.f0.equals(ss.getTableSettingConfig().getSubscriptionTableName()))
                .map(value -> {
                    final RowData rowData = deserializationSchema.deserialize(value.f2.getBytes(StandardCharsets.UTF_8));
                    rowData.setRowKind(value.f1);
                    return rowData;
                }, deserializationSchema.getProducedType());

            final TableLoadSchemaMsg.TableSettingConfig tableSettingConfig = ss.getTableSettingConfig();
            FlinkSink.forRowData(ds)
                .upsert(tableSettingConfig.isUpset())
                .distributionMode(tableSettingConfig.getDistributionMode())
                .tableLoader(ss.getTableLoader())
                .equalityFieldColumns(tableSettingConfig.getEqualityFieldColumns())
                .append();
        }
    }

}
