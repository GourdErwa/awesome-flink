package io.group.flink.stream.sink.iceberg;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.Data;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.flink.TableLoader;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

/**
 * iceberg 载入 catalog 获取表相关信息
 *
 * @author Li.Wei by 2022/5/18
 */
@Data
@Builder
public class TableLoadSchemaMsg implements Serializable, Comparable<TableLoadSchemaMsg> {
    // catalog.db.表名
    private String fullTableName;
    private TableSettingConfig tableSettingConfig;
    private TableLoader tableLoader;
    private transient Table icebergTable;
    private transient Schema icebergSchema;

    private JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema;

    @Override
    public int compareTo(@NotNull TableLoadSchemaMsg o) {
        return tableSettingConfig.subscriptionTableName.compareTo(o.tableSettingConfig.subscriptionTableName);
    }

    /**
     * <code>配置示例：<pre>
     * table-settings=[
     *     {
     *     table-identifier="iceberg_db_sample.common_user"
     *     subscription-table-name="common_user"
     *     upsert=true
     *     equality-field-columns="id;dt"
     *     distribution-mode=HASH
     *     },
     *     {
     *      table-identifier="iceberg_db_sample.order"
     *      subscription-table-name="order"
     *      upsert=true
     *      equality-field-columns="id;dt"
     *      distribution-mode=HASH
     *      }
     * ]
     * </pre>
     * </code>
     */
    @Data
    public static class TableSettingConfig implements Serializable {
        private String tableIdentifier;
        // 未配置默认等于 tableIdentifier 中表名
        private String subscriptionTableName;
        private boolean upset = false;
        private DistributionMode distributionMode = DistributionMode.NONE;
        private List<String> equalityFieldColumns = ImmutableList.of();

        public TableSettingConfig() {
        }

        public static List<TableSettingConfig> builder(Object config) {
            // TODO 根据配置信息初始化需要 sink 的表配置
            return Collections.emptyList();
        }
    }
}
