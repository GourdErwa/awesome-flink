package io.group.flink.stream.cdc.schema;

import com.google.common.collect.Maps;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.group.flink.stream.constant.DebeziumJsonConstant;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.json.DecimalFormat;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.storage.ConverterType;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.connect.json.JsonConverterConfig.DECIMAL_FORMAT_CONFIG;
import static org.apache.kafka.connect.json.JsonConverterConfig.SCHEMAS_ENABLE_CONFIG;

/**
 * 自定义 JSON 序列化器，{@link TableIdentifierDebeziumJson}。
 * <p>
 * 主要特征：
 * <li>在 debezium-json 基础上提取出 db、table 字段，可用于分流时指定对应目的表。
 *
 * <p>输出示例</p>
 * <pre><code>
 * 1> TableIdentifierDebeziumJson(
 * db=example_cdc,
 * table=time_type,
 * json={
 *   "before": null,
 *   "after": {
 *     "id": "fdJHzA==",
 *     "f_date": 19152,
 *     "f_time": 39743000000,
 *     "f_timestap": "2022-06-09T03:02:23Z",
 *     "f_unix": 1654743743,
 *     "f_unix_millisecond": 1654743743000
 *   },
 *   "source": {
 *     "version": "1.5.4.Final",
 *     "connector": "mysql",
 *     "name": "mysql_binlog_source",
 *     "ts_ms": 0,
 *     "snapshot": "false",
 *     "db": "example_cdc",
 *     "sequence": null,
 *     "table": "time_type",
 *     "server_id": 0,
 *     "gtid": null,
 *     "file": "",
 *     "pos": 0,
 *     "row": 0,
 *     "thread": null,
 *     "query": null
 *   },
 *   "op": "r",
 *   "ts_ms": 1655109754821,
 *   "transaction": null
 * }
 * </pre>
 *
 * @author Li.Wei by 2022/5/17
 * @see TableIdentifierDebeziumJsonDeserializationSchemaV1
 * @see TableIdentifierDebeziumJsonDeserializationSchemaV2
 */
@Getter
public abstract class TableIdentifierDebeziumJsonDeserializationSchemaBase
    implements DebeziumDeserializationSchema<TableIdentifierDebeziumJsonDeserializationSchemaBase.TableIdentifierDebeziumJson> {
    private transient JsonConverter jsonConverter;
    private final boolean includeSchema;
    private final Map<String, Object> customConverterConfigs = Maps.newHashMap();


    public TableIdentifierDebeziumJsonDeserializationSchemaBase() {
        this(false);
    }

    public TableIdentifierDebeziumJsonDeserializationSchemaBase(boolean includeSchema) {
        this(includeSchema, Collections.emptyMap());
    }

    public TableIdentifierDebeziumJsonDeserializationSchemaBase(boolean includeSchema, Map<String, Object> customConverterConfigs) {
        this.includeSchema = includeSchema;
        this.customConverterConfigs.put("converter.type", ConverterType.VALUE.getName());
        this.customConverterConfigs.put(SCHEMAS_ENABLE_CONFIG, isIncludeSchema());
        this.customConverterConfigs.put(DECIMAL_FORMAT_CONFIG, DecimalFormat.NUMERIC.name());
        this.customConverterConfigs.putAll(customConverterConfigs);
    }

    abstract JsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs);

    @Override
    public void deserialize(SourceRecord sr, Collector<TableIdentifierDebeziumJson> out) {
        if (this.jsonConverter == null) {
            this.jsonConverter = this.initializeJsonConverter(this.customConverterConfigs);
        }
        Struct value = (Struct) sr.value();
        final Struct source = value.getStruct(DebeziumJsonConstant.SOURCE);
        final String db = source.getString(DebeziumJsonConstant.DB);
        final String table = source.getString(DebeziumJsonConstant.TABLE);

        final byte[] bytes = this.jsonConverter.fromConnectData(sr.topic(), sr.valueSchema(), sr.value());
        out.collect(new TableIdentifierDebeziumJson(db, table, new String(bytes)));
    }

    @Override
    public TypeInformation<TableIdentifierDebeziumJson> getProducedType() {
        return TypeInformation.of(TableIdentifierDebeziumJson.class);
    }

    /**
     * 提取出 [db、table、debezium-json] 序列化结果
     *
     * @author Li.Wei by 2022/6/13
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class TableIdentifierDebeziumJson implements Serializable {
        private String db;
        private String table;
        // debezium-json
        private String json;
    }
}
