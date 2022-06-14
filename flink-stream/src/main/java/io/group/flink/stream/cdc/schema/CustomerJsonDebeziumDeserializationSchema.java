package io.group.flink.stream.cdc.schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.debezium.time.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.time.LocalDate;
import java.util.List;
import java.util.Map;

import static io.debezium.data.Envelope.FieldName.*;
import static io.group.flink.stream.constant.DebeziumJsonConstant.TABLE;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIME_FORMAT;

/**
 * Tuple3<String, RowKind, String> = Tuple3<表名, RowKind, 表单行数据 json 字符串>
 * <p>
 * 目前不支持 after、before 节点包含嵌套数据结构解析。
 *
 * <p>输出示例</p>
 * <pre><code>
 * (time_type,DELETE,{"f_date":"2022-06-09","f_time":"10:39:39","f_timestap":"2022-06-09T02:39:39Z","f_unix_millisecond":"1654742379000","id":"3","f_unix":"1654742379"})
 * (time_type,INSERT,{"f_date":"2022-06-09","f_time":"10:39:39","f_timestap":"2022-06-09T02:39:39Z","f_unix_millisecond":"1654742379000","id":"4","f_unix":"1654742379"})
 * </pre>
 *
 * @author Li.Wei by 2022/5/17
 * @deprecated use {@link RowKindJsonDeserializationSchemaV2}
 */
public class CustomerJsonDebeziumDeserializationSchema
    implements DebeziumDeserializationSchema<Tuple3<String, RowKind, String>> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple3<String, RowKind, String>> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        Struct value = (Struct) sourceRecord.value();
        final Struct source = value.getStruct(SOURCE);
        String table = source.getString(TABLE);
        Schema valueSchema = sourceRecord.valueSchema();
        if (op != Envelope.Operation.CREATE && op != Envelope.Operation.READ) {
            if (op == Envelope.Operation.DELETE) {
                this.emit(Tuple3.of(table, RowKind.DELETE, this.extractBeforeRow(value, valueSchema)), out);
            } else {
                this.emit(Tuple3.of(table, RowKind.UPDATE_BEFORE, this.extractBeforeRow(value, valueSchema)), out);
                this.emit(Tuple3.of(table, RowKind.UPDATE_AFTER, this.extractAfterRow(value, valueSchema)), out);
            }
        } else {
            this.emit(Tuple3.of(table, RowKind.INSERT, this.extractAfterRow(value, valueSchema)), out);
        }
    }

    private Map<String, Object> extractAfterRow(Struct value, Schema valueSchema) {
        return builderStringObjectMap(valueSchema.field(AFTER).schema(), value.getStruct(AFTER));
    }

    private Map<String, Object> extractBeforeRow(Struct value, Schema valueSchema) {
        return builderStringObjectMap(valueSchema.field(BEFORE).schema(), value.getStruct(BEFORE));
    }

    protected Map<String, Object> builderStringObjectMap(Schema schema, Struct struct) {
        final List<Field> fields = schema.fields();
        final Map<String, Object> r = Maps.newHashMapWithExpectedSize(fields.size());
        for (Field field : fields) {
            Object o = struct.get(field);
            if (o == null) {
                continue; // 空值忽略
            }

            // 嵌套解析
            final Schema fSchema = field.schema();

            Object v;
            if (fSchema.type() == Schema.Type.STRUCT) {
                v = this.builderStringObjectMap(fSchema, (Struct) o);
            } else if (fSchema.type() == Schema.Type.ARRAY) {
                final List<?> collection = (List<?>) o;
                final List<Object> list = Lists.newArrayListWithCapacity(((List<?>) o).size());
                for (Object elem : collection) {
                    final Schema valueSchema = fSchema.valueSchema();
                    final Object fieldValue = convertToObject(valueSchema, elem);
                    list.add(fieldValue);
                }
                v = list;
            } else if (fSchema.type() == Schema.Type.MAP) {
                Map<?, ?> map = (Map<?, ?>) o;
                Map<Object, Object> rMap = Maps.newHashMapWithExpectedSize(map.size());
                // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    rMap.put(
                        convertToObject(fSchema.keySchema(), entry.getKey()),
                        convertToObject(fSchema.valueSchema(), entry.getValue())
                    );
                }
                v = rMap;
            } else {
                v = convertToObject(fSchema, o);
            }
            r.put(field.name(), v);
        }
        return r;
    }

    /**
     * 时间类型特殊处理，io.debezium.time 包下时间以 fromEpochMillis/Micro 时间传输
     *
     * @param fSchema 当前字段 schema
     * @param fVal    当前字段值
     * @return 当前字段特殊解析后的值
     */
    private static Object convertToObject(Schema fSchema, Object fVal) {
        // 时间特殊处理 , 自增长主键时 field.schema().name 可能为空
        final String schemaName = fSchema.name();
        if (schemaName == null) {
            return fVal;
        }

        if (fVal instanceof Long) {
            switch (schemaName) {
                case Timestamp.SCHEMA_NAME:
                    fVal = TimestampData.fromEpochMillis((Long) fVal)
                        .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                    break;
                case MicroTime.SCHEMA_NAME: {  // TIME
                    long micro = (long) fVal;
                    fVal = TimestampData.fromEpochMillis(
                        micro / 1000, 0).toLocalDateTime().toLocalTime().format(RFC3339_TIME_FORMAT);
                    break;
                }
                case MicroTimestamp.SCHEMA_NAME: {
                    long micro;
                    micro = (long) fVal;
                    fVal = TimestampData.fromEpochMillis(
                            micro / 1000, (int) (micro % 1000 * 1000))
                        .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                    break;
                }
                case NanoTimestamp.SCHEMA_NAME:
                    long nano = (long) fVal;
                    fVal = TimestampData.fromEpochMillis(
                            nano / 1000_000, (int) (nano % 1000_000))
                        .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                    break;
                default:
                    break;
            }
        } else if (fVal instanceof Integer) {
            if (Date.SCHEMA_NAME.equals(schemaName)) { // Date
                fVal = LocalDate.ofEpochDay((Integer) fVal);
            }
        }
        return fVal;
    }

    private void emit(Tuple3<String, RowKind, Map<String, Object>> row,
                      Collector<Tuple3<String, RowKind, String>> collector)
        throws JsonProcessingException {
        // f2 序列化为 json 字符串
        collector.collect(Tuple3.of(row.f0, row.f1, OBJECT_MAPPER.writeValueAsString(row.f2)));
    }

    @Override
    public TypeInformation<Tuple3<String, RowKind, String>> getProducedType() {
        return TypeInformation.of(new TypeHint<Tuple3<String, RowKind, String>>() {
        });
    }
}
