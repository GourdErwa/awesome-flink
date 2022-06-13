package io.group.flink.stream.cdc.mysql;

import com.google.common.collect.Maps;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import io.debezium.time.MicroTimestamp;
import io.debezium.time.NanoTimestamp;
import io.debezium.time.Timestamp;
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

import java.util.List;
import java.util.Map;

import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIMESTAMP_FORMAT;

/**
 * Tuple3<String, RowKind, String> = Tuple3<表名, RowKind, 表单行数据 json 字符串>
 *
 * <p>输出示例</p>
 * <pre><code>
 * (order,INSERT,{"user_id":6,"creat_time":1653414082000,"price":53,"goods_id":"cv_1246","pay_type":1,"id":4,"pay_time":1653414084000})
 * (order,INSERT,{"user_id":4,"creat_time":1653315501000,"price":52,"goods_id":"cv_1246","pay_type":1,"id":2,"pay_time":1653401906000})
 * (order,INSERT,{"user_id":7,"creat_time":1653381471000,"price":52,"goods_id":"cv_1246","pay_type":0,"id":3,"pay_time":1653381473000})
 * (order,INSERT,{"user_id":1,"creat_time":1653315501000,"price":52,"goods_id":"cv_1245","pay_type":1,"id":1,"pay_time":1653401906000})
 * </pre>
 *
 * TODO 序列化时注意时间序列化方式。
 *
 * @author Li.Wei by 2022/5/17
 */
public class CustomerJsonDebeziumDeserializationSchema
    implements DebeziumDeserializationSchema<Tuple3<String, RowKind, String>> {
    private static final long serialVersionUID = 2L;
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<Tuple3<String, RowKind, String>> out) throws Exception {

        Envelope.Operation op = Envelope.operationFor(sourceRecord);
        Struct value = (Struct) sourceRecord.value();
        final Struct source = value.getStruct("source");
        String table = source.getString("table");
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
        return builderStringObjectMap(valueSchema.field("after").schema(), value.getStruct("after"));
    }

    private Map<String, Object> extractBeforeRow(Struct value, Schema valueSchema) {
        return builderStringObjectMap(valueSchema.field("before").schema(), value.getStruct("before"));
    }

    private Map<String, Object> builderStringObjectMap(Schema schema, Struct struct) {
        final List<Field> fields = schema.fields();
        final Map<String, Object> r = Maps.newHashMapWithExpectedSize(fields.size());
        for (Field field : fields) {
            final String name = field.name();
            Object o = struct.get(field);
            if (o == null) {
                continue;
            }
            // 时间特殊处理
            if (o instanceof Long) {
                switch (field.schema().name()) {
                    case Timestamp.SCHEMA_NAME:
                        o = TimestampData.fromEpochMillis((Long) o)
                            .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                        break;
                    case MicroTimestamp.SCHEMA_NAME:
                        long micro = (long) o;
                        o = TimestampData.fromEpochMillis(
                                micro / 1000, (int) (micro % 1000 * 1000))
                            .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                        break;
                    case NanoTimestamp.SCHEMA_NAME:
                        long nano = (long) o;
                        o = TimestampData.fromEpochMillis(
                                nano / 1000_000, (int) (nano % 1000_000))
                            .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT);
                        break;
                    default:
                        break;
                }
            }
            r.put(name, o.toString());
        }
        return r;
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
