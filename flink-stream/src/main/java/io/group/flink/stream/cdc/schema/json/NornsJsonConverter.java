package io.group.flink.stream.cdc.schema.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.debezium.time.Date;
import io.debezium.time.Timestamp;
import io.debezium.time.*;
import lombok.Getter;
import org.apache.flink.table.data.TimestampData;
import org.apache.kafka.connect.data.Time;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIMESTAMP_FORMAT;
import static org.apache.flink.formats.common.TimeFormats.RFC3339_TIME_FORMAT;

/**
 * 自定义 json 转换器，在 {@link  JsonConverter} 基础上扩展了 {@link #LOGICAL_CONVERTERS} 对时间的解析。
 * <p>
 * 说明：
 * <li>默认使用 RFC3339_TIMESTAMP_FORMAT UTC 格式进行时间格式化
 * <li>目前仅支持序列化，主要用于 cdc 写出 changelog-json 解析后分流到不同目的端。
 * <li>如果为 sink 表使用 debezium-json 格式反序列化，请慎重使用（推荐直接使用官方默认序列化器），使用时请测试序列化一致性。
 *
 * @author Li.Wei by 2022/6/14
 */
@Getter
public class NornsJsonConverter extends JsonConverter {
    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final JsonNodeFactory JSON_NODE_FACTORY = JsonNodeFactory.withExactBigDecimals(true);
    // 自定义特殊数据类型转换器 key=schema.name
    private static final HashMap<String, LogicalTypeConverter> LOGICAL_CONVERTERS = new HashMap<>();

    static {
        LOGICAL_CONVERTERS.put(Decimal.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof BigDecimal))
                throw new DataException("Invalid type for Decimal, expected BigDecimal but was " + value.getClass());

            final BigDecimal decimal = (BigDecimal) value;
            switch (config.decimalFormat()) {
                case NUMERIC:
                    return JSON_NODE_FACTORY.numberNode(decimal);
                case BASE64:
                    return JSON_NODE_FACTORY.binaryNode(Decimal.fromLogical(schema, decimal));
                default:
                    throw new DataException("Unexpected " + JsonConverterConfig.DECIMAL_FORMAT_CONFIG + ": " + config.decimalFormat());
            }
        });

        LOGICAL_CONVERTERS.put(org.apache.kafka.connect.data.Date.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Date, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(org.apache.kafka.connect.data.Date.fromLogical(schema, (java.util.Date) value));
        });

        LOGICAL_CONVERTERS.put(Time.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Time, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(Time.fromLogical(schema, (java.util.Date) value));
        });

        LOGICAL_CONVERTERS.put(org.apache.kafka.connect.data.Timestamp.LOGICAL_NAME, (schema, value, config) -> {
            if (!(value instanceof java.util.Date))
                throw new DataException("Invalid type for Timestamp, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.numberNode(org.apache.kafka.connect.data.Timestamp.fromLogical(schema, (java.util.Date) value));
        });
        // --------------------------------------------
        // 自定义时间特殊处理
        // --------------------------------------------

        // io.debezium.time.Date 日期类型
        LOGICAL_CONVERTERS.put(Date.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Integer))
                throw new DataException("Invalid type for Integer, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(LocalDate.ofEpochDay((Integer) value).toString());
        });

        LOGICAL_CONVERTERS.put(Timestamp.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Long, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(TimestampData.fromEpochMillis((Long) value)
                .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT)
            );
        });
        LOGICAL_CONVERTERS.put(MicroTime.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Long, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(TimestampData.fromEpochMillis(TimeUnit.MICROSECONDS.toMillis((long) value))
                .toLocalDateTime().format(RFC3339_TIME_FORMAT));
        });
        LOGICAL_CONVERTERS.put(NanoTime.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Long, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(TimestampData.fromEpochMillis(TimeUnit.NANOSECONDS.toMillis((long) value))
                .toLocalDateTime().format(RFC3339_TIME_FORMAT));
        });
        LOGICAL_CONVERTERS.put(MicroTimestamp.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Long, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(TimestampData.fromEpochMillis(TimeUnit.MICROSECONDS.toMillis((long) value))
                .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT));
        });
        LOGICAL_CONVERTERS.put(NanoTimestamp.SCHEMA_NAME, (schema, value, config) -> {
            if (!(value instanceof Long))
                throw new DataException("Invalid type for Long, expected Date but was " + value.getClass());
            return JSON_NODE_FACTORY.textNode(TimestampData.fromEpochMillis(TimeUnit.NANOSECONDS.toMillis((long) value))
                .toLocalDateTime().format(RFC3339_TIMESTAMP_FORMAT));
        });
    }

    private JsonConverterConfig config;

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object value) {
        try {
            return objectMapper.writeValueAsBytes(convertToJsonNode(schema, value));
        } catch (JsonProcessingException e) {
            throw new DataException("Converting Kafka Connect data to byte[] failed due to serialization error: ", e);
        }
    }

    public String convertToJsonString(Schema schema, Object value) throws JsonProcessingException {
        return new String(objectMapper.writeValueAsBytes(convertToJsonNode(schema, value)), StandardCharsets.UTF_8);
    }

    private JsonNode convertToJsonNode(Schema schema, Object value) {
        if (value == null) {
            if (schema == null) // Any schema is valid and we don't have a default, so treat this as an optional schema
                return null;
            if (schema.defaultValue() != null)
                return convertToJsonNode(schema, schema.defaultValue());
            if (schema.isOptional())
                return JSON_NODE_FACTORY.nullNode();
            throw new DataException("Conversion error: null value for field that is required and has no default value");
        }

        if (schema != null && schema.name() != null) {
            LogicalTypeConverter logicalConverter = LOGICAL_CONVERTERS.get(schema.name());
            if (logicalConverter != null)
                return logicalConverter.toJson(schema, value, config);
        }

        final Schema.Type schemaType;
        if (schema == null) {
            schemaType = ConnectSchema.schemaType(value.getClass());
            if (schemaType == null)
                throw new DataException("Java class " + value.getClass() + " does not have corresponding schema type.");
        } else {
            schemaType = schema.type();
        }

        switch (schemaType) {
            case INT8:
                return JSON_NODE_FACTORY.numberNode((Byte) value);
            case INT16:
                return JSON_NODE_FACTORY.numberNode((Short) value);
            case INT32:
                return JSON_NODE_FACTORY.numberNode((Integer) value);
            case INT64:
                return JSON_NODE_FACTORY.numberNode((Long) value);
            case FLOAT32:
                return JSON_NODE_FACTORY.numberNode((Float) value);
            case FLOAT64:
                return JSON_NODE_FACTORY.numberNode((Double) value);
            case BOOLEAN:
                return JSON_NODE_FACTORY.booleanNode((Boolean) value);
            case STRING:
                CharSequence charSeq = (CharSequence) value;
                return JSON_NODE_FACTORY.textNode(charSeq.toString());
            case BYTES:
                if (value instanceof byte[])
                    return JSON_NODE_FACTORY.binaryNode((byte[]) value);
                else if (value instanceof ByteBuffer)
                    return JSON_NODE_FACTORY.binaryNode(((ByteBuffer) value).array());
                else
                    throw new DataException("Invalid type for bytes type: " + value.getClass());
            case ARRAY: {
                final List<?> collection = (List<?>) value;
                final ArrayNode list = JSON_NODE_FACTORY.arrayNode();
                for (Object elem : collection) {
                    final Schema valueSchema = schema.valueSchema();
                    final JsonNode fieldValue = convertToJsonNode(valueSchema, elem);
                    list.add(fieldValue);
                }
                return list;
            }
            case MAP: {
                Map<?, ?> map = (Map<?, ?>) value;
                // If true, using string keys and JSON object; if false, using non-string keys and Array-encoding
                boolean objectMode;
                if (schema == null) {
                    objectMode = true;
                    for (Map.Entry<?, ?> entry : map.entrySet()) {
                        if (!(entry.getKey() instanceof String)) {
                            objectMode = false;
                            break;
                        }
                    }
                } else {
                    objectMode = schema.keySchema().type() == Schema.Type.STRING;
                }
                ObjectNode obj = null;
                ArrayNode list = null;
                if (objectMode)
                    obj = JSON_NODE_FACTORY.objectNode();
                else
                    list = JSON_NODE_FACTORY.arrayNode();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    Schema keySchema = schema == null ? null : schema.keySchema();
                    Schema valueSchema = schema == null ? null : schema.valueSchema();
                    JsonNode mapKey = convertToJsonNode(keySchema, entry.getKey());
                    JsonNode mapValue = convertToJsonNode(valueSchema, entry.getValue());

                    if (objectMode)
                        obj.set(mapKey.asText(), mapValue);
                    else
                        list.add(JSON_NODE_FACTORY.arrayNode().add(mapKey).add(mapValue));
                }
                return objectMode ? obj : list;
            }
            case STRUCT: {
                Struct struct = (Struct) value;
                if (!struct.schema().equals(schema))
                    throw new DataException("Mismatching schema.");
                ObjectNode obj = JSON_NODE_FACTORY.objectNode();
                for (Field field : schema.fields()) {
                    obj.set(field.name(), convertToJsonNode(field.schema(), struct.get(field)));
                }
                return obj;
            }
            default:
                throw new DataException("Couldn't convert " + value + " to JSON.");
        }
    }


    @Override
    public void configure(Map<String, ?> configs) {
        config = new JsonConverterConfig(configs);

        objectMapper.setNodeFactory(JSON_NODE_FACTORY);
    }


    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {
        return null;
    }

    private interface LogicalTypeConverter {
        JsonNode toJson(Schema schema, Object value, JsonConverterConfig config);
    }
}
