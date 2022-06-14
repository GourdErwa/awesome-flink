package io.group.flink.stream.cdc.schema;

import org.apache.kafka.connect.json.JsonConverter;

import java.util.Map;

/**
 * 功能说明参考 {@link  TableIdentifierDebeziumJsonDeserializationSchemaBase}。
 * 使用官方提供 {@link JsonConverter} 进行序列化。
 *
 * @author Li.Wei by 2022/6/14
 */
public class TableIdentifierDebeziumJsonDeserializationSchemaV1
    extends TableIdentifierDebeziumJsonDeserializationSchemaBase {

    @Override
    protected JsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs) {
        final JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(customConverterConfigs);
        return jsonConverter;
    }
}
