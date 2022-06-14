package io.group.flink.stream.cdc.schema;

import org.apache.kafka.connect.json.JsonConverter;

import java.util.Map;

/**
 * 功能说明参考 {@link  RowKindJsonDeserializationSchemaBase}。
 * 使用官方提供 {@link JsonConverter} 进行序列化。
 *
 * @author Li.Wei by 2022/6/13
 */
public class RowKindJsonDeserializationSchemaV1
    extends RowKindJsonDeserializationSchemaBase {

    @Override
    protected JsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs) {
        final JsonConverter jsonConverter = new JsonConverter();
        jsonConverter.configure(customConverterConfigs);
        return jsonConverter;
    }

}
