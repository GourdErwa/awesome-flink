package io.group.flink.stream.cdc.schema;


import io.group.flink.stream.cdc.schema.json.NornsJsonConverter;

import java.util.Map;

/**
 * 功能说明参考 {@link  RowKindJsonDeserializationSchemaBase}。
 * 使用自定义扩展 {@link NornsJsonConverter} 进行序列化。
 *
 * @author Li.Wei by 2022/5/17
 */
public class RowKindJsonDeserializationSchemaV2
    extends RowKindJsonDeserializationSchemaBase {

    @Override
    protected NornsJsonConverter initializeJsonConverter(Map<String, Object> customConverterConfigs) {
        final NornsJsonConverter jsonConverter = new NornsJsonConverter();
        jsonConverter.configure(customConverterConfigs);
        return jsonConverter;
    }
}
