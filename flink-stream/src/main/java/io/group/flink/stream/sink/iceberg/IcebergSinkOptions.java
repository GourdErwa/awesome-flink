package io.group.flink.stream.sink.iceberg;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.iceberg.DistributionMode;

import java.util.List;
import java.util.Map;

/**
 * @author Li.Wei by 2022/5/17
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IcebergSinkOptions {

    public static final ConfigOption<String> CATALOG_NAME =
        ConfigOptions.key("catalog-name").stringType().noDefaultValue()
            .withDescription("catalog name.");
    public static final ConfigOption<Map<String, String>> CATALOG_PROPERTIES =
        ConfigOptions.key("catalog-properties").mapType().noDefaultValue()
            .withDescription("CatalogLoader.hive/hadoop(name,hadoopConf,properties).");

    public static final ConfigOption<List<String>> TABLE_SETTINGS =
        ConfigOptions.key("table-settings").stringType().asList().noDefaultValue()
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");

    public static final ConfigOption<String> TABLE_IDENTIFIER =
        ConfigOptions.key("table-identifier").stringType().noDefaultValue()
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");
    public static final ConfigOption<String> SUBSCRIPTION_TABLE_NAME =
        ConfigOptions.key("subscription-table-name").stringType().noDefaultValue()
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");
    public static final ConfigOption<Boolean> UPSERT =
        ConfigOptions.key("upsert").booleanType().defaultValue(true)
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");
    public static final ConfigOption<List<String>> EQUALITY_FIELD_COLUMNS =
        ConfigOptions.key("equality-field-columns").stringType().asList().noDefaultValue()
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");
    public static final ConfigOption<DistributionMode> DISTRIBUTION_MODE =
        ConfigOptions.key("distribution-mode").enumType(DistributionMode.class).defaultValue(DistributionMode.HASH)
            .withDescription("参考 TableSettingConfig 注释说明，配置执行表参数信息");
}
