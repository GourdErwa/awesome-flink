package io.group.flink.stream.sink.iceberg;

import com.google.common.base.Splitter;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonRowDataDeserializationSchema;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iceberg.CatalogProperties.CACHE_ENABLED;
import static org.apache.iceberg.flink.FlinkCatalogFactory.*;

/**
 * @author Li.Wei by 2022/5/18
 */
@Slf4j
public class CatalogUtil {
    private CatalogUtil() {
    }

    //  tableLoader 与 icebergTable、flinkSchema 字段二选一即可
    public static Set<TableLoadSchemaMsg> initTableLoadMsg(Configuration configuration) {
        CatalogLoader catalogLoader = createCatalogLoader(configuration);
        final Catalog catalog = catalogLoader.loadCatalog();
        final String catalogName = catalog.name();
        final Set<TableLoadSchemaMsg> schemaMsgSet =
            TableLoadSchemaMsg.TableSettingConfig.builder(null)
                .stream()
                .map(tsc -> {
                    final List<String> tables = Splitter.on(".").trimResults().splitToList(tsc.getTableIdentifier());
                    final TableIdentifier tableIdentifier = TableIdentifier.of(tables.get(0), tables.get(1));
                    log.info("load tableIdentifier:{}", tableIdentifier);
                    final Table table = catalog.loadTable(tableIdentifier);
                    final TableLoader tableLoader = TableLoader.fromCatalog(catalogLoader, tableIdentifier);
                    final org.apache.iceberg.Schema schema = table.schema();
                    // json-RowData 序列化
                    final RowType rowType = FlinkSchemaUtil.convert(schema);
                    final JsonRowDataDeserializationSchema jsonRowDataDeserializationSchema =
                        new JsonRowDataDeserializationSchema(rowType, InternalTypeInfo.of(rowType),
                            false, false, TimestampFormat.ISO_8601);

                    return TableLoadSchemaMsg.builder()
                        .fullTableName(catalogName + "." + tsc.getTableIdentifier())
                        //.tableName(tableIdentifier.name())
                        .tableSettingConfig(tsc)
                        .tableLoader(tableLoader)
                        .icebergTable(table)
                        .icebergSchema(schema)
                        //.flinkSchema(schema)
                        //.flinkTableSchema(tableSchema)
                        .jsonRowDataDeserializationSchema(jsonRowDataDeserializationSchema)
                        .build();
                }).collect(Collectors.toSet());

        log.info("init tableLoadSettings size: {}, schemaSettings: {}", schemaMsgSet.size(), schemaMsgSet);
        return schemaMsgSet;
    }

    /**
     * 构建 CatalogLoader
     *
     * @param configuration configuration
     * @return CatalogLoader
     */
    public static CatalogLoader createCatalogLoader(Configuration configuration) {
        final Map<String, String> properties = configuration.get(IcebergSinkOptions.CATALOG_PROPERTIES);
        final String catalogName = properties.get(IcebergSinkOptions.CATALOG_NAME.key());
        final String catalogType = properties.get(FlinkCatalogFactory.ICEBERG_CATALOG_TYPE);
        CatalogLoader catalogLoader;
        switch (catalogType) {
            case ICEBERG_CATALOG_TYPE_HIVE:
                catalogLoader = CatalogLoader.hive(catalogName, new org.apache.hadoop.conf.Configuration(), properties);
                break;
            case ICEBERG_CATALOG_TYPE_HADOOP:
                catalogLoader = CatalogLoader.hadoop(catalogName, new org.apache.hadoop.conf.Configuration(), properties);
                break;
            default:
                throw new RuntimeException("catalogLoader error. catalogName[" + catalogName + "] not found!");
        }
        return catalogLoader;
    }

    public static org.apache.flink.table.catalog.Catalog createFlinkCatalog(Configuration configuration) {
        CatalogLoader catalogLoader = createCatalogLoader(configuration);
        final String catalogName = configuration.getString(IcebergSinkOptions.CATALOG_NAME);
        final Map<String, String> properties = configuration.get(IcebergSinkOptions.CATALOG_PROPERTIES);

        String defaultDatabase = properties.getOrDefault(DEFAULT_DATABASE, DEFAULT_DATABASE_NAME);

        Namespace baseNamespace = Namespace.empty();
        if (properties.containsKey(BASE_NAMESPACE)) {
            baseNamespace = Namespace.of(properties.get(BASE_NAMESPACE).split("\\."));
        }
        boolean cacheEnabled = Boolean.parseBoolean(properties.getOrDefault(CACHE_ENABLED, "true"));
        return new FlinkCatalog(catalogName, defaultDatabase, baseNamespace, catalogLoader, cacheEnabled, 1000L);
    }
}
