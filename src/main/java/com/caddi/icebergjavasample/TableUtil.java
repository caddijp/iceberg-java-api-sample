package com.caddi.icebergjavasample;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;

import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class TableUtil {

  private TableUtil() {}

  public static RESTCatalog getCatalog(String catalogUri) {

    var catalog = new RESTCatalog();
    Map<String, String> catalogConfig = new HashMap<>();
    catalogConfig.put("type", "rest");
    catalogConfig.put("uri", catalogUri);

    //TODO, 実際の環境に合わせて設定内容を変えること
    catalogConfig.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
    catalogConfig.put("s3.endpoint", "http://localhost:9001");
    catalogConfig.put("s3.path-style-access", "true");
    catalogConfig.put("s3.region", "us-east-2");
    catalogConfig.put("s3.access-key-id", "admin");
    catalogConfig.put("s3.secret-access-key", "password");

    catalog.initialize("rest", catalogConfig);

    return catalog;
  }

  public static Table getOrCreateTableAndNamespace(
      RESTCatalog catalog,
      String namespace,
      String table,
      Schema schema,
      PartitionSpec partitionSpec,
      SortOrder sortOrder) {
    if (!catalog.namespaceExists(Namespace.of(namespace))) {
      catalog.createNamespace(Namespace.of(namespace));
    }
    var ns = catalog.loadNamespaceMetadata(Namespace.of(namespace));

    if (!catalog.tableExists(TableIdentifier.of(Namespace.of(namespace), table))) {

      var location =
          ns.get("location") + "/" + table + "-" + UUID.randomUUID().toString().replaceAll("-", "");

      return catalog
          .buildTable(TableIdentifier.of(Namespace.of(namespace), table), schema)
          .withLocation(location)
          .withPartitionSpec(partitionSpec)
          .withSortOrder(sortOrder)
          .withProperties(
              Map.of(
                  "write.metadata.delete-after-commit.enabled", "true",
                  "write.metadata.previous-versions-max", "100",
                  "write.object-storage.enabled", "true"))
          .create();
    } else {
      return getTable(catalog, namespace, table);
    }
  }

  public static Table getTable(RESTCatalog catalog, String namespace, String table) {

    var tbl = catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), table));
    tbl.updateProperties()
        .set("write.metadata.delete-after-commit.enabled", "true")
        .set("write.metadata.previous-versions-max", "100")
        .set("write.object-storage.enabled", "true")
        .commit();
    return catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), table));
  }


  public static Map<String, Object> convertRecord(Schema schema, Map<?, ?> src) {
    var r = new HashMap<String, Object>();

    for (Types.NestedField col : schema.columns()) {

      if (!src.containsKey(col.name())) {
        continue;
      }
      var srcValue = src.get(col.name());
      if (srcValue == null) {
        r.put(col.name(), null);
        continue;
      }
      var value =
          switch (col.type()) {
            case Types.UUIDType t -> UUID.fromString(srcValue.toString());
            case Types.StringType t -> srcValue.toString();
            case Types.BooleanType t -> (boolean) srcValue;
            case Types.TimestampType t -> OffsetDateTime.parse(srcValue.toString());
            case Types.LongType t ->
                srcValue.getClass().equals(Integer.class) ? (long) (int) srcValue : (long) srcValue;
            case Types.IntegerType t -> (int) srcValue;
            case Types.FloatType t -> (float) srcValue;
            case Types.DoubleType t -> (double) srcValue;
            case Types.DecimalType t -> new BigDecimal(srcValue.toString());
            default -> throw new IllegalStateException("Unexpected type: " + col.type());
          };
      r.put(col.name(), value);
    }
    return r;
  }
}
