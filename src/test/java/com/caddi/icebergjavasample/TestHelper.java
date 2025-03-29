package com.caddi.icebergjavasample;

import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.expressions.Expressions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestHelper {

  public static List<Map<String, Object>> getData(String namespace, String tableName)
      throws IOException {
    var catalog = TableUtil.getCatalog("http://localhost:8181");
    var table = catalog.loadTable(TableIdentifier.of(Namespace.of(namespace), tableName));

    var result = new ArrayList<Map<String, Object>>();
    var scan = IcebergGenerics.read(table).build();
    for (var i = scan.iterator(); i.hasNext(); ) {
      var data = i.next();
      var map = new HashMap<String, Object>();
      for (int k = 0; k < data.size(); k++) {
        var field = SampleDefinition.SCHEMA_SAMPLE.findField(k + 1);
        map.put(field.name(), data.get(k));
      }
      result.add(map);
    }
    result.sort((a, b) -> a.get("id").toString().compareTo(b.get("id").toString()));
    return result;
  }
}
