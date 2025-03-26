package com.caddi.icebergjavasample;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;

public class SampleDefinition {

    public static final Schema SCHEMA_SAMPLE =
            new Schema(
                    List.of(
                            // common column
                            Types.NestedField.required(1, "id", Types.UUIDType.get()),
                            // table column
                            Types.NestedField.required(2, "name", Types.StringType.get()),
                            Types.NestedField.required(3, "price", Types.IntegerType.get()),
                            Types.NestedField.required(4, "registered_at", Types.TimestampType.withZone())));

    public static final PartitionSpec SAMPLE_PARTITION = PartitionSpec.builderFor(SCHEMA_SAMPLE)
            .bucket("name", 16).build();
}
