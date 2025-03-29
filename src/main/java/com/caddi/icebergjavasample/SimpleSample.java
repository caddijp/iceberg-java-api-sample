package com.caddi.icebergjavasample;

import org.apache.iceberg.*;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.parquet.Parquet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class SimpleSample {
    public static void main(String[] args) throws Exception {
        var sample = new SimpleSample();
        sample.perform(args[0], args[1], args[2], new FileInputStream(args[3]));
    }

    private static final Logger LOG = LoggerFactory.getLogger(SimpleSample.class);

    public void perform(String restCatalogUri, String namespace, String table, InputStream input) throws Exception {

        LOG.info("get rest catalog from {}", restCatalogUri);
        var catalog = TableUtil.getCatalog(restCatalogUri);
        var tbl =
                TableUtil.getOrCreateTableAndNamespace(
                        catalog, namespace, table, SampleDefinition.SCHEMA_SAMPLE,
                        PartitionSpec.unpartitioned(), SortOrder.unsorted());

        var transaction = tbl.newTransaction();
        var append = transaction.newAppend();
        transaction.newOverwrite().
        var fileId = OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd_HHmmss"))
                + "_"
                + UUID.randomUUID();
        String filepath = tbl.location() + "/data/" + fileId + ".parquet";
        var file = tbl.io().newOutputFile(filepath);
        var dataWriter =
                Parquet.writeData(file)
                        .schema(tbl.schema())
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .overwrite()
                        .withSpec(PartitionSpec.unpartitioned())
                        .build();
        var record = GenericRecord.create(tbl.schema());
        try (var lines = new JsonlReader(input)) {
            // data add to parquetWriter
            while (lines.hasNext()) {
                var r = lines.next();
                var row = record.copy(TableUtil.convertRecord(tbl.schema(), r));
                dataWriter.write(row);
            }
        }
        // writing finish, then commit data file.
        dataWriter.close();
        var dataFile = dataWriter.toDataFile();
        append.appendFile(dataFile);

        // commit
        append.commit();
        transaction.commitTransaction();

        LOG.info("finish");
    }
}