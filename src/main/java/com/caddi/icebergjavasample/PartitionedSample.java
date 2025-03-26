package com.caddi.icebergjavasample;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.PartitionedFanoutWriter;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.InputStream;

public class PartitionedSample {
    public static void main(String[] args) throws Exception{
        var sample = new PartitionedSample();
        sample.perform(args[0], args[1], args[2], new FileInputStream(args[3]), Boolean.parseBoolean(args[4]));
    }

    private static final Logger LOG = LoggerFactory.getLogger(PartitionedSample.class);

    private static final long DATAFILE_MAX_SIZE = 100 * 1024 * 1024;

    public void perform(String restCatalogUri, String namespace, String table, InputStream input, boolean partitioned) throws Exception {

        LOG.info("get rest catalog from {}", restCatalogUri);
        var catalog = TableUtil.getCatalog(restCatalogUri);

        var partitionSpec = partitioned ? SampleDefinition.SAMPLE_PARTITION : PartitionSpec.unpartitioned();
        LOG.info("load table {} for schema {}", table, namespace);
        var tbl =
                TableUtil.getOrCreateTableAndNamespace(
                        catalog, namespace, table, SampleDefinition.SCHEMA_SAMPLE,
                        partitionSpec, SortOrder.unsorted());

        var transaction = tbl.newTransaction();

        var append = transaction.newAppend();
        var appenderFactory = new GenericAppenderFactory(tbl.schema());

        // 複数プロセスで同時に挿入する場合は、partitionId, taskIdをプロセスごとに分けないと、同名のファイルを作ってしまう。
        int partitionId = 1;
        int taskId = 1;
        var outputFileFactory =
                OutputFileFactory.builderFor(tbl, partitionId, taskId).format(FileFormat.PARQUET).build();
        final PartitionKey partitionKey = new PartitionKey(tbl.spec(), tbl.spec().schema());
        // partitionの有無に応じて、 Writerの実装を分ける。
        // writerはサイズを加味してデータファイルを分割し、
        // さらにPartitionedFanoutWriterは、パーティションの値でデータファイルを分割する。
        var writer =
                partitioned ?
                        new PartitionedFanoutWriter<Record>(
                                tbl.spec(),
                                FileFormat.PARQUET,
                                appenderFactory,
                                outputFileFactory,
                                tbl.io(),
                                DATAFILE_MAX_SIZE) {
                            @Override
                            protected PartitionKey partition(Record record) {
                                partitionKey.partition(record);
                                return partitionKey;
                            }
                        }
                        : new UnpartitionedWriter<Record>(
                        tbl.spec(),
                        FileFormat.PARQUET,
                        appenderFactory,
                        outputFileFactory,
                        tbl.io(),
                        DATAFILE_MAX_SIZE);


        var record = GenericRecord.create(tbl.schema());

        try (var lines = new JsonlReader(input)) {
            // data add to parquetWriter
            while (lines.hasNext()) {
                var r = lines.next();
                var row = record.copy(TableUtil.convertRecord(tbl.schema(), r));
                writer.write(row);
            }
        }

        // writer が生成したファイルをすべて append opertionに追加しコミットする。
        for (var dataFile : writer.dataFiles()) {
            append.appendFile(dataFile);
        }
        LOG.info("insert complete. append commit");
        append.commit();
        transaction.commitTransaction();

        LOG.info("end");
    }
}