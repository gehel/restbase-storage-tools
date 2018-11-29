package org.wikimedia.restbase.krv;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;

import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.io.sstable.CQLTombstoneSSTableWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class TombstoneWriter implements Closeable {
    static final int TOMBSTONES_PER_SSTABLE = 500000;

    private static final Logger LOG = LoggerFactory.getLogger(TombstoneWriter.class);
    private static final String TABLE_NAME = "data";

    // @formatter:off
    private static final String TABLE_SCHEMA = "CREATE TABLE \"%s\".%s (" + 
            "\"_domain\"          text, " + 
            "key                  text, " + 
            "rev                  int, " + 
            "tid                  timeuuid, " + 
            "\"content-location\" text, " + 
            "\"content-type\"     text, " + 
            "tags                 set<text>, " + 
            "value                text, " + 
            "PRIMARY KEY ((\"_domain\", key), rev, tid)) WITH" +
            "    CLUSTERING ORDER BY (rev DESC, tid DESC)";

    private static final String DELETE_STMNT     = "DELETE FROM \"%s\".%s WHERE \"_domain\" =  ? AND key =  ? AND rev =  ? AND tid <  ?";
    private static final String LOG_DELETE_STMNT = "DELETE FROM \"{}\".{} WHERE \"_domain\" = '{}' AND key = '{}' AND rev = {} AND tid < {}";
    // @formatter:on

    private int count = 0;
    private CQLTombstoneSSTableWriter writer;
    private final File outputDirectory;
    private final String keyspace;

    TombstoneWriter(String outputBase, String keyspace) {
        this.outputDirectory = newOutputDirectory(outputBase, keyspace);
        this.keyspace = keyspace;
        this.writer = newWriter();
    }

    void addRow(Row row, UUID delTid) throws InvalidRequestException, IOException {
        if ((++count % TOMBSTONES_PER_SSTABLE) == 0) {
            LOG.info("{} new tombstones written, check-pointing SSTable files", TOMBSTONES_PER_SSTABLE);
            this.writer.close();
            this.writer = newWriter();
        }
        this.writer.addRow(row.getDomain(), row.getKey(), row.getRev(), delTid);
        LOG.debug(LOG_DELETE_STMNT, this.keyspace, TABLE_NAME, row.getDomain(), row.getKey(), row.getRev(), delTid);
    }

    File getOutputDirectory() {
        return this.outputDirectory;
    }

    @Override
    public void close() throws IOException {
        this.writer.close();
    }

    private CQLTombstoneSSTableWriter newWriter() {
        return newWriter(this.outputDirectory, this.keyspace);
    }

    static File newOutputDirectory(String base, String keyspaceName) {
        File path = Paths.get(base, UUID.randomUUID().toString(), keyspaceName, TABLE_NAME).toFile();
        if (path.exists())
            throw new RuntimeException(String.format("%s already exists; This should not happen!", path.toString()));
        if (!path.mkdirs())
            throw new RuntimeException(String.format("Unable to create output directory %s", path.toString()));
        return path;
    }

    static CQLTombstoneSSTableWriter newWriter(File output, String keyspace) {
        LOG.info("Creating new table writer using output directory: {}", output);
        LOG.debug("Creating new table writer using schema: {}", String.format(TABLE_SCHEMA, keyspace, TABLE_NAME));
        LOG.debug("Creating new table writer using statement: {}", String.format(DELETE_STMNT, keyspace, TABLE_NAME));
        return CQLTombstoneSSTableWriter.builder()
                                        .inDirectory(output)
                                        .forTable(String.format(TABLE_SCHEMA, keyspace, TABLE_NAME))
                                        .using(String.format(DELETE_STMNT, keyspace, TABLE_NAME))
                                        .build();
    }
}
