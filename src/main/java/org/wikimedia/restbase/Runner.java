package org.wikimedia.restbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.datastax.driver.core.utils.UUIDs;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;

// TODO: Accept a token start(/end?) as argument(s), and pass to TableScanner
// TODO: Periodically log the current token

public class Runner {
    private static final Logger LOG = LoggerFactory.getLogger(Runner.class);
    private static final int RECENCY_WINDOW_MS = 86400000;

    @Parameter(names = "--cqlshrc", description = "Path to Cassandra cqlshrc file.", required = true)
    private String cqlshrc;

    @Parameter(names = "--hostname", description = "Cassandra hostname to contact.")
    private String hostname = "127.0.0.1";

    @Parameter(names = "--port", description = "Cassandra port number.")
    private int port = 9042;

    @Parameter(names = "--timeout", description = "Cassandra timeout (in milliseconds)")
    private int timeout = 12000;

    @Parameter(names = "--keyspace", description = "Cassandra keyspace to cleanup.", required = true)
    private String keyspace;

    @Parameter(names = "--no-ssl", description = "Disable SSL client encryption.")
    private boolean noSsl = false;

    @Parameter(names = "--output-dir", description = "Directory to write output SSTables.", required = true)
    private String outPath;

    @Parameter(names = "--page-state", description = "Driver page state")
    private String pageState;

    @Parameter(names = "--help", help = true)
    private boolean help;

    public static void main(String... args) throws Exception {
        Runner app = new Runner();
        JCommander argsParser = JCommander.newBuilder().addObject(app).build();
        try {
            argsParser.parse(args);
        }
        catch (ParameterException e) {
            System.err.println(e.getLocalizedMessage());
            argsParser.usage();
            System.exit(1);
        }
        if (app.help) {
            argsParser.usage();
            System.exit(0);
        }

        if (!app.noSsl) {
            Properties sysProps = System.getProperties();
            if (sysProps.getProperty("javax.net.ssl.trustStore") == null)
                LOG.warn("SSL encryption enabled but \"javax.net.ssl.trustStore\" system property unset!");
            if (sysProps.getProperty("javax.net.ssl.trustStorePassword") == null)
                LOG.warn("SSL encryption enabled but \"javax.net.ssl.trustStorePassword\" system property unset!");
        }

        Cqlshrc cfg = Cqlshrc.parse(app.cqlshrc);
        Optional<String> pageState = app.pageState != null ? Optional.of(app.pageState) : Optional.absent();

        int numPartitions = 1;
        int numRows = 0;
        int numDeletes = 0;
        long started = System.currentTimeMillis();
        long deleteFrom = started - RECENCY_WINDOW_MS;
        UUID deleteFromTid = UUIDs.startOf(deleteFrom);
        Row previous = null;
        List<Row> rows = new ArrayList<>();

        final TombstoneWriter writer = new TombstoneWriter(app.outPath, app.keyspace);
        final EditHistogram histogram = new EditHistogram();

        // FIXME: Can we just use a finally clause on the following try?
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                LOG.info("Shutting down...");
                try {
                    writer.close();
                    LOG.info("Successfully closed writer.");
                }
                catch (IOException e) {
                    LOG.error("Exception encountered closing SSTable writer: {}", e);
                }
            }
        }));

        try (TableScanner scanner = new TableScanner(
                app.hostname,
                app.port,
                app.keyspace,
                "data",
                cfg.getUsername(),
                cfg.getPassword(),
                pageState,
                Optional.of(app.timeout),
                Optional.of(!app.noSsl))) {

            for (Row row : scanner) {
                if (previous == null) {
                    previous = row;
                    rows.add(row);
                    numRows++;
                    continue;
                }
                // The start of a new partition
                if (!previous.getDomain().equals(row.getDomain()) || !previous.getKey().equals(row.getKey())) {
                    numPartitions++;

                    Iterator<Row> rowsIter = rows.iterator();

                    // The most recent is saved from deletion, (everything else goes if outside the recency-window)
                    Row latest = rowsIter.next();

                    int currentRev = -1;
                    while (rowsIter.hasNext()) {
                        Row r = rowsIter.next();

                        // We are doing range deletes, so it is unnecessary to act on anything other than the first
                        // render of each revision.
                        if (currentRev == r.getRev())
                            continue;
                        else currentRev = r.getRev();

                        assert !r.equals(latest) : "Latest should not be evaluated for removal!";

                        if (r.getUnixTimestamp() < deleteFrom) {
                            if (r.getRev() == latest.getRev())
                                writer.addRow(r, latest.getTid());
                            else writer.addRow(r, deleteFromTid);
                            numDeletes++;
                        }
                    }
                    histogram.update(rows);
                    rows = Lists.newArrayList();
                }

                rows.add(row);
                previous = row;
                numRows++;

                if ((numRows % 10000) == 0) {
                    int elapsed = (int) (System.currentTimeMillis() - started) / 1000;
                    LOG.info(
                            "Partitions scanned: {} ({}/s), rows {} ({}/s), deletes {} ({}/s)",
                            numPartitions,
                            numPartitions / elapsed,
                            numRows,
                            numRows / elapsed,
                            numDeletes,
                            numDeletes / elapsed);
                }

                if ((numRows % 100000) == 0) LOG.info("Current page state: {}", scanner.getPagingState().toString());
            }
        }

        LOG.info("Partitions scanned: {}, rows {}, deletes {}", numPartitions, numRows, numDeletes);
        LOG.info("SSTables written to output directory: {}", writer.getOutputDirectory().toString());
        histogram.write(LOG);
        LOG.info("Complete.");

        System.exit(0);
    }

}
