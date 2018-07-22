package org.wikimedia.restbase.krv;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;

public class TableScanner implements Iterable<Row>, Iterator<Row>, AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(TableScanner.class);
    private static final String QUERY = "SELECT \"_domain\", key, rev, tid FROM \"%s\".%s";

    private final Cassandra cassandra;
    private final ResultSet results;
    private final Iterator<com.datastax.driver.core.Row> iterator;

    public TableScanner(
            String host,
            int port,
            String keyspace,
            String table,
            Optional<String> user,
            Optional<String> pass,
            Optional<String> pageState,
            Optional<Integer> timeout,
            Optional<Boolean> ssl) {

        checkNotNull(keyspace);
        checkNotNull(table);

        this.cassandra = new Cassandra(host, port, user, pass, timeout, ssl, Optional.of(1000));
        
        Statement statement = new SimpleStatement(String.format(QUERY, keyspace, table));

        // If a page-state was passed in, use it.
        if (pageState.isPresent()) {
            LOG.info("Resuming at page state: {}", pageState.get());
            statement.setPagingState(PagingState.fromString(pageState.get()));
        }

        this.results = cassandra.getSession().execute(statement);
        this.iterator = results.iterator();
    }

    public PagingState getPagingState() {
        return this.results.getExecutionInfo().getPagingState();
    }

    @Override
    public boolean hasNext() {
        // Pre-fetch more results
        if (this.results.getAvailableWithoutFetching() <= 1000 && !this.results.isFullyFetched())
            this.results.fetchMoreResults();
        return this.iterator.hasNext();
    }

    @Override
    public Row next() {
        return new Row(this.iterator.next());
    }

    @Override
    public Iterator<Row> iterator() {
        return this;
    }

    @Override
    public void close() throws Exception {
        if (this.cassandra != null)
            this.cassandra.close();
    }

}
