package org.wikimedia.restbase;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TableMetadata;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;

class Cassandra implements AutoCloseable {

    private final Cluster cluster;
    private final Session session;

    Cassandra(String host, int port, Optional<String> user, Optional<String> pass) {
        this(host, port, user, pass, Optional.absent(), Optional.absent(), Optional.absent());
    }

    Cassandra(
            String host,
            int port,
            Optional<String> user,
            Optional<String> pass,
            Optional<Integer> timeout,
            Optional<Boolean> ssl,
            Optional<Integer> fetchSize) {

        checkNotNull(host);
        checkArgument(port > 0, "Port numbers must be positive");

        Cluster.Builder builder = Cluster.builder().addContactPoint(host).withPort(port);
        if (timeout.isPresent()) builder.withSocketOptions(new SocketOptions().setReadTimeoutMillis(timeout.get()));
        if (ssl.isPresent() && ssl.get()) builder.withSSL();

        if ((user.isPresent() && !pass.isPresent()) || (pass.isPresent() && !user.isPresent()))
            throw new IllegalArgumentException("username and password must be mutually (un)set");
        if (user.isPresent()) builder.withCredentials(user.get(), pass.get());

        if (fetchSize.isPresent()) {
            QueryOptions queryOpts = new QueryOptions();
            queryOpts.setFetchSize(fetchSize.get());
            builder.withQueryOptions(queryOpts);
        }

        this.cluster = builder.build();
        this.session = this.cluster.connect();
    }

    Session getSession() {
        return this.session;
    }

    Iterable<String> tableNames(String keyspace) {
        KeyspaceMetadata km = this.cluster.getMetadata().getKeyspace(quote(keyspace));
        checkNotNull(km, keyspace);
        return Iterables.transform(km.getTables(), new Function<TableMetadata, String>() {
            @Override
            public String apply(TableMetadata tm) {
                return tm.getName();
            }
        });
    }

    Iterable<String> keyspaceNames() {
        return Iterables.transform(this.cluster.getMetadata().getKeyspaces(), new Function<KeyspaceMetadata, String>() {
            @Override
            public String apply(KeyspaceMetadata km) {
                return km.getName();
            }
        });
    }

    @Override
    public void close() throws Exception {
        if (this.cluster != null) this.cluster.close();
    }

    private static String quote(String val) {
        return String.format("\"%s\"", val);
    }
}
