package org.wikimedia.restbase.krv;

import java.util.Objects;
import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

class Row {
    private final String domain;
    private final String key;
    private final int rev;
    private final UUID tid;

    Row(com.datastax.driver.core.Row row) {
        this(row.getString("_domain"), row.getString("key"), row.getInt("rev"), row.getUUID("tid"));
    }

    Row(String domain, String key, int rev, UUID tid) {
        this.domain = domain;
        this.key = key;
        this.rev = rev;
        this.tid = tid;
    }

    String getDomain() {
        return domain;
    }

    String getKey() {
        return key;
    }

    int getRev() {
        return rev;
    }

    UUID getTid() {
        return tid;
    }

    long getUnixTimestamp() {
        return UUIDs.unixTimestamp(this.tid);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Row row = (Row) o;
        return rev == row.rev &&
                Objects.equals(domain, row.domain) &&
                Objects.equals(key, row.key) &&
                Objects.equals(tid, row.tid);
    }

    @Override
    public int hashCode() {
        return Objects.hash(domain, key, rev, tid);
    }
}
