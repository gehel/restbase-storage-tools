package org.wikimedia.restbase.krv;

import java.util.UUID;

import com.datastax.driver.core.utils.UUIDs;

public class Row {
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

    public String getDomain() {
        return domain;
    }

    public String getKey() {
        return key;
    }

    public int getRev() {
        return rev;
    }

    public UUID getTid() {
        return tid;
    }

    public long getUnixTimestamp() {
        return UUIDs.unixTimestamp(this.tid);
    }

    @Override
    public String toString() {
        return "Row [domain=" + domain + ", key=" + key + ", rev=" + rev + ", tid=" + tid + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((domain == null) ? 0 : domain.hashCode());
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + rev;
        result = prime * result + ((tid == null) ? 0 : tid.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        Row other = (Row) obj;
        if (domain == null) {
            if (other.domain != null) return false;
        }
        else if (!domain.equals(other.domain)) return false;
        if (key == null) {
            if (other.key != null) return false;
        }
        else if (!key.equals(other.key)) return false;
        if (rev != other.rev) return false;
        if (tid == null) {
            if (other.tid != null) return false;
        }
        else if (!tid.equals(other.tid)) return false;
        return true;
    }
}

