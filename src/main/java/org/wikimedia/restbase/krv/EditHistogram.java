package org.wikimedia.restbase.krv;

import java.io.PrintStream;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.google.common.collect.Lists;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

class EditHistogram {

    private final List<AtomicInteger> buckets = Lists.newArrayList(new AtomicInteger(0));

    EditHistogram update(List<Row> rows) {
        for (int i = 1; i < rows.size(); i++) {
            Instant prev = Instant.ofEpochMilli(rows.get(i - 1).getUnixTimestamp());
            Instant curr = Instant.ofEpochMilli(rows.get(i).getUnixTimestamp());
            Duration age = Duration.between(curr, prev);
            int weeks = (int) (age.toDays() / 7);

            // Expand the buckets list as necessary
            while (this.buckets.size() < (weeks + 1))
                this.buckets.add(new AtomicInteger(0));
            
            // XXX: Break-fix; Why would weeks be negative?
            if (weeks < 0) continue;

            this.buckets.get(weeks).getAndIncrement();
        }
        return this;
    }

    List<AtomicInteger> getBuckets() {
        return this.buckets;
    }

    @SuppressFBWarnings(value = "SLF4J_SIGN_ONLY_FORMAT", justification = "log message is split over multiple logging calls.")
    void write(Logger log) {
        log.info("| Weeks | Count |");
        for (int i = 0; i < this.buckets.size(); i++)
            log.info("| {} | {} |", i, this.buckets.get(i));
    }

    void write(PrintStream writer) {
        writer.printf("| %-7s | %-7s |%n", "Weeks", "Count");
        for (int i = 0; i < this.buckets.size(); i++)
            writer.printf("| %7d | %7d |%n", i, this.buckets.get(i));
    }

}
