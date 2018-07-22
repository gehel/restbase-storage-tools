package org.wikimedia.restbase.krv;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.junit.Test;
import org.wikimedia.restbase.krv.EditHistogram;
import org.wikimedia.restbase.krv.Row;

import com.datastax.driver.core.utils.UUIDs;
import com.google.common.collect.Lists;

public class EditHistogramTest {

    @Test
    public void test() {
        List<Row> rows = Lists.newArrayList();

        Instant now = Instant.now();
        Instant weeksAgo1 = now.minus(Duration.ofDays(1));
        Instant weeksAgo2 = now.minus(Duration.ofDays(8));
        Instant weeksAgo3 = now.minus(Duration.ofDays(22));
        Instant weeksAgo4 = now.minus(Duration.ofDays(43));

        rows.add(new Row("D", "K", 1, UUIDs.startOf(now.toEpochMilli())));
        rows.add(new Row("D", "K", 1, UUIDs.startOf(weeksAgo1.toEpochMilli())));
        rows.add(new Row("D", "K", 1, UUIDs.startOf(weeksAgo2.toEpochMilli())));
        rows.add(new Row("D", "K", 1, UUIDs.startOf(weeksAgo3.toEpochMilli())));
        rows.add(new Row("D", "K", 1, UUIDs.startOf(weeksAgo4.toEpochMilli())));

        EditHistogram histo = new EditHistogram().update(rows);

        assertEquals(histo.getBuckets().size(), 4);
    }

}
