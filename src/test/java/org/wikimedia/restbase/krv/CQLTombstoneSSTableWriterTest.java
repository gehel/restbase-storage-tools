package org.wikimedia.restbase.krv;

import java.io.File;
import java.io.IOException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.wikimedia.restbase.krv.TombstoneWriter;

public class CQLTombstoneSSTableWriterTest {
    private static final String keyspace = "test";
    private static final String table = "data";
    private static final String output = "target" + File.separator + keyspace + File.separator + table;

    private TombstoneWriter writer = null;

    @Before
    public void setUp() {
        this.writer = new TombstoneWriter(output, keyspace);
    }

    public void tearDown() throws IOException {
        if (this.writer != null)
            this.writer.close();
    }

    @Ignore
    @Test
    public void test() {
        // TODO: this.
    }
}
