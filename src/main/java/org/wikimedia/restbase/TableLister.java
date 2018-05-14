package org.wikimedia.restbase;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.internal.Lists;
import com.google.common.base.Optional;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;

public class TableLister {

    @Parameter(names = "--cqlshrc", description = "Path to Cassandra cqlshrc file.", required = true)
    private String cqlshrc;

    @Parameter(names = "--hostname", description = "Cassandra hostname to contact.")
    private String hostname = "127.0.0.1";

    @Parameter(names = "--port", description = "Cassandra port number.")
    private int port = 9042;

    @Parameter(names = "--help", help = true)
    private boolean help;

    public static void main(String... args) throws Exception {
        TableLister app = new TableLister();
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

        Cqlshrc cfg = Cqlshrc.parse(app.cqlshrc);
        try (Cassandra cassandra = new Cassandra(
                app.hostname,
                app.port,
                cfg.getUsername(),
                cfg.getPassword(),
                Optional.absent(),
                Optional.of(true),
                Optional.absent())) {

            Map<String, String> namesMap = Maps.newHashMap();

            Ordering<Map.Entry<String, String>> byValues = new Ordering<Map.Entry<String, String>>() {
                @Override
                public int compare(Entry<String, String> left, Entry<String, String> right) {
                    return left.getValue().compareTo(right.getValue());
                }
            };

            for (String name : cassandra.keyspaceNames()) {
                if (!name.matches("\\w+_T_\\w+")) continue;
                if (!Iterables.contains(cassandra.tableNames(name), "meta")) continue;

                String query = String.format("SELECT value FROM \"%s\".meta WHERE key = 'schema' LIMIT 1", name);
                com.datastax.driver.core.Row r = cassandra.getSession().execute(query).one();

                JSONParser parser = new JSONParser();
                JSONObject json = (JSONObject) parser.parse(r.getString("value"));
                namesMap.put(name, (String) json.get("table"));
            }

            List<Map.Entry<String, String>> namesList = Lists.newArrayList(namesMap.entrySet());
            Collections.sort(namesList, byValues);

            for (Map.Entry<String, String> entry : namesList)
                System.out.printf("%-50s | %s%n", entry.getKey(), entry.getValue());
        }

        System.exit(0);

    }

}
