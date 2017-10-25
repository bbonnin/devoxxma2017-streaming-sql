package io.millesabords.demo.streamingsql.flink;

import org.apache.flink.table.api.Table;
import org.apache.flink.types.Row;

public class LogStreamSqlProcessor extends LogStreamProcessor {

    public static void main(final String[] args) throws Exception {
        new LogStreamSqlProcessor().run();
    }

    public LogStreamSqlProcessor() {
        initEnv();
    }

    public void run() throws Exception {

        tableEnv.registerDataStream("weblogs", dataset,
                "ts, ip_address, url, status, nb_bytes, rowtime.rowtime");

        // No STREAM keyword => https://issues.apache.org/jira/browse/FLINK-4546
        final String query =
                "SELECT url, TUMBLE_END(rowtime, INTERVAL '10' SECOND), COUNT(*) AS nb_requests " +
                        "FROM weblogs " +
                        "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), url";

        final Table table = tableEnv.sql(query);

        tableEnv.toAppendStream(table, Row.class).print();

        execEnv.execute();
    }
}
