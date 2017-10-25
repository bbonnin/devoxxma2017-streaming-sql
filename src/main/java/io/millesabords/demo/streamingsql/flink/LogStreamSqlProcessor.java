package io.millesabords.demo.streamingsql.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Time;

public class LogStreamSqlProcessor extends LogStreamProcessor {

    private static final MapFunction<String, Tuple5<Time, String, String, String, Integer>> logMapper =
        new MapFunction<String, Tuple5<Time, String, String, String, Integer>>() {

            @Override
            public Tuple5<Time, String, String, String, Integer> map(final String log) throws Exception {
                // <timestamp> <IP address> <method> <url> <#bytes>
                final String[] fields = log.split("\t");
                final Time logDate = new Time(Long.parseLong(fields[0]));
                final int nbBytes = Integer.parseInt(fields[4]);
                return new Tuple5<>(logDate, fields[1], fields[2], fields[3], nbBytes);
            }
        };

    private static final AscendingTimestampExtractor tsExtractor =
        new AscendingTimestampExtractor<Tuple5<Time, String, String, String, Integer>>() {

            @Override
            public long extractAscendingTimestamp(final Tuple5<Time, String, String, String, Integer> element) {
                return element.f0.getTime();
            }
        };

    public static void main(final String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        final StreamTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        final DataStream<String> text = env.socketTextStream("localhost", 5000, "\n");

        final DataStream<Tuple5<Time, String, String, String, Integer>> dataset = text
                .map(logMapper)
                .assignTimestampsAndWatermarks(tsExtractor);

        tableEnv.registerDataStream("weblogs", dataset,
                "ts, ip_address, url, status, nb_bytes, rowtime.rowtime");

        // No STREAM keyword => https://issues.apache.org/jira/browse/FLINK-4546
        final String query =
                "SELECT url, TUMBLE_END(rowtime, INTERVAL '10' SECOND), COUNT(*) AS nb_requests " +
                "FROM weblogs " +
                "GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND), url";

        final Table table = tableEnv.sql(query);

        tableEnv.toAppendStream(table, Row.class).print();

        env.execute();

    }
}
