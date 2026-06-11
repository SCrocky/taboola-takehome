package com.taboola.spark;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.types.DataTypes;

public class SparkApp {

    private static final String JDBC_URL = "jdbc:hsqldb:hsql://localhost/xdb";
    private static final String JDBC_USER = "sa";
    private static final String JDBC_PASS = "";

    private static final DateTimeFormatter DATE_FMT =
        DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    private static final String CREATE_TABLE_SQL = loadSql(
        "/sql/create_table.sql"
    );
    private static final String UPSERT_SQL = loadSql("/sql/upsert.sql");

    public static void main(String[] args) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));

        SparkSession spark = SparkSession.builder()
            .master("local[4]")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.shuffle.partitions", "4")
            .config(
                "spark.metrics.conf.*.sink.jmx.class",
                "org.apache.spark.metrics.sink.JmxSink"
            )
            .config("spark.sql.streaming.metricsEnabled", "true")
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        getEvents(spark)
            .groupBy(
                functions.window(functions.col("timestamp"), "1 minute"),
                functions.col("eventId")
            )
            .count()
            .select(
                functions.col("window.start").alias("time_bucket"),
                functions.col("eventId"),
                functions.col("count")
            )
            .writeStream()
            .outputMode("update")
            .option("checkpointLocation", "/tmp/taboola-spark-checkpoint")
            .trigger(Trigger.ProcessingTime(10, TimeUnit.SECONDS))
            .foreachBatch(
                (VoidFunction2<Dataset<Row>, Long>) (batchDF, _batchId) -> {
                    batchDF.foreachPartition(rows -> {
                        Map<String, List<Row>> byTable = new HashMap<>();
                        while (rows.hasNext()) {
                            Row row = rows.next();
                            Timestamp ts = row.getTimestamp(0);
                            String table =
                                "event_counters_" +
                                DATE_FMT.format(ts.toInstant());
                            byTable
                                .computeIfAbsent(table, k -> new ArrayList<>())
                                .add(row);
                        }
                        try (
                            Connection conn = DriverManager.getConnection(
                                JDBC_URL,
                                JDBC_USER,
                                JDBC_PASS
                            )
                        ) {
                            conn.setAutoCommit(false);

                            for (Map.Entry<
                                String,
                                List<Row>
                            > entry : byTable.entrySet()) {
                                String table = entry.getKey();
                                ensureTable(conn, table);
                                try (
                                    PreparedStatement ps =
                                        conn.prepareStatement(
                                            UPSERT_SQL.replace("{TABLE}", table)
                                        )
                                ) {
                                    for (Row row : entry.getValue()) {
                                        ps.setTimestamp(1, row.getTimestamp(0));
                                        ps.setLong(2, row.getLong(1));
                                        ps.setLong(3, row.getLong(2));
                                        ps.addBatch();
                                    }
                                    ps.executeBatch();
                                }
                            }

                            conn.commit();
                        }
                    });
                }
            )
            .start();

        spark.streams().awaitAnyTermination();
    }

    private static void ensureTable(Connection conn, String table)
        throws Exception {
        try (Statement stmt = conn.createStatement()) {
            stmt.execute(CREATE_TABLE_SQL.replace("{TABLE}", table));
        }
    }

    private static String loadSql(String resource) {
        try (InputStream is = SparkApp.class.getResourceAsStream(resource)) {
            if (is == null) throw new RuntimeException(
                "SQL resource not found: " + resource
            );
            try (Scanner scanner = new Scanner(is, "UTF-8")) {
                scanner.useDelimiter("\\A");
                return scanner.hasNext() ? scanner.next().trim() : "";
            }
        } catch (IOException e) {
            throw new RuntimeException(
                "Failed to load SQL resource: " + resource,
                e
            );
        }
    }

    private static Dataset<Row> getEvents(SparkSession spark) {
        return spark
            .readStream()
            .format("rate")
            .option("rowsPerSecond", "2000000")
            .load()
            .withColumn(
                "eventId",
                functions
                    .rand()
                    .multiply(functions.lit(100))
                    .cast(DataTypes.LongType)
            )
            .select("eventId", "timestamp");
    }
}
