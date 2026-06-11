package com.taboola.api;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/counters")
public class Controller {

    private final JdbcTemplate jdbc;

    public Controller(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    private static final DateTimeFormatter BUCKET_FMT =
        DateTimeFormatter.ofPattern("uuuuMMddHHmm").withResolverStyle(ResolverStyle.STRICT);

    @GetMapping("/currentTime")
    public String currentTime() {
        return LocalDateTime.now(java.time.ZoneOffset.UTC).format(BUCKET_FMT);
    }

    @GetMapping("/time/{bucket}")
    public Map<String, Long> getByBucket(@PathVariable String bucket) {
        validateBucket(bucket);
        validateTableExists(bucket);
        Map<String, Long> result = jdbc.query(
            "SELECT event_id, event_count FROM " + table(bucket) + " WHERE time_bucket = ?",
            rs -> {
                Map<String, Long> r = new LinkedHashMap<>();
                while (rs.next()) r.put(rs.getString("event_id"), rs.getLong("event_count"));
                return r;
            },
            timestamp(bucket)
        );
        if (result == null || result.isEmpty())
            throw notFound("No events recorded for bucket: " + bucket);
        return result;
    }

    @GetMapping("/time/{bucket}/eventId/{eventId}")
    public Map<String, Long> getByBucketAndEventId(@PathVariable String bucket, @PathVariable String eventId) {
        validateBucket(bucket);
        validateTableExists(bucket);
        int id = parseEventId(eventId);
        List<Long> rows = jdbc.queryForList(
            "SELECT event_count FROM " + table(bucket) + " WHERE time_bucket = ? AND event_id = ?",
            Long.class, timestamp(bucket), id
        );
        return Collections.singletonMap("event_count", rows.isEmpty() ? 0L : rows.get(0));
    }

    private void validateBucket(String bucket) {
        if (!bucket.matches("\\d{12}"))
            throw badRequest("Time bucket must be 12 digits in format YYYYMMddHHmm");
        try {
            LocalDateTime.parse(bucket, BUCKET_FMT);
        } catch (DateTimeParseException e) {
            throw badRequest("Invalid date/time in bucket: " + bucket);
        }
    }

    private void validateTableExists(String bucket) {
        String tableName = table(bucket).toUpperCase();
        Integer count = jdbc.queryForObject(
            "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = 'PUBLIC'",
            Integer.class, tableName
        );
        if (count == null || count == 0)
            throw notFound("No data available for date: " + bucket.substring(0, 8));
    }

    private int parseEventId(String eventId) {
        try {
            int id = Integer.parseInt(eventId);
            if (id < 0 || id > 99)
                throw badRequest("Event ID must be between 0 and 99, got: " + eventId);
            return id;
        } catch (NumberFormatException e) {
            throw badRequest("Event ID must be an integer between 0 and 99");
        }
    }

    private static ResponseStatusException badRequest(String msg) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, msg);
    }

    private static ResponseStatusException notFound(String msg) {
        return new ResponseStatusException(HttpStatus.NOT_FOUND, msg);
    }

    private String table(String bucket) {
        return "event_counters_" + bucket.substring(0, 8);
    }

    private Timestamp timestamp(String bucket) {
        LocalDateTime ldt = LocalDateTime.parse(bucket, BUCKET_FMT);
        return Timestamp.valueOf(ldt);
    }
}
