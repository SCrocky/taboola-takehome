-- Table DDL template — one table per calendar day.
-- The Spark app generates the table name dynamically: event_counters_YYYYMMDD
--
-- time_bucket: UTC timestamp floored to the minute (e.g. 2019-03-04 10:01:00)
-- event_id:    value in range 0–99
-- count:       running total for this (bucket, event_id) pair within the day

CREATE TABLE IF NOT EXISTS event_counters_YYYYMMDD (
    time_bucket TIMESTAMP NOT NULL,
    event_id    BIGINT    NOT NULL,
    count       BIGINT    NOT NULL,
    CONSTRAINT pk_event_counters_YYYYMMDD PRIMARY KEY (time_bucket, event_id)
);
