CREATE TABLE IF NOT EXISTS {TABLE} (
    time_bucket TIMESTAMP NOT NULL,
    event_id    SMALLINT   NOT NULL,
    event_count       BIGINT    NOT NULL,
    CONSTRAINT pk_{TABLE} PRIMARY KEY (time_bucket, event_id)
)
