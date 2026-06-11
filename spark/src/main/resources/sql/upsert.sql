MERGE INTO {TABLE} AS t
USING (VALUES (?, ?, ?)) AS s(time_bucket, event_id, event_count)
ON t.time_bucket = s.time_bucket AND t.event_id = s.event_id
WHEN MATCHED     THEN UPDATE SET t.event_count = s.event_count
WHEN NOT MATCHED THEN INSERT (time_bucket, event_id, event_count) VALUES (s.time_bucket, s.event_id, s.event_count)
