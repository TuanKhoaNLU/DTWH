-- transforms.sql
USE dwh_tiktok;

-- Simple transform: extract videos and creators from staging_raw JSON
INSERT INTO creators (creator_id, name, followers, extra)
SELECT
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.creator.id')) AS creator_id,
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.creator.name')) AS name,
  CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.creator.followers')) AS UNSIGNED) AS followers,
  JSON_EXTRACT(raw_json, '$.creator') AS extra
FROM staging_raw
WHERE processed = FALSE
  AND JSON_EXTRACT(raw_json, '$.creator.id') IS NOT NULL
ON DUPLICATE KEY UPDATE
  name = VALUES(name),
  followers = VALUES(followers),
  extra = VALUES(extra);

INSERT INTO videos (video_id, creator_id, title, description, created_at, duration_seconds, stats, raw_json)
SELECT
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.id')) AS video_id,
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.creator.id')) AS creator_id,
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.title')) AS title,
  JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.description')) AS description,
  STR_TO_DATE(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.create_time')), '%Y-%m-%dT%H:%i:%s') AS created_at,
  CAST(JSON_UNQUOTE(JSON_EXTRACT(raw_json, '$.duration')) AS UNSIGNED) AS duration_seconds,
  JSON_EXTRACT(raw_json, '$.stats') AS stats,
  raw_json
FROM staging_raw
WHERE processed = FALSE
  AND JSON_EXTRACT(raw_json, '$.id') IS NOT NULL
ON DUPLICATE KEY UPDATE
  title = VALUES(title),
  description = VALUES(description),
  stats = VALUES(stats),
  raw_json = VALUES(raw_json);

-- mark processed
UPDATE staging_raw SET processed = TRUE WHERE processed = FALSE;
