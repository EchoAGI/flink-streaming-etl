CREATE TABLE comments (
  id STRING,
  content STRING,
  proc_time AS PROCTIME(),
  PRIMARY KEY (id) NOT ENFORCED
) WITH (
  'connector' = 'kafka',
  'topic' = 'crawler.crawler.comments',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'cdc',
  'format' = 'debezium-json',
  'debezium-json.schema-include' = 'false',
  'debezium-json.ignore-parse-errors' = 'false',
  'debezium-json.timestamp-format.standard' = 'ISO-8601'
);