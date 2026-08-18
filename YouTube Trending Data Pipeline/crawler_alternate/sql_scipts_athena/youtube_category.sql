DROP TABLE IF EXISTS `youtube_category_raw`;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_statistics_reference_data (
  kind STRING,
  etag STRING,
  items ARRAY<STRUCT<
    kind: STRING,
    etag: STRING,
    id: STRING,
    snippet: STRUCT<
      channelId: STRING,
      title: STRING,
      assignable: BOOLEAN
    >
  >>
)
PARTITIONED BY (region STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
WITH SERDEPROPERTIES (
  'read.single.line'='false'
)
LOCATION 's3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics_reference_data/'
TBLPROPERTIES (
  'projection.enabled'='true',
  'projection.region.type'='enum',
  'projection.region.values'='ca,de,fr,gb,in,jp,kr,mx,ru,us',
  'storage.location.template'='s3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics_reference_data/region=${region}/'
);