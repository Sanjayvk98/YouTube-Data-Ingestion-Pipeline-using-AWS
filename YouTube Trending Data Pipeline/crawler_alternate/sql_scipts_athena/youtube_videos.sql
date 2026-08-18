DROP TABLE IF EXISTS `youtube_videos`;

CREATE EXTERNAL TABLE IF NOT EXISTS raw_statistics (
  video_id STRING,
  trending_date STRING,
  title STRING,
  channel_title STRING,
  category_id INT,
  publish_time STRING,
  tags STRING,
  views BIGINT,
  likes BIGINT,
  dislikes BIGINT,
  comment_count BIGINT,
  thumbnail_link STRING,
  comments_disabled STRING,
  ratings_disabled STRING,
  video_error_or_removed STRING,
  description STRING
)
PARTITIONED BY (region STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
ESCAPED BY '\\'
LINES TERMINATED BY '\n'
LOCATION 's3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics/'
TBLPROPERTIES (
  'skip.header.line.count'='1'
  'projection.enabled'='true',
  'projection.region.type'='enum',
  'projection.region.values'='ca,de,fr,gb,in,jp,kr,mx,ru,us',
  'storage.location.template'='s3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics/region=${region}/'
);


----------------------------------------------------v2 ------------------------------------------------------------------------------

DROP TABLE youtube_videos;

CREATE EXTERNAL TABLE youtube_videos (
  video_id STRING,
  trending_date STRING,
  title STRING,
  channel_title STRING,
  category_id INT,
  publish_time STRING,
  tags STRING,
  views BIGINT,
  likes BIGINT,
  dislikes BIGINT,
  comment_count BIGINT,
  thumbnail_link STRING,
  comments_disabled STRING,
  ratings_disabled STRING,
  video_error_or_removed STRING,
  description STRING
)
PARTITIONED BY (region STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
  'separatorChar' = ',',
  'quoteChar' = '"'
)
STORED AS TEXTFILE
LOCATION 's3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics/'
TBLPROPERTIES (
  'skip.header.line.count'='1',
  'projection.enabled'='true',
  'projection.region.type'='enum',
  'projection.region.values'='ca,de,fr,gb,in,jp,kr,mx,ru,us',
  'storage.location.template'='s3://amzn-yt-data-pipeline-bronze-ap-south-1-dev/youtube/raw_statistics/region=${region}/'
);