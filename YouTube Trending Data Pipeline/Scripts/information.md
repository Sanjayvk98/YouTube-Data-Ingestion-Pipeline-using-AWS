Bronze Bucket Name - amzn-yt-data-pipeline-bronze-ap-south-1-dev
Silver Bucket Name - amzn-yt-data-pipeline-silver-ap-south-1-dev
Gold Bucket Name - amzn-yt-data-pipeline-gold-ap-south-1-dev

Script Bucket - amzn-yt-data-pipeline-script-ap-south-1-dev

SNS ARN - <SN- ARN>

GLUE_DB_BRONZE - amzn-yt-data-pipeline-bronze-dev

GLUE_DB_SILVER - amzn-yt-data-pipeline-silver-dev

GLUE_DB_GOLD - amzn-yt-data-pipeline-gold-dev

--bronze_database amzn-yt-data-pipeline-bronze-dev
--bronze_table youtube_videos
--silver_bucket amzn-yt-data-pipeline-silver-ap-south-1-dev
--silver_database amzn-yt-data-pipeline-silver-dev
--silver_table clean_statistics

--silver_database amzn-yt-data-pipeline-silver-dev
--gold_bucket amzn-yt-data-pipeline-gold-ap-south-1-dev
--gold_database amzn-yt-data-pipeline-gold-dev