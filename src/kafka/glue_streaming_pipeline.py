"""
Glue Streaming Job — consumes real-time Adobe hit data from MSK Kafka.

How it differs from the batch Glue job (glue_skp_pipeline.py):
  Batch job  : reads a static S3 file, runs once, exits.
  Stream job : runs continuously, processes Kafka messages in micro-batches
               (every BATCH_WINDOW seconds), writes incremental results to S3.

The same 9-step SKP attribution logic is applied to each micro-batch.

Architecture:
  MSK topic (skp-hits)
      → Glue Streaming (micro-batch every 60s)
          → same domain/keyword attribution logic
              → S3: processed/streaming/YYYY/MM/DD/HH/

Glue Streaming key concepts:
  - create_data_frame_from_options: opens a continuous Kafka reader
  - forEachBatch: applies your function to each micro-batch DataFrame
  - checkpointLocation: Glue tracks Kafka offsets here so if the job
    restarts it picks up where it left off (no duplicate processing)
"""
import sys
import re
import json
import logging
from datetime import datetime
from urllib.parse import urlparse, parse_qs

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType, StructType, StructField

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Glue bootstrap ────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'msk_bootstrap_servers',   # MSK Serverless endpoint
    'kafka_topic',             # e.g. skp-hits
    'output_path',             # s3://skp-processed-dev-.../processed/streaming/
    'checkpoint_path',         # s3://skp-raw-dev-.../checkpoints/streaming/
    'env',
    'config',
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

config = json.loads(args.get('config', '{}'))
SEARCH_ENGINES = config.get('search_engines', [
    'google.com', 'bing.com', 'yahoo.com', 'msn.com', 'ask.com', 'aol.com'
])
KEYWORD_PARAMS = config.get('keyword_params', ['q', 'p', 'query', 'text', 's', 'qs'])
PURCHASE_EVENT = config.get('purchase_event', '1')
REVENUE_THRESHOLD = config.get('revenue_threshold', 0.0)
BATCH_WINDOW = config.get('batch_window_seconds', 60)

# ── Schema — matches what the producer sends ──────────────────────────────────
# Kafka messages arrive as JSON strings. We define the schema so Spark
# can parse them efficiently instead of inferring at runtime.
HIT_SCHEMA = StructType([
    StructField('hit_time_gmt', StringType()),
    StructField('date_time', StringType()),
    StructField('user_agent', StringType()),
    StructField('ip', StringType()),
    StructField('event_list', StringType()),
    StructField('geo_city', StringType()),
    StructField('geo_region', StringType()),
    StructField('geo_country', StringType()),
    StructField('pagename', StringType()),
    StructField('page_url', StringType()),
    StructField('product_list', StringType()),
    StructField('referrer', StringType()),
])

# ── UDFs — same logic as batch job ───────────────────────────────────────────
def extract_domain(url):
    if not url:
        return None
    match = re.search(r'https?://(?:www\.)?([^/?\s]+)', url)
    return match.group(1).lower() if match else None


def extract_keyword(url):
    if not url:
        return None
    try:
        params = parse_qs(urlparse(url).query)
        for param in ['q', 'p', 'query', 'text', 's', 'qs']:
            if param in params and params[param][0].strip():
                return params[param][0].strip()
    except Exception:
        pass
    return None


def parse_revenue(product_list):
    if not product_list:
        return 0.0
    total = 0.0
    for item in product_list.split(','):
        parts = item.split(';')
        try:
            total += float(parts[3]) if len(parts) > 3 else 0.0
        except (ValueError, IndexError):
            pass
    return total


extract_domain_udf = F.udf(extract_domain, StringType())
extract_keyword_udf = F.udf(extract_keyword, StringType())
parse_revenue_udf = F.udf(parse_revenue, DoubleType())


# ── Micro-batch processor ─────────────────────────────────────────────────────
def process_batch(batch_df, batch_id):
    """
    Called automatically by Glue for every micro-batch.

    batch_df : a regular Spark DataFrame containing messages from this window
    batch_id : monotonically increasing integer — useful for logging/debugging

    Why forEachBatch instead of continuous processing:
      forEachBatch lets us use all standard Spark DataFrame operations
      (joins, window functions, aggregations) which aren't available in
      pure streaming mode.
    """
    count = batch_df.count()
    logger.info("Batch %d: %d raw messages received", batch_id, count)

    if count == 0:
        logger.info("Batch %d: empty — skipping", batch_id)
        return

    # Kafka delivers messages as binary. Parse the JSON value column
    # into typed columns using our schema.
    df = batch_df.select(
        F.from_json(F.col('value').cast('string'), HIT_SCHEMA).alias('hit')
    ).select('hit.*')

    # Drop rows where mandatory fields are null
    df = df.filter(
        F.col('hit_time_gmt').isNotNull() &
        F.col('ip').isNotNull() &
        F.col('referrer').isNotNull()
    )

    if df.count() == 0:
        logger.info("Batch %d: all rows filtered out", batch_id)
        return

    # Deduplication within this micro-batch
    df = df.dropDuplicates(['hit_time_gmt', 'ip', 'page_url'])

    # Derive ref_domain and search_keyword from referrer URL
    df = (df
          .withColumn('ref_domain', extract_domain_udf(F.col('referrer')))
          .withColumn('search_keyword', extract_keyword_udf(F.col('referrer'))))

    # First-touch search session per visitor in this batch
    search_df = df.filter(F.col('ref_domain').isin(SEARCH_ENGINES))
    if search_df.count() == 0:
        logger.info("Batch %d: no search engine referrals", batch_id)
        return

    window = Window.partitionBy('ip').orderBy('hit_time_gmt')
    search_df = (search_df
                 .withColumn('rn', F.row_number().over(window))
                 .filter(F.col('rn') == 1)
                 .drop('rn'))

    # Revenue: rows with a purchase event and a product_list
    revenue_df = (df
                  .filter(F.col('product_list').isNotNull())
                  .filter(F.col('event_list').contains(PURCHASE_EVENT))
                  .withColumn('revenue', parse_revenue_udf(F.col('product_list'))))

    # Attribution join
    attributed = (search_df.alias('s')
                  .join(revenue_df.alias('r'), on='ip', how='inner')
                  .select(
                      F.col('s.ref_domain').alias('search_engine'),
                      F.col('s.search_keyword').alias('keyword'),
                      F.col('r.revenue'),
                  )
                  .filter(F.col('revenue') > REVENUE_THRESHOLD))

    # Aggregate
    result = (attributed
              .groupBy('search_engine', 'keyword')
              .agg(F.sum('revenue').alias('revenue'))
              .orderBy(F.desc('revenue')))

    # Partition output by hour so files don't pile up in one folder
    # e.g. processed/streaming/2026/03/01/14/
    now = datetime.utcnow()
    partition_path = (f"{args['output_path'].rstrip('/')}/"
                      f"{now.year}/{now.month:02d}/{now.day:02d}/{now.hour:02d}/")

    (result.write
           .mode('append')
           .option('sep', '\t')
           .option('header', 'true')
           .csv(partition_path))

    logger.info("Batch %d: wrote %d rows to %s", batch_id, result.count(), partition_path)


# ── Open Kafka stream ─────────────────────────────────────────────────────────
#
# connectionType="kafka" tells Glue this is a Kafka source.
# MSK Serverless uses IAM auth — the Glue job's IAM role must have
# kafka:DescribeCluster and kafka-cluster:* permissions on the MSK cluster.
#
# starting_offsets_json="latest" means: only process NEW messages from now on.
# Use "earliest" to reprocess everything from the beginning of the topic.
#
kafka_options = {
    "connectionType": "kafka",
    "connectionName": f"skp-msk-{args['env']}",   # Glue Connection defined in console
    "topicName": args['kafka_topic'],
    "startingOffsets": "latest",
    "inferSchema": "false",                        # we provide HIT_SCHEMA explicitly
    "classification": "json",
}

streaming_df = glueContext.create_data_frame_from_options(
    connection_type="kafka",
    connection_options=kafka_options,
)

# ── Start the streaming query ─────────────────────────────────────────────────
#
# processingTime: how often Glue triggers process_batch (micro-batch window).
# checkpointLocation: where Glue stores Kafka offsets. If the job crashes
# and restarts, it reads this to know which messages have already been
# processed — preventing duplicates.
#
glue_query = (streaming_df.writeStream
              .foreachBatch(process_batch)
              .trigger(processingTime=f'{BATCH_WINDOW} seconds')
              .option('checkpointLocation', args['checkpoint_path'])
              .start())

logger.info("Streaming job started — micro-batch every %ds", BATCH_WINDOW)
glue_query.awaitTermination()   # runs until the job is stopped in AWS console

job.commit()
