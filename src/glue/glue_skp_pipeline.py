"""
Glue ETL job — replaces spark_skp_pipeline.py + EMR Serverless.
Processes raw Adobe Analytics hit data (12-column tab-delimited format).
ref_domain and search_keyword are derived from the referrer URL.
"""
import sys
import re
import json
import logging
from urllib.parse import urlparse, parse_qs

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType, StringType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Glue bootstrap ────────────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    'JOB_NAME', 'input_path', 'output_path', 'env', 'config'
])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)          # enables job bookmarks

config = json.loads(args.get('config', '{}'))
SEARCH_ENGINES = config.get('search_engines', [
    'google.com', 'bing.com', 'yahoo.com', 'msn.com', 'ask.com', 'aol.com'
])
KEYWORD_PARAMS = config.get('keyword_params', ['q', 'p', 'query', 'text', 's', 'qs'])
PURCHASE_EVENT = config.get('purchase_event', '1')
REVENUE_THRESHOLD = config.get('revenue_threshold', 0.0)
NULL_THRESHOLD = config.get('null_threshold', 0.5)

input_path = args['input_path']
output_path = args['output_path']
logger.info("Input: %s | Output: %s", input_path, output_path)

# ── Step 1: Read ──────────────────────────────────────────────────────────────
df = spark.read.option("sep", "\t").option("header", "true").csv(input_path)
logger.info("Rows read: %d | Columns: %s", df.count(), df.columns)

# ── Step 2: Schema validation — matches actual hit_data.tab columns ───────────
REQUIRED = ['hit_time_gmt', 'ip', 'event_list', 'product_list', 'referrer']
missing = [c for c in REQUIRED if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns: {missing}")

# ── Step 3: Null validation ───────────────────────────────────────────────────
total = df.count()
for col in ['hit_time_gmt', 'ip', 'referrer']:
    null_rate = df.filter(F.col(col).isNull()).count() / total
    if null_rate > NULL_THRESHOLD:
        raise ValueError(f"Null rate too high for '{col}': {null_rate:.2%}")

# ── Step 4: Deduplication ─────────────────────────────────────────────────────
KEY_COLS = ['hit_time_gmt', 'ip', 'page_url']
available_keys = [c for c in KEY_COLS if c in df.columns]
df = df.dropDuplicates(available_keys)

# ── Step 5: Derive ref_domain and search_keyword from referrer URL ────────────
def extract_domain(url):
    """Extract bare domain from referrer URL (strips www. prefix)."""
    if not url:
        return None
    match = re.search(r'https?://(?:www\.)?([^/?\s]+)', url)
    return match.group(1).lower() if match else None

def extract_keyword(url):
    """Extract search keyword from referrer URL query params."""
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

extract_domain_udf = F.udf(extract_domain, StringType())
extract_keyword_udf = F.udf(extract_keyword, StringType())

df = (df
      .withColumn('ref_domain', extract_domain_udf(F.col('referrer')))
      .withColumn('search_keyword', extract_keyword_udf(F.col('referrer'))))

# ── Step 6: Session extraction — first-touch search referral per visitor ──────
search_df = df.filter(F.col('ref_domain').isin(SEARCH_ENGINES))
window = Window.partitionBy('ip').orderBy('hit_time_gmt')
search_df = (search_df
             .withColumn('rn', F.row_number().over(window))
             .filter(F.col('rn') == 1)
             .drop('rn'))

# ── Step 7: Revenue parsing from product_list ─────────────────────────────────
def parse_revenue(product_list):
    """Sum revenue from Adobe product_list: category;name;qty;price;...,... """
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

parse_revenue_udf = F.udf(parse_revenue, DoubleType())

# Only rows with purchase event (event_list contains "1")
revenue_df = (df
              .filter(F.col('product_list').isNotNull())
              .filter(F.col('event_list').contains(PURCHASE_EVENT))
              .withColumn('revenue', parse_revenue_udf(F.col('product_list'))))

# ── Step 8: Attribution — join purchase revenue to first-touch search session ─
attributed = (search_df.alias('s')
              .join(revenue_df.alias('r'), on='ip', how='inner')
              .select(
                  F.col('s.ref_domain').alias('search_engine'),
                  F.col('s.search_keyword').alias('keyword'),
                  F.col('r.revenue'),
              )
              .filter(F.col('revenue') > REVENUE_THRESHOLD))

# ── Step 9: Aggregate and write ───────────────────────────────────────────────
result = (attributed
          .groupBy('search_engine', 'keyword')
          .agg(F.sum('revenue').alias('revenue'))
          .orderBy(F.desc('revenue')))

logger.info("Result rows: %d", result.count())
logger.info("Writing to: %s", output_path)

(result.write
       .mode('overwrite')
       .option('sep', '\t')
       .option('header', 'true')
       .csv(output_path))

job.commit()
logger.info("Glue job complete.")
