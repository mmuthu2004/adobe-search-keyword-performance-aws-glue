"""
Glue ETL job — replaces spark_skp_pipeline.py + EMR Serverless.
Same 9-step pipeline; GlueContext wraps the SparkSession.
Job bookmarks handle file-level deduplication automatically.
"""
import sys
import json
import logging
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

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
SEARCH_ENGINES = config.get('search_engines', ['google.com', 'bing.com', 'yahoo.com'])
REVENUE_THRESHOLD = config.get('revenue_threshold', 0.0)
NULL_THRESHOLD = config.get('null_threshold', 0.5)

input_path = args['input_path']
output_path = args['output_path']
logger.info("Input: %s | Output: %s", input_path, output_path)

# ── Step 1: Read ──────────────────────────────────────────────────────────────
df = spark.read.option("sep", "\t").option("header", "true").csv(input_path)

# ── Step 2: Schema validation ─────────────────────────────────────────────────
REQUIRED = [
    'hit_time_gmt', 'ref_domain', 'search_engine_natural_keyword',
    'post_visid_high', 'post_visid_low', 'visit_num',
    'visit_page_num', 'product_list', 'ip',
]
missing = [c for c in REQUIRED if c not in df.columns]
if missing:
    raise ValueError(f"Missing columns: {missing}")

# ── Step 3: Null validation ───────────────────────────────────────────────────
total = df.count()
for col in ['hit_time_gmt', 'ref_domain', 'ip']:
    null_rate = df.filter(F.col(col).isNull()).count() / total
    if null_rate > NULL_THRESHOLD:
        raise ValueError(f"Null rate too high for '{col}': {null_rate:.2%}")

# ── Step 4: Row-level deduplication ──────────────────────────────────────────
# File-level dedup is handled by Glue job bookmarks (--job-bookmark-option)
KEY_COLS = ['hit_time_gmt', 'post_visid_high', 'post_visid_low', 'visit_num', 'visit_page_num']
df = df.dropDuplicates(KEY_COLS)

# ── Step 5: Session extraction — first-touch search referral per visitor ──────
search_df = df.filter(F.col('ref_domain').isin(SEARCH_ENGINES))
window = Window.partitionBy('ip').orderBy('hit_time_gmt')
search_df = (search_df
             .withColumn('rn', F.row_number().over(window))
             .filter(F.col('rn') == 1)
             .drop('rn'))

# ── Step 6: Revenue parsing ───────────────────────────────────────────────────
def parse_revenue(product_list):
    """Extract total revenue from Adobe Analytics product_list string."""
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
revenue_df = (df.filter(F.col('product_list').isNotNull())
                .withColumn('revenue', parse_revenue_udf(F.col('product_list'))))

# ── Step 7: Attribution — join revenue to first-touch session ─────────────────
attributed = (search_df.alias('s')
              .join(revenue_df.alias('r'), on='ip', how='inner')
              .select(
                  F.col('s.ref_domain').alias('search_engine'),
                  F.col('s.search_engine_natural_keyword').alias('keyword'),
                  F.col('r.revenue'),
              )
              .filter(F.col('revenue') > REVENUE_THRESHOLD))

# ── Step 8: Aggregation ───────────────────────────────────────────────────────
result = (attributed
          .groupBy('search_engine', 'keyword')
          .agg(F.sum('revenue').alias('revenue'))
          .orderBy(F.desc('revenue')))

# ── Step 9: Output (tab-delimited) ───────────────────────────────────────────
logger.info("Writing results to %s", output_path)
(result.write
       .mode('overwrite')
       .option('sep', '\t')
       .option('header', 'true')
       .csv(output_path))

job.commit()
logger.info("Glue job complete.")
