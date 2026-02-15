from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import logging
import os
import argparse

logging.basicConfig(
    level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Clickhouse')

def main():
  spark = None
  parser = argparse.ArgumentParser()
  parser.add_argument("--execution-date")
  args = parser.parse_args()
  exec_date = args.execution_date
  try:
    spark = SparkSession.builder \
       .appName("CoreloadClickhouse") \
       .getOrCreate()

    core_path = "s3a://core/dim_assets/"
    df = spark.read.parquet(core_path).filter(col("load_date") == exec_date)


    ch_options = {
        "host":     os.environ["CLICKHOUSE_HOST"],
        "port":     os.environ["CLICKHOUSE_PORT"],
        "user":     os.environ["CLICKHOUSE_USER"],
        "password": os.environ["CLICKHOUSE_PASSWORD"],
        "database": os.environ["CLICKHOUSE_DB"],
        "table":    os.environ["CLICKHOUSE_TABLE"]
      }
    
    logger.info("📤 Sending data to ClickHouse...")
    df.write \
      .format("clickhouse") \
      .options(**ch_options) \
      .option("create_table_options", "ENGINE = ReplacingMergeTree(load_datetime)") \
      .option("order_by", "load_date, asset_id") \
      .option("partition_by", "toYYYYMM(load_date)") \
      .option("primary_key", "load_date, asset_id") \
      .mode("append") \
      .save()
    
  except Exception as e:
     logger.critical(f"❌ Job failed: {str(e)}", exc_info=True)

  finally:
    if spark:
       spark.stop()

if __name__ == "__main__":
    main()