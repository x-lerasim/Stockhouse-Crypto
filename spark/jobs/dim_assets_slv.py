from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, lit, explode, from_unixtime
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Core')


def deduplication(df):
    w = Window.partitionBy("asset_id").orderBy(col("load_datetime").desc())
    deduped_df = (
        df.withColumn("rn", row_number().over(w))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    logger.info("Deduplication complete")
    return deduped_df


def get_latest_core_snapshot(spark, core_path):
    try:
        existing_core_df = spark.read.parquet(core_path).select(
            "asset_id",
            "symbol",
            "asset_name",
            "rank",
            "price_usd",
            "change_24h",
            "market_cap",
            "load_datetime",
        )
    except AnalysisException:
        logger.info("Core dataset does not exist yet. First load will insert all rows.")
        return None

    latest_window = Window.partitionBy("asset_id").orderBy(col("load_datetime").desc())
    return (
        existing_core_df.withColumn("rn", row_number().over(latest_window))
        .filter(col("rn") == 1)
        .drop("rn", "load_datetime")
    )


def main():
    "Raw -> Core"
    parser = argparse.ArgumentParser()
    parser.add_argument("--execution-date")
    args = parser.parse_args()
    exec_date = args.execution_date

    spark = None
    try:
        spark = SparkSession.builder \
            .appName("CoreTransformation") \
            .getOrCreate()

        raw_path = f"s3a://raw/assets/load_date={exec_date}/"
        raw_df = spark.read.json(raw_path)
        df = (
            raw_df.select(
                col("timestamp").cast("long").alias("timestamp"),
                col("data").alias("assets"),
            )
            .select("timestamp", explode(col("assets")).alias("asset"))
            .select("timestamp", col("asset.*"))
            .withColumn("load_date", lit(exec_date).cast("date"))
            .withColumn(
                "load_datetime",
                from_unixtime((col("timestamp") / 1000).cast("long")).cast("timestamp")
            )
            .cache()
        )

        initial_count = df.count()
        logger.info(f"Read {initial_count} records from raw")

        dim_df = df.select(
            col('id').cast('string').alias('asset_id'),
            col('symbol').cast('string'),
            col('name').cast('string').alias('asset_name'),
            col('rank').cast('int'),
            col('priceUsd').cast('decimal(28,8)').alias('price_usd'),
            col('changePercent24Hr').cast('decimal(18,8)').alias('change_24h'),
            col('marketCapUsd').cast('decimal(38,2)').alias('market_cap'),
            col('load_datetime').cast('timestamp'),
            col('load_date')
        )

        dim_df = dim_df.filter(
            col('price_usd').isNotNull() &
            (col('price_usd') > 0) &
            col('market_cap').isNotNull() &
            (col('market_cap') > 0)
        )

        filtered_count = dim_df.count()
        logger.info(f"After quality filters: {filtered_count} records")

        dim_df = deduplication(dim_df)
        core_path = "s3a://core/dim_assets/"
        latest_core_df = get_latest_core_snapshot(spark, core_path)

        if latest_core_df is not None:
            src = dim_df.alias("src")
            tgt = latest_core_df.alias("tgt")
            changed_df = (
                src.join(tgt, on="asset_id", how="left")
                .filter(
                    col("tgt.asset_id").isNull() |
                    (~col("src.symbol").eqNullSafe(col("tgt.symbol"))) |
                    (~col("src.asset_name").eqNullSafe(col("tgt.asset_name"))) |
                    (~col("src.rank").eqNullSafe(col("tgt.rank"))) |
                    (~col("src.price_usd").eqNullSafe(col("tgt.price_usd"))) |
                    (~col("src.change_24h").eqNullSafe(col("tgt.change_24h"))) |
                    (~col("src.market_cap").eqNullSafe(col("tgt.market_cap")))
                )
                .select("src.*")
            )
        else:
            changed_df = dim_df

        changed_count = changed_df.count()
        logger.info(f"Incremental rows to write: {changed_count}")
        if changed_count == 0:
            logger.info("No changed assets detected. Skipping core write.")
            return

        changed_df.write \
            .mode('append') \
            .partitionBy('load_date') \
            .parquet(core_path)

        logger.info("Success")
    except Exception as e:
        logger.critical(f"Job failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.catalog.clearCache()
            spark.stop()


if __name__ == "__main__":
    main()
