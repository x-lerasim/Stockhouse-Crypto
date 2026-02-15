from pyspark.sql import SparkSession
from pyspark.sql.functions import col ,to_date, from_unixtime, row_number
from pyspark.sql.window import Window 
import logging
import argparse

logging.basicConfig(
    level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Core')

def deduplication(df):
    w = Window.partitionBy("asset_id","load_date") \
                    .orderBy(col("load_datetime").desc())
    
    deduped_df = df.withColumn("rn", row_number().over(w)) \
                    .filter(col("rn") == 1) \
                    .drop("rn")

    logger.info(f"Deduplication complete")
    return deduped_df

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
        
        raw_path ="s3a://raw/assets/"
        df = spark.read.parquet(raw_path).filter(col("load_date")== exec_date).cache()

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


        dim_df=dim_df.filter(
                        col('price_usd').isNotNull() & 
                        (col('price_usd')>0) &
                        col('market_cap').isNotNull() & 
                        (col('market_cap') > 0) 
                    )
        
        filtered_count = dim_df.count()
        logger.info(f"After quality filters: {filtered_count} records")

        # DEDUPLICATION
        
        dim_df=deduplication(dim_df)
        
        core_path = "s3a://core/dim_assets/"

        dim_df.write \
            .mode('overwrite') \
            .option('partitionOverwriteMode' , 'dynamic') \
            .partitionBy('load_date') \
            .parquet(core_path)
        
        logger.info("Success")
    except Exception as e:
            logger.critical(f"❌ Job failed: {str(e)}", exc_info=True)
            
    finally:
        if spark:
            spark.catalog.clearCache()
            spark.stop()

if __name__ == "__main__":
    main()