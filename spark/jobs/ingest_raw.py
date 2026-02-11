from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, to_date, lit
import requests
import logging
import time
import os
from requests.exceptions import RequestException

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('raw')
logger.info('Ingesting raw data from API to MinIO')

BASE_URL = 'https://rest.coincap.io/v3'
API_KEY = os.getenv("COINCAP_API_KEY")

if not API_KEY:
    logger.critical("COINCAP_API_KEY environment variable is not set")
    raise ValueError("COINCAP_API_KEY is required")

HEADERS = {
    'Authorization': f'Bearer {API_KEY}'
}

def get_assets(max_retries=3, timeout=30):
    URL=f"{BASE_URL}/assets"
    for attempt in range(max_retries):
        try:
            logger.info(f"API Request attempt {attempt + 1}/{max_retries}")
            
            response = requests.get(URL, headers=HEADERS, timeout=timeout)
            response.raise_for_status()
            
            data = response.json()

            if 'data' not in data or 'timestamp' not in data:
                raise ValueError("Invalid API response structure")
            
            if not data['data']:
                raise ValueError("API returned empty data array - this is unexpected")
            
            logger.info(f"Successfully fetched {len(data['data'])} assets")
            return data
        
        except RequestException as e:
            logger.error(f"Request failed (attempt {attempt + 1}): {str(e)}")

            if attempt == max_retries - 1:
                logger.critical("All retry attempts exhausted")
                raise

            wait_time = 2 ** attempt
            logger.info(f"Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
        
        except ValueError as e:
            logger.error(f"Data validation error: {str(e)}")
            raise

def main():
    "API -> Raw"
    spark = None

    try:
        spark = SparkSession.builder \
                .appName("CryptoRawIngestion") \
                .config("spark.sql.shuffle.partitions", "1") \
                .getOrCreate()
        logger.info("Spark session created successfully")

        assets_json = get_assets()

        api_timestamp = assets_json["timestamp"]
        data = assets_json["data"]
        
        df = spark.createDataFrame(data)
        df = df.withColumn('timestamp', lit(api_timestamp))
        df = df.withColumn('load_datetime', from_unixtime(col('timestamp')/1000))
        df = df.withColumn('load_date', to_date(col('load_datetime')))

        initial_count = df.count()

        output_path = "s3a://raw/assets/"

        df.write \
            .mode('append') \
            .partitionBy('load_date') \
            .parquet(output_path)
        
        logger.info(f"✅ Successfully loaded {initial_count} records to raw layer")
    
    except Exception as e:
        logger.critical(f"❌ Job failed with error: {str(e)}", exc_info=True)
        raise
    
    finally:
        if spark:
            spark.stop()
            logger.info("Spark session stopped")

if __name__ == "__main__":
    main()