import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import logging

# Add parent directory to path to allow importing config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    # Make sure to include the required packages for Kafka and PostgreSQL
    # --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.postgresql:postgresql:42.6.0
    spark = SparkSession.builder \
        .appName("StockMarketPipeline") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("WARN")
    
    # Define schema matching our JSON payload
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True)
    ])

    # Read from Kafka
    logger.info(f"Connecting to Kafka at {settings.KAFKA_BOOTSTRAP_SERVERS}, topic: {settings.KAFKA_TOPIC}")
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", settings.KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", settings.KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Parse JSON value
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Cast timestamp
    transformed_df = parsed_df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd HH:mm:ss"))

    # Function to write batch to PostgreSQL
    def write_to_postgres(batch_df, batch_id):
        logger.info(f"Writing batch {batch_id} to PostgreSQL...")
        # Add driver for postgres if missing, typically run via spark-submit with --packages
        batch_df.write \
            .format("jdbc") \
            .option("url", settings.get_jdbc_url()) \
            .option("dbtable", "raw_stock_data") \
            .option("user", settings.POSTGRES_USER) \
            .option("password", settings.POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

    # Write stream
    query = transformed_df.writeStream \
        .foreachBatch(write_to_postgres) \
        .outputMode("append") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
