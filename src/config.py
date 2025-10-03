import os
from pyspark.sql import SparkSession

class Config:
    """Configuration settings for Spark COVID analysis"""
    
    # Spark configuration
    SPARK_APP_NAME = "COVID-19 Analysis"
    SPARK_MASTER = "local[*]"  # Use "yarn" for cluster deployment
    
    # Data paths
    DATA_PATH = "data/raw/"
    OUTPUT_PATH = "data/processed/"
    
    @staticmethod
    def create_spark_session():
        """Create and configure Spark session"""
        return SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .master(Config.SPARK_MASTER) \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .getOrCreate()