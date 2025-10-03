from pyspark.sql import DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *
from config import Config

class DataLoader:
    """Load and preprocess COVID-19 data"""
    
    @staticmethod
    def load_covid_data(spark, file_path: str) -> DataFrame:
        """Load COVID-19 dataset"""
        
        # Define schema for better performance
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("country", StringType(), True),
            StructField("confirmed", IntegerType(), True),
            StructField("deaths", IntegerType(), True),
            StructField("recovered", IntegerType(), True),
            StructField("active", IntegerType(), True),
            StructField("population", IntegerType(), True)
        ])
        
        df = spark.read \
            .option("header", "true") \
            .schema(schema) \
            .csv(file_path)
        
        return df
    
    @staticmethod
    def preprocess_data(df: DataFrame) -> DataFrame:
        """Clean and preprocess the data"""
        
        # Handle missing values
        df_clean = df.fillna({
            'confirmed': 0,
            'deaths': 0,
            'recovered': 0,
            'active': 0
        })
        
        # Add derived columns
        df_processed = df_clean \
            .withColumn("mortality_rate", 
                       when(col("confirmed") > 0, 
                           col("deaths") / col("confirmed") * 100)
                       .otherwise(0)) \
            .withColumn("recovery_rate",
                       when(col("confirmed") > 0,
                           col("recovered") / col("confirmed") * 100)
                       .otherwise(0)) \
            .withColumn("cases_per_million",
                       when(col("population") > 0,
                           col("confirmed") / col("population") * 1000000)
                       .otherwise(0))
        
        return df_processed