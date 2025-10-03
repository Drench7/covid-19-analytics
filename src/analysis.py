from pyspark.sql import DataFrame
from pyspark.sql.functions import *
from pyspark.sql.window import Window

class CovidAnalysis:
    """COVID-19 data analysis using Spark"""
    
    @staticmethod
    def get_global_summary(df: DataFrame) -> DataFrame:
        """Get global summary statistics"""
        return df.groupBy("date").agg(
            sum("confirmed").alias("total_confirmed"),
            sum("deaths").alias("total_deaths"),
            sum("recovered").alias("total_recovered"),
            sum("active").alias("total_active")
        ).orderBy("date")
    
    @staticmethod
    def get_country_analysis(df: DataFrame) -> DataFrame:
        """Analyze data by country"""
        window_spec = Window.partitionBy("country").orderBy("date")
        
        country_analysis = df \
            .withColumn("daily_cases", 
                       col("confirmed") - lag("confirmed", 1).over(window_spec)) \
            .withColumn("daily_deaths",
                       col("deaths") - lag("deaths", 1).over(window_spec)) \
            .filter(col("daily_cases").isNotNull())
        
        return country_analysis
    
    @staticmethod
    def get_top_countries(df: DataFrame, metric: str = "confirmed", limit: int = 10) -> DataFrame:
        """Get top countries by specified metric"""
        latest_data = df.filter(col("date") == df.select(max("date")).collect()[0][0])
        
        return latest_data \
            .orderBy(col(metric).desc()) \
            .select("country", "confirmed", "deaths", "recovered", "mortality_rate") \
            .limit(limit)
    
    @staticmethod
    def calculate_growth_rates(df: DataFrame, days: int = 7) -> DataFrame:
        """Calculate growth rates over specified days"""
        window_spec = Window.partitionBy("country").orderBy("date")
        
        growth_df = df \
            .withColumn("prev_cases", lag("confirmed", days).over(window_spec)) \
            .withColumn("growth_rate",
                       when(col("prev_cases") > 0,
                           (col("confirmed") - col("prev_cases")) / col("prev_cases") * 100)
                       .otherwise(0)) \
            .filter(col("prev_cases").isNotNull())
        
        return growth_df
    
    @staticmethod
    def get_trend_analysis(df: DataFrame, country: str = None) -> DataFrame:
        """Analyze trends for specific country or globally"""
        base_df = df.filter(col("country") == country) if country else df
        
        trend_analysis = base_df.groupBy("date").agg(
            sum("confirmed").alias("total_confirmed"),
            sum("deaths").alias("total_deaths")
        ).orderBy("date")
        
        # Calculate 7-day moving averages
        window_spec = Window.orderBy("date").rowsBetween(-6, 0)
        
        return trend_analysis \
            .withColumn("confirmed_ma", 
                       avg("total_confirmed").over(window_spec)) \
            .withColumn("deaths_ma",
                       avg("total_deaths").over(window_spec))