from src.config import Config
from src.data_loader import DataLoader
from src.analysis import CovidAnalysis
from pyspark.sql import SparkSession

def main():
    """Main execution function"""
    
    # Initialize Spark
    spark = Config.create_spark_session()
    
    try:
        # Load data
        print("Loading COVID-19 data...")
        df = DataLoader.load_covid_data(spark, "data/raw/covid_data.csv")
        
        # Preprocess data
        print("Preprocessing data...")
        df_processed = DataLoader.preprocess_data(df)
        
        # Perform analyses
        print("Performing analysis...")
        
        # Global summary
        global_summary = CovidAnalysis.get_global_summary(df_processed)
        print("Global Summary:")
        global_summary.show(10)
        
        # Top countries
        top_countries = CovidAnalysis.get_top_countries(df_processed)
        print("Top 10 Countries by Confirmed Cases:")
        top_countries.show()
        
        # Country analysis
        country_analysis = CovidAnalysis.get_country_analysis(df_processed)
        print("Country Analysis Sample:")
        country_analysis.filter(col("country") == "United States").show(10)
        
        # Growth analysis
        growth_rates = CovidAnalysis.calculate_growth_rates(df_processed)
        print("Fastest Growing Countries:")
        growth_rates.filter(col("date") == growth_rates.select(max("date")).collect()[0][0]) \
                   .orderBy(col("growth_rate").desc()) \
                   .select("country", "growth_rate") \
                   .limit(10) \
                   .show()
        
        # Save results
        print("Saving results...")
        global_summary.write.mode("overwrite").parquet("data/processed/global_summary")
        top_countries.write.mode("overwrite").parquet("data/processed/top_countries")
        
        print("Analysis completed successfully!")
        
    except Exception as e:
        print(f"Error during analysis: {str(e)}")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    main()