# create_sample_data.py
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_sample_data():
    """Create sample COVID-19 data for testing"""
    
    countries = ['United States', 'India', 'Brazil', 'Russia', 'France', 
                'Germany', 'UK', 'Italy', 'Spain', 'Japan', 'China',
                'South Korea', 'Mexico', 'Canada', 'Australia']
    
    dates = pd.date_range(start='2023-01-01', end='2023-12-31', freq='D')
    
    data = []
    for country in countries:
        # Different starting points for each country
        base_cases = np.random.randint(1000, 50000)
        for date in dates:
            # Realistic growth with some randomness
            daily_growth = np.random.normal(1.01, 0.05)
            base_cases = max(0, int(base_cases * daily_growth))
            
            row = {
                'date': date.strftime('%Y-%m-%d'),
                'country': country,
                'confirmed': base_cases,
                'deaths': int(base_cases * np.random.uniform(0.01, 0.03)),
                'recovered': int(base_cases * np.random.uniform(0.7, 0.85)),
                'active': int(base_cases * np.random.uniform(0.1, 0.2)),
                'population': np.random.randint(10000000, 1000000000)
            }
            data.append(row)
    
    df = pd.DataFrame(data)
    df.to_csv('data/raw/covid_data.csv', index=False)
    print("Sample COVID-19 data created successfully!")
    print(f"File saved: data/raw/covid_data.csv")
    print(f"Records created: {len(df):,}")
    print(f"Date range: {dates[0].date()} to {dates[-1].date()}")
    print(f"Countries: {len(countries)}")

if __name__ == "__main__":
    create_sample_data()