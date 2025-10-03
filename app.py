# app.py
import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

# Set page configuration
st.set_page_config(
    page_title="COVID-19 Analytics Dashboard",
    page_icon="🦠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 3rem;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 10px;
        margin: 0.5rem 0;
    }
</style>
""", unsafe_allow_html=True)

# Title and description
st.markdown('<h1 class="main-header">🌍 COVID-19 Analytics Dashboard</h1>', unsafe_allow_html=True)
st.markdown("Comprehensive analysis of global COVID-19 data with interactive visualizations")

# Load data with caching
@st.cache_data
def load_data():
    try:
        df = pd.read_csv("data/raw/covid_data.csv")
        df['date'] = pd.to_datetime(df['date'])
        return df
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return pd.DataFrame()

df = load_data()

# Show data info in sidebar
st.sidebar.title("📊 Dataset Info")
if not df.empty:
    st.sidebar.write(f"**Records:** {len(df):,}")
    st.sidebar.write(f"**Countries:** {df['country'].nunique()}")
    st.sidebar.write(f"**Date Range:** {df['date'].min().date()} to {df['date'].max().date()}")

# Sidebar filters
st.sidebar.title("🔍 Filters")
available_countries = df['country'].unique() if not df.empty else []
selected_countries = st.sidebar.multiselect(
    "Select Countries:",
    options=available_countries,
    default=available_countries[:5] if len(available_countries) > 5 else available_countries
)

# Main content
if df.empty:
    st.error("No data loaded. Please check your data file.")
else:
    # Filter data based on selection
    filtered_df = df[df['country'].isin(selected_countries)] if selected_countries else df
    
    # KEY METRICS SECTION
    st.header("📈 Key Metrics")
    
    # Calculate latest global metrics
    latest_date = df['date'].max()
    latest_data = df[df['date'] == latest_date]
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_cases = latest_data['confirmed'].sum()
        st.metric("Total Confirmed Cases", f"{total_cases:,}")
    
    with col2:
        total_deaths = latest_data['deaths'].sum()
        st.metric("Total Deaths", f"{total_deaths:,}")
    
    with col3:
        mortality_rate = (total_deaths / total_cases * 100) if total_cases > 0 else 0
        st.metric("Global Mortality Rate", f"{mortality_rate:.2f}%")
    
    with col4:
        total_recovered = latest_data['recovered'].sum()
        st.metric("Total Recovered", f"{total_recovered:,}")

    # GLOBAL TRENDS SECTION
    st.header("🌍 Global Trends")
    
    # Global daily aggregation
    global_daily = df.groupby('date').agg({
        'confirmed': 'sum',
        'deaths': 'sum',
        'recovered': 'sum',
        'active': 'sum'
    }).reset_index()
    
    # Calculate daily changes
    global_daily['new_cases'] = global_daily['confirmed'].diff()
    global_daily['new_deaths'] = global_daily['deaths'].diff()
    
    # Create two columns for charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Cumulative Cases Over Time")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.plot(global_daily['date'], global_daily['confirmed'], label='Confirmed', linewidth=2, color='blue')
        ax.plot(global_daily['date'], global_daily['deaths'], label='Deaths', linewidth=2, color='red')
        ax.plot(global_daily['date'], global_daily['recovered'], label='Recovered', linewidth=2, color='green')
        ax.set_xlabel('Date')
        ax.set_ylabel('Number of Cases')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
        st.pyplot(fig)
    
    with col2:
        st.subheader("Daily New Cases")
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.bar(global_daily['date'], global_daily['new_cases'], alpha=0.7, color='lightblue', label='Daily Cases')
        # 7-day moving average
        global_daily['cases_7d_avg'] = global_daily['new_cases'].rolling(7).mean()
        ax.plot(global_daily['date'], global_daily['cases_7d_avg'], color='darkblue', linewidth=2, label='7-day Average')
        ax.set_xlabel('Date')
        ax.set_ylabel('New Cases per Day')
        ax.legend()
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)
        st.pyplot(fig)

    # COUNTRY COMPARISON SECTION
    st.header("🏛️ Country Comparison")
    
    # Get latest data for each country
    latest_by_country = df.loc[df.groupby('country')['date'].idxmax()].copy()
    latest_by_country['mortality_rate'] = (latest_by_country['deaths'] / latest_by_country['confirmed'] * 100).fillna(0)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Top 10 Countries - Confirmed Cases")
        top_countries = latest_by_country.nlargest(10, 'confirmed')
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.barh(top_countries['country'], top_countries['confirmed'], color='skyblue')
        ax.set_xlabel('Confirmed Cases')
        ax.set_title('Top 10 Countries by Confirmed Cases')
        # Add value labels
        for i, v in enumerate(top_countries['confirmed']):
            ax.text(v + v*0.01, i, f'{v:,}', va='center', fontsize=9)
        st.pyplot(fig)
    
    with col2:
        st.subheader("Mortality Rate Distribution")
        significant_countries = latest_by_country[latest_by_country['confirmed'] > 1000]
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.hist(significant_countries['mortality_rate'].dropna(), bins=20, 
                alpha=0.7, color='red', edgecolor='black')
        ax.axvline(significant_countries['mortality_rate'].mean(), 
                  color='darkred', linestyle='--', linewidth=2,
                  label=f'Mean: {significant_countries["mortality_rate"].mean():.2f}%')
        ax.set_xlabel('Mortality Rate (%)')
        ax.set_ylabel('Number of Countries')
        ax.legend()
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)

    # MORTALITY & RECOVERY ANALYSIS
    st.header("⚕️ Mortality & Recovery Analysis")
    
    latest_by_country['recovery_rate'] = (latest_by_country['recovered'] / latest_by_country['confirmed'] * 100).fillna(0)
    significant_countries = latest_by_country[latest_by_country['confirmed'] > 1000]
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Highest Mortality Rates")
        high_mortality = significant_countries.nlargest(10, 'mortality_rate')
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(high_mortality['country'], high_mortality['mortality_rate'], color='salmon')
        ax.set_xlabel('Mortality Rate (%)')
        ax.set_title('Countries with Highest Mortality Rates')
        for i, v in enumerate(high_mortality['mortality_rate']):
            ax.text(v + 0.1, i, f'{v:.2f}%', va='center', fontsize=9)
        st.pyplot(fig)
    
    with col2:
        st.subheader("Mortality vs Recovery Rate")
        fig, ax = plt.subplots(figsize=(10, 6))
        scatter = ax.scatter(significant_countries['mortality_rate'], 
                           significant_countries['recovery_rate'],
                           s=significant_countries['confirmed']/10000,
                           alpha=0.6)
        ax.set_xlabel('Mortality Rate (%)')
        ax.set_ylabel('Recovery Rate (%)')
        ax.set_title('Mortality Rate vs Recovery Rate')
        ax.grid(True, alpha=0.3)
        st.pyplot(fig)

    # DATA TABLE SECTION
    st.header("📋 Data Summary")
    
    # Show summary table
    summary_cols = ['country', 'confirmed', 'deaths', 'recovered', 'mortality_rate', 'recovery_rate']
    display_data = latest_by_country[summary_cols].round(2)
    st.dataframe(display_data.nlargest(15, 'confirmed'), use_container_width=True)

# Footer
st.markdown("---")
st.markdown("""
<div style='text-align: center'>
    <p>Built with ❤️ using Streamlit, Pandas, and Matplotlib</p>
    <p>Data Source: Sample COVID-19 Dataset</p>
</div>
""", unsafe_allow_html=True)