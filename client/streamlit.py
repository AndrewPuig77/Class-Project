import streamlit as st
import pandas as pd 
import plotly.express as px
import plotly.graph_objects as go
import requests
from datetime import datetime, timedelta

# Page config
st.set_page_config(page_title="Water Quality Dashboard", layout="wide")

# API base URL
API_BASE = "http://127.0.0.1:5000/api"

# Title
st.title("🌊 Water Quality Data Dashboard")

# Sidebar Controls
st.sidebar.title("Control Panel")

# Date filtering
st.sidebar.subheader("Date Filter")
date_filter = st.sidebar.text_input("Date (MM/DD/YY)", placeholder="12/16/21")

# Temperature range
st.sidebar.subheader("Temperature (°C)")
temp_min = st.sidebar.number_input("Min Temp", value=None)
temp_max = st.sidebar.number_input("Max Temp", value=None)

# Salinity range
st.sidebar.subheader("Salinity (ppt)")
sal_min = st.sidebar.number_input("Min Salinity", value=None)
sal_max = st.sidebar.number_input("Max Salinity", value=None)

# ODO range
st.sidebar.subheader("ODO (mg/L)")
odo_min = st.sidebar.number_input("Min ODO", value=None)
odo_max = st.sidebar.number_input("Max ODO", value=None)

# Pagination
st.sidebar.subheader("Pagination")
limit = st.sidebar.slider("Limit", min_value=100, max_value=1000, value=100, step=10)
skip = st.sidebar.number_input("Skip", min_value=0, value=0, step=10)

# Apply filters button
apply_filters = st.sidebar.button("Apply Filters", type="primary")

# Build query parameters
params = {"limit": limit, "skip": skip}
if date_filter:
    params["date"] = date_filter
if temp_min is not None:
    params["min_temp"] = temp_min
if temp_max is not None:
    params["max_temp"] = temp_max
if sal_min is not None:
    params["min_sal"] = sal_min
if sal_max is not None:
    params["max_sal"] = sal_max
if odo_min is not None:
    params["min_odo"] = odo_min
if odo_max is not None:
    params["max_odo"] = odo_max

# Fetch data from API
try:
    response = requests.get(f"{API_BASE}/observations", params=params, timeout=50)
    
    if response.status_code == 200:
        data = response.json()
        items = data.get("items", [])
        total_count = data.get("count", 0)
        
        # Display counts
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Total Matching Records", total_count)
        with col2:
            st.metric("Returned Records", len(items))
        with col3:
            st.metric("Current Page", skip // limit + 1 if limit > 0 else 1)
        
        if items:
            df = pd.DataFrame(items)
            
            # Data Table
            st.subheader("Observations Data")
            
            # Show key columns first
            display_cols = ["date", "Time", "latitude", "longitude", "temperature", "salinity", "odo"]
            available_cols = [col for col in display_cols if col in df.columns]
            st.dataframe(df[available_cols], use_container_width=True, height=300)
            
            # Download button
            csv = df.to_csv(index=False)
            st.download_button(
                label="Download Data as CSV",
                data=csv,
                file_name="water_quality_data.csv",
                mime="text/csv"
            )
            
            # Visualizations
            st.subheader("Visualizations")
            
            # Create tabs for different charts
            tab1, tab2, tab3, tab4 = st.tabs(["Temperature Over Time", "Salinity Distribution", "Temp vs Salinity", "Map View"])
            
            with tab1:
                # Line chart - Temperature over time
                if "temperature" in df.columns:
                    # Create a combined datetime for better plotting
                    df_sorted = df.sort_index()
                    fig1 = px.line(df_sorted, y="temperature", 
                                  title="Temperature Over Time",
                                  labels={"temperature": "Temperature (°C)", "index": "Record Index"})
                    fig1.update_traces(line_color='#FF6B6B')
                    st.plotly_chart(fig1, use_container_width=True)
                else:
                    st.warning("Temperature data not available")
            
            with tab2:
                # Histogram - Salinity distribution
                if "salinity" in df.columns:
                    fig2 = px.histogram(df, x="salinity", nbins=30,
                                       title="Salinity Distribution",
                                       labels={"salinity": "Salinity (ppt)", "count": "Frequency"})
                    fig2.update_traces(marker_color='#4ECDC4')
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.warning("Salinity data not available")
            
            with tab3:
                # Scatter plot - Temperature vs Salinity colored by ODO
                if all(col in df.columns for col in ["temperature", "salinity", "odo"]):
                    fig3 = px.scatter(df, x="temperature", y="salinity", color="odo",
                                     title="Temperature vs Salinity (colored by ODO)",
                                     labels={"temperature": "Temperature (°C)", 
                                            "salinity": "Salinity (ppt)",
                                            "odo": "ODO (mg/L)"},
                                     color_continuous_scale="Viridis")
                    st.plotly_chart(fig3, use_container_width=True)
                else:
                    st.warning("Required data not available for scatter plot")
            
            with tab4:
                # Map view
                if all(col in df.columns for col in ["latitude", "longitude"]):
                    fig4 = px.scatter_mapbox(df, lat="latitude", lon="longitude",
                                            hover_data=["temperature", "salinity", "odo", "date"],
                                            color="temperature",
                                            size_max=15,
                                            zoom=10,
                                            title="Observation Locations")
                    fig4.update_layout(mapbox_style="open-street-map")
                    st.plotly_chart(fig4, use_container_width=True)
                else:
                    st.warning("Location data not available")
        
        else:
            st.warning("No observations found with the current filters.")
    
    else:
        st.error(f"API request failed with status code {response.status_code}")
        st.write("Response:", response.text)

except requests.exceptions.RequestException as e:
    st.error(f"Failed to connect to API: {e}")
    st.info("Make sure your Flask API is running at http://127.0.0.1:5000")

# Statistics Panel
st.divider()
st.subheader("Summary Statistics")

try:
    stats_response = requests.get(f"{API_BASE}/stats", timeout=50)
    if stats_response.status_code == 200:
        stats = stats_response.json()
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.markdown("**Temperature**")
            if "temperature" in stats:
                st.write(f"Min: {stats['temperature']['min']:.2f} °C")
                st.write(f"Max: {stats['temperature']['max']:.2f} °C")
                st.write(f"Avg: {stats['temperature']['avg']:.2f} °C")
                st.write(f"Std Dev: {stats['temperature']['stddev']:.2f}")
        
        with col2:
            st.markdown("**Salinity**")
            if "salinity" in stats:
                st.write(f"Min: {stats['salinity']['min']:.2f} ppt")
                st.write(f"Max: {stats['salinity']['max']:.2f} ppt")
                st.write(f"Avg: {stats['salinity']['avg']:.2f} ppt")
                st.write(f"Std Dev: {stats['salinity']['stddev']:.2f}")
        
        with col3:
            st.markdown("**ODO**")
            if "odo" in stats:
                st.write(f"Min: {stats['odo']['min']:.2f} mg/L")
                st.write(f"Max: {stats['odo']['max']:.2f} mg/L")
                st.write(f"Avg: {stats['odo']['avg']:.2f} mg/L")
                st.write(f"Std Dev: {stats['odo']['stddev']:.2f}")
    else:
        st.error("Failed to fetch statistics")
except Exception as e:
    st.error(f"Error fetching stats: {e}")

# Outliers Panel
st.divider()
st.subheader("Outlier Detection")

col1, col2, col3 = st.columns(3)
with col1:
    outlier_field = st.selectbox("Field", ["temperature", "salinity", "odo"])
with col2:
    outlier_method = st.selectbox("Method", ["zscore", "iqr"])
with col3:
    outlier_k = st.number_input("K value", value=3.0 if outlier_method == "zscore" else 1.5, step=0.1)

if st.button("Detect Outliers"):
    try:
        outlier_params = {"field": outlier_field, "method": outlier_method, "k": outlier_k}
        outlier_response = requests.get(f"{API_BASE}/outliers", params=outlier_params, timeout=50)
        
        if outlier_response.status_code == 200:
            outlier_data = outlier_response.json()
            outlier_count = outlier_data.get("count", 0)
            outliers = outlier_data.get("outliers", [])
            
            st.metric("Outliers Found", outlier_count)
            
            if outliers:
                outlier_df = pd.DataFrame(outliers)
                display_cols = [outlier_field, "latitude", "longitude", "Date"]
                if outlier_method == "zscore" and "z_score" in outlier_df.columns:
                    display_cols.append("z_score")
                available = [col for col in display_cols if col in outlier_df.columns]
                st.dataframe(outlier_df[available], use_container_width=True)
            else:
                st.success("No outliers detected with current parameters!")
        else:
            st.error("Failed to fetch outliers")
    except Exception as e:
        st.error(f"Error detecting outliers: {e}")