# Dashboard/Dashboard.py
# ==========================================================
# PURPOSE:
# This Streamlit application visualizes real-time aggregated
# sales data stored in MongoDB. It allows interactive analysis
# by country, gender, or product with dynamic filters and charts.
# ==========================================================
import pandas as pd              # Data manipulation and analysis
import plotly.express as px      # Interactive visualizations
import streamlit as st           # Web app framework for data apps
import pymongo                   # MongoDB client for Python

# ==========================================================
# Streamlit Page Configuration
# ==========================================================
# WHAT:
# Defines page title, icon, and layout.
# WHY:
# Improves user experience and dashboard readability.
st.set_page_config(
    page_title="Sales Dashboard",
    page_icon=":bar_chart:",
    layout="wide"
)

# ==========================================================
# MongoDB Connection (Cached)
# ==========================================================
# WHAT:
# Creates a reusable MongoDB connection.
# WHY:
# Avoids reconnecting on every rerun (performance optimization).
# HOW:
# Streamlit cache_resource keeps the connection alive.
@st.cache_resource
def init_connection():
    return pymongo.MongoClient(**st.secrets["mongo"])

client = init_connection()

# ==========================================================
# Data Retrieval with Cache
# ==========================================================
# WHAT:
# Fetches documents from MongoDB collections.
# WHY:
# Reduces database load and improves dashboard performance.
# HOW:
# Cached for 10 minutes (ttl=600 seconds).
@st.cache_data(ttl=600)
def get_data(collection_name):
    db = client.Analysis
    items = list(db[collection_name].find())
    return items

# ==========================================================
# User Interaction: Dimension Selection
# ==========================================================
# WHAT:
# Dropdown allowing the user to choose the analysis dimension.
# WHY:
# Enables flexible analysis without multiple dashboards.
groupby_column = st.selectbox(
    'What would you like to analyse?',
    ('country', 'gender', 'Product_name'),
)

# ==========================================================
# Dynamic Collection Selection
# ==========================================================
# WHAT:
# Chooses MongoDB collection based on user selection.
# WHY:
# Each collection stores pre-aggregated Spark results.
if groupby_column == "Product_name":
    items = get_data("ProductAmount")
elif groupby_column == "gender":
    items = get_data("GenderAmount")
else:
    items = get_data("CountryAmount")

# ==========================================================
# Convert MongoDB Data to Pandas DataFrame
# ==========================================================
df = pd.DataFrame(items)

# ==========================================================
# Data Validation and Cleaning
# ==========================================================
# WHAT:
# Checks if data exists and prepares it for visualization.
if df.empty:
    st.warning("No data found in the selected collection!")
else:
    # Remove MongoDB internal ID if present
    if '_id' in df.columns:
        df = df.drop(columns=['_id'])

    # Ensure required aggregation column exists
    if 'total_order_amount' not in df.columns:
        st.error("Column 'total_order_amount' not found in the collection!")
    else:

        # ==================================================
        # Main Dashboard Title
        # ==================================================
        st.title(":bar_chart: Sales Dashboard")
        st.markdown("##")

        # ==================================================
        # KPI Section: Total Sales
        # ==================================================
        # WHAT:
        # Displays a global sales metric.
        # WHY:
        # Provides quick business insight at a glance.
        total_sales = df["total_order_amount"].sum()

        left_column, right_column = st.columns(2)
        with left_column:
            st.subheader("Total Sales:")
        with right_column:
            st.subheader(f"{total_sales} $")

        st.markdown("---")

        # ==================================================
        # Sidebar Filters
        # ==================================================
        # WHAT:
        # Allows interactive filtering of displayed data.
        # WHY:
        # Users can focus on specific countries/genders/products.
        st.sidebar.header("Please Filter Here:")
        fields = st.sidebar.multiselect(
            f"Select the {groupby_column}:",
            options=df[groupby_column].unique(),
            default=df[groupby_column].unique()
        )

        # Apply filter
        df_selection = df[df[groupby_column].isin(fields)]

        # ==================================================
        # Bar Chart Visualization
        # ==================================================
        # WHAT:
        # Displays total sales per selected dimension.
        # WHY:
        # Makes comparisons easy and intuitive.
        st.subheader(f"::fireworks:: Sales by {groupby_column} 📈")
        fig = px.bar(
            df_selection,
            x=groupby_column,
            y='total_order_amount',
            color='total_order_amount',
            color_continuous_scale=['red', 'yellow', 'green'],
            template='plotly_white',
            title=f'<b>Sales & Profit by {groupby_column}</b>'
        )

        # ==================================================
        # Pie Chart Visualization
        # ==================================================
        # WHAT:
        # Shows contribution percentage of each category.
        # WHY:
        # Helps identify dominant segments.
        fig1 = px.pie(
            df_selection,
            values="total_order_amount",
            names=groupby_column,
            title=f'<b>Sales & Profit by {groupby_column}</b>'
        )

        # Display charts side by side
        left_column, right_column = st.columns(2)
        left_column.plotly_chart(fig, use_container_width=True)
        right_column.plotly_chart(fig1, use_container_width=True)

        # ==================================================
        # UI Cleanup
        # ==================================================
        # WHAT:
        # Hides default Streamlit footer.
        # WHY:
        # Cleaner and more professional dashboard look.
        hide_st_style = """
        <style>
        footer {visibility: hidden;}
        </style>
        """
        st.markdown(hide_st_style, unsafe_allow_html=True)

# ==========================================================
# FINAL SUMMARY :
# ==========================================================
# 1. Spark writes aggregated streaming results into MongoDB.
# 2. Streamlit reads data efficiently using caching.
# 3. Users can dynamically select analysis dimensions.
# 4. KPIs and interactive charts provide business insights.
# 5. This dashboard is the final visualization layer
#    of the real-time data pipeline.
