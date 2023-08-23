# realtime_sales_app_bs.py

import pandas as pd
import plotly.express as px
import streamlit as st
import datetime
from millify import millify
from google.oauth2 import service_account
from google.cloud import bigquery

# Page config.
st.set_page_config(page_title="Realtime Sales Report - Beck Søndergaard")

# Custom CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400&display=swap');
    * {
        font-family: 'IBM Plex Sans', sans-serif !important;
    }
    div.css-1r6slb0.e1f1d6gn1 {
        border: 1px solid #CFCFCF;
        border-radius: 4px;
        padding: 2% 2% 2% 4%;
        margin-top: 8%;
    }
    </style>
    """,
    unsafe_allow_html=True
)

# Sign-on
def check_password():
    """Returns `True` if the user had the correct password."""

    def password_entered():
        """Checks whether a password entered by the user is correct."""
        if st.session_state["password"] == st.secrets["password"]:
            st.session_state["password_correct"] = True
            del st.session_state["password"]  # don't store password
        else:
            st.session_state["password_correct"] = False

    if "password_correct" not in st.session_state:
        # First run, show input for password.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        return False
    elif not st.session_state["password_correct"]:
        # Password not correct, show input + error.
        st.text_input(
            "Password", type="password", on_change=password_entered, key="password"
        )
        st.error("😕 Password incorrect")
        return False
    else:
        # Password correct.
        return True

if check_password():
    # Sidebar
    with st.sidebar:
        st.write("""
            ### About
            This report shows near-realtime data for sales across all Beck Søndergaard Shopify webshops.
    
            *Note: Use the date range selector to specify which period you are interested in.*
        """)
    
    # Create API client.
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    client = bigquery.Client(credentials=credentials)
    
    # Perform query.
    # Uses st.cache_data to only rerun when the query changes or after 10 min.
    @st.cache_data(ttl=600)
    def run_query(query):
        query_job = client.query(query)
        rows_raw = query_job.result()
        # Convert to list of dicts. Required for st.cache_data to hash the return value.
        rows = [dict(row) for row in rows_raw]
        return rows
    
    # Query: Orders
    orders_raw = run_query("""
    SELECT
      *,
      CASE
          WHEN presentment_currency = "GBP" THEN total_price * 8.66
          WHEN presentment_currency = "EUR" THEN total_price * 7.45
          WHEN presentment_currency = "NOK" THEN total_price * 0.64
          WHEN presentment_currency = "SEK" THEN total_price * 0.63
          WHEN presentment_currency = "DKK" THEN total_price
          ELSE total_price
      END AS total_price_converted
    FROM (SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
      FROM `performancemarketing-364011.shopify_dk.order`
        UNION ALL
          SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
          FROM `performancemarketing-364011.shopify_com.order`
            UNION ALL
              SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
              FROM `performancemarketing-364011.shopify_de.order`
                UNION ALL
                  SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
                  FROM `performancemarketing-364011.shopify_no.order`
                    UNION ALL
                      SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
                      FROM `performancemarketing-364011.shopify_se.order`
                        UNION ALL
                          SELECT  created_at, shipping_address_country, shipping_address_country_code, presentment_currency, total_price
                          FROM `performancemarketing-364011.shopify_uk.order`) AS tbl
    """)
    
    orders_df = pd.DataFrame.from_dict(orders_raw)
    orders_df['created_at'] = pd.to_datetime(orders_df['created_at'])
    orders_df['created_at_hour'] = orders_df['created_at'].dt.round('H')
    
    # Query: Ordered products 
    ordered_products_raw = run_query("""
    # Shopify DK
    SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
    FROM `performancemarketing-364011.shopify_dk.order` AS o
    LEFT JOIN `performancemarketing-364011.shopify_dk.order_line` as ol
    ON o.id = ol.order_id
      UNION ALL
        # Shopify COM
        SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
        FROM `performancemarketing-364011.shopify_com.order` AS o
        LEFT JOIN `performancemarketing-364011.shopify_com.order_line` as ol
        ON o.id = ol.order_id
          UNION ALL
            # Shopify DE
            SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
            FROM `performancemarketing-364011.shopify_de.order` AS o
            LEFT JOIN `performancemarketing-364011.shopify_de.order_line` as ol
            ON o.id = ol.order_id
              UNION ALL
                # Shopify UK
                SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
                FROM `performancemarketing-364011.shopify_uk.order` AS o
                LEFT JOIN `performancemarketing-364011.shopify_uk.order_line` as ol
                ON o.id = ol.order_id
                  UNION ALL
                    # Shopify SE
                    SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
                    FROM `performancemarketing-364011.shopify_se.order` AS o
                    LEFT JOIN `performancemarketing-364011.shopify_se.order_line` as ol
                    ON o.id = ol.order_id
                      UNION ALL
                        # Shopify NO
                        SELECT o.created_at, o.id, ol.quantity, ol.title, ol.variant_title, ol.vendor
                        FROM `performancemarketing-364011.shopify_no.order` AS o
                        LEFT JOIN `performancemarketing-364011.shopify_no.order_line` as ol
                        ON o.id = ol.order_id;
    """)
    
    ordered_products_df = pd.DataFrame.from_dict(ordered_products_raw)
    ordered_products_df['created_at'] = pd.to_datetime(ordered_products_df['created_at'])
    ordered_products_df['created_at_hour'] = ordered_products_df['created_at'].dt.round('H')
    
    # Logo
    st.image("bs_logo_ed_1.png")
    
    # Date range selector: Default to today and yesterday
    today_date = datetime.date.today()
    today_month = today_date.month
    today_year = today_date.year
    yesterday_day = today_date.day - 1
    yesterday_date = datetime.date(today_year, today_month, yesterday_day)
    
    d = st.date_input(
        label="Select period (two dates):",
        value=[yesterday_date, today_date],
        max_value=today_date
    )
    
    from_date = pd.to_datetime(d[0], utc=True)
    to_date = pd.to_datetime(d[1], utc=True).replace(hour=23, minute=59, second=59)
    
    
    # Plot: Hourly sales
    df = orders_df[['created_at_hour', 'total_price_converted']].groupby('created_at_hour').sum()
    df = df.reset_index()
    df = df.loc[(df['created_at_hour'] >= from_date) & (df['created_at_hour'] <= to_date)]
    
    st.markdown("<h4 style='text-align: center; font-weight: 300; margin-top: 6%;'>Hourly sales</h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-weight: 400; font-size: 12px; margin-bottom: -2%'>ACROSS COUNTRIES</p>", unsafe_allow_html=True)
    
    fig = px.bar(
        x='created_at_hour', y='total_price_converted', 
        data_frame=df,
        labels={'created_at_hour': 'Time of Day (by Hour)', 'total_price_converted': 'Sales (DKK)'},
        #color_discrete_sequence=['darkgrey']
        #line_shape='spline'
    )
    fig.update_layout(dragmode=False)
    fig.update_xaxes(fixedrange=True)
    fig.update_yaxes(fixedrange=True)
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Score cards
    col1, col2, col3 = st.columns(3)
    
    total_sales = orders_df.loc[
        (orders_df['created_at_hour'] >= from_date) & (orders_df['created_at_hour'] <= to_date), 
        ['total_price_converted']
    ].sum().round(2)
    total_sales_millified = millify(total_sales, precision=2)
    
    total_orders = orders_df.loc[
        (orders_df['created_at_hour'] >= from_date) & (orders_df['created_at_hour'] <= to_date), 
        ['total_price_converted']
    ].count()
    
    avg_basket = orders_df.loc[
        (orders_df['created_at_hour'] >= from_date) & (orders_df['created_at_hour'] <= to_date), 
        ['total_price_converted']
    ].mean().round(2)
    
    col1.metric(label="Total Sales (DKK)", value=total_sales_millified)
    col2.metric(label="Orders", value=total_orders)
    col3.metric(label="Avg. Basket (DKK)", value=avg_basket)
    
    
    # Plot: Shipping Countries
    lat_lon = pd.read_csv("latitude-longitude-countries.csv")
    joined = orders_df.merge(
        lat_lon[['Country', 'Latitude', 'Longitude']], 
        how='left', 
        left_on='shipping_address_country', 
        right_on='Country'
    )
    joined = joined.drop(columns='Country')
    joined = joined.loc[(joined['created_at_hour'] >= from_date) & (joined['created_at_hour'] <= to_date)]
    joined_grouped = joined[['shipping_address_country', 'Latitude', 'Longitude']].value_counts().reset_index()
    
    st.markdown("<h4 style='text-align: center; font-weight: 300; margin-top: 8%;'>Top countries</h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-weight: 400; font-size: 12px; margin-bottom: -8%;'>BY NUMBER OF SHIPMENTS</p>", unsafe_allow_html=True)
    
    px.set_mapbox_access_token("pk.eyJ1IjoiZGFuaWVsYWFydXAiLCJhIjoiY2xjcDZlOTg0MDF0dTN2bzQxdTk4Zzg0aCJ9.oIX1blOigCZ5KMle7NjCXQ")
    fig = px.scatter_mapbox(
        data_frame=joined_grouped,
        lat='Latitude',
        lon='Longitude',
        hover_name='shipping_address_country',
        size='count',
        size_max=40,
        center={'lat': 56, 'lon': 10},
        zoom=2,
        mapbox_style='light'
    )
    
    st.plotly_chart(fig, use_container_width=True)
    
    # Table of top products
    op_stats = ordered_products_df.loc[
        (ordered_products_df['created_at_hour'] >= from_date) & (ordered_products_df['created_at_hour'] <= to_date),
        ['title', 'quantity']
    ].groupby('title').sum('quantity')
    op_stats = op_stats.sort_values(by='quantity', ascending=False)
    
    quantity_sum = op_stats['quantity'].sum()
    op_stats['pct'] = op_stats['quantity'] / quantity_sum * 100
    op_stats['pct'] = op_stats['pct'].round(2)
    
    op_stats_head = op_stats.head(10)
    
    st.markdown("<h4 style='text-align: center; font-weight: 300; margin-top: -4%;'>Top products</h4>", unsafe_allow_html=True)
    st.markdown("<p style='text-align: center; font-weight: 400; font-size: 12px; margin-bottom: 4%;'>BY NUMBER OF ORDERS</p>", unsafe_allow_html=True)
    st.dataframe(
        op_stats_head,
        column_config={
            'title': 'Product name',
            'quantity': 'Items sold',
            'pct': 'Share (%)'
        },
        use_container_width=True
    )










