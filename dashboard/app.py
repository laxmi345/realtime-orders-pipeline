"""Live dashboard reading from PostgreSQL (auto-refresh every 5s)."""
import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

DB_URL = os.getenv("DB_URL", "postgresql+psycopg2://analytics:analytics@postgres:5432/ordersdb")

st.set_page_config(page_title="Live Orders Dashboard", layout="wide")
st_autorefresh(interval=5000, key="refresh")
engine = create_engine(DB_URL)


def query(sql):
    return pd.read_sql(text(sql), engine)


st.title("Real-Time Orders Dashboard")
st.caption("Kafka -> Spark Structured Streaming -> PostgreSQL -> Streamlit")

try:
    kpi = query("SELECT * FROM kpis").fillna(0).iloc[0]
except Exception as exc:  # DB/table not ready yet
    st.warning("Waiting for data pipeline... (%s)" % exc)
    st.stop()

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Orders", int(kpi["total_orders"]))
c2.metric("Total Revenue", "Rs {:,.0f}".format(float(kpi["total_revenue"])))
c3.metric("Avg Order Value", "Rs {:,.0f}".format(float(kpi["avg_order_value"])))
c4.metric("Unique Customers", int(kpi["unique_customers"]))

per_min = query("SELECT * FROM sales_per_minute")
if per_min.empty:
    st.info("No orders yet. Wait a few seconds for Spark to write the first batch.")
    st.stop()

st.subheader("Revenue per minute (last 30 min)")
st.plotly_chart(px.line(per_min, x="minute", y="revenue", markers=True), use_container_width=True)

left, right = st.columns(2)
with left:
    st.subheader("Revenue by category")
    st.plotly_chart(px.bar(query("SELECT * FROM category_revenue"), x="category", y="revenue"),
                    use_container_width=True)
with right:
    st.subheader("Revenue by city")
    st.plotly_chart(px.bar(query("SELECT * FROM city_revenue"), x="city", y="revenue"),
                    use_container_width=True)

left2, right2 = st.columns(2)
with left2:
    st.subheader("Payment method mix")
    st.plotly_chart(px.pie(query("SELECT * FROM payment_mix"), names="payment_method", values="orders"),
                    use_container_width=True)
with right2:
    st.subheader("Latest orders")
    st.dataframe(query("SELECT order_id, category, city, amount, event_time FROM orders "
                       "ORDER BY event_time DESC LIMIT 10"), use_container_width=True)
