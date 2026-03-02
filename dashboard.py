import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from config import DUCKDB_PATH

st.set_page_config(
    page_title="SkyLogix Weather Dashboard",
    page_icon="🌤️",
    layout="wide"
)

# ── Data loading ──────────────────────────────────────────────────────────────

@st.cache_data(ttl=60)
def load_data():
    con = duckdb.connect(DUCKDB_PATH, read_only=True)
    df = con.execute("""
        SELECT
            f.reading_time,
            f.updated_at,
            c.city_name,
            c.country,
            c.latitude,
            c.longitude,
            w.main        AS condition_main,
            w.description AS condition_desc,
            w.icon,
            f.temperature,
            f.feels_like,
            f.humidity,
            f.pressure,
            f.wind_speed,
            f.wind_deg,
            f.wind_gust,
            f.visibility,
            f.cloud_coverage
        FROM fact_weather_readings f
        JOIN dim_city c ON f.city_id = c.city_id
        JOIN dim_weather_condition w ON f.condition_id = w.condition_id
        ORDER BY f.reading_time DESC
    """).fetchdf()
    con.close()
    return df


def load_latest(df):
    return df.sort_values("reading_time", ascending=False).groupby("city_name").first().reset_index()


# ── UI ────────────────────────────────────────────────────────────────────────

st.title("🚛 SkyLogix Transportation — Weather Operations Dashboard")
st.caption("Real-time weather intelligence for Lagos · Accra · Johannesburg")
st.divider()

try:
    df = load_data()
except Exception as e:
    st.error(f"Could not connect to warehouse: {e}")
    st.info("Run `python transform.py` first to populate the analytics warehouse.")
    st.stop()

if df.empty:
    st.warning("No data in warehouse yet. Run `python mongo_loader.py` then `python transform.py`.")
    st.stop()

latest = load_latest(df)

# ── KPI cards ─────────────────────────────────────────────────────────────────

st.subheader("Current Conditions")
cols = st.columns(3)

city_colors = {"Lagos": "#FF6B6B", "Accra": "#4ECDC4", "Johannesburg": "#45B7D1"}

for i, row in latest.iterrows():
    col = cols[i % 3]
    color = city_colors.get(row["city_name"], "#888")
    alert = "🔴" if row["temperature"] > 35 or row["wind_speed"] > 10 else "🟢"

    col.markdown(f"""
    <div style="border-left: 4px solid {color}; padding: 12px 16px; border-radius: 6px; background: #1e1e2e; margin-bottom: 8px;">
        <h3 style="margin:0; color:{color}">{alert} {row['city_name']}, {row['country']}</h3>
        <p style="margin:4px 0; font-size:2rem; font-weight:bold">{row['temperature']:.1f}°C</p>
        <p style="margin:2px 0; color:#aaa">Feels like {row['feels_like']:.1f}°C &nbsp;·&nbsp; {row['condition_desc'].title()}</p>
        <p style="margin:2px 0; color:#aaa">💧 {row['humidity']}% humidity &nbsp;·&nbsp; 💨 {row['wind_speed']} m/s wind</p>
        <p style="margin:2px 0; color:#aaa">👁️ {row['visibility']/1000:.1f} km visibility &nbsp;·&nbsp; ☁️ {row['cloud_coverage']}% cloud</p>
        <p style="margin:4px 0; font-size:0.75rem; color:#666">Updated: {row['reading_time'].strftime('%Y-%m-%d %H:%M UTC')}</p>
    </div>
    """, unsafe_allow_html=True)

st.divider()

# ── Charts row 1 ─────────────────────────────────────────────────────────────

col1, col2 = st.columns(2)

with col1:
    st.subheader("Temperature Comparison")
    fig = px.bar(
        latest,
        x="city_name", y="temperature",
        color="city_name",
        color_discrete_map=city_colors,
        labels={"temperature": "Temperature (°C)", "city_name": "City"},
        text=latest["temperature"].apply(lambda x: f"{x:.1f}°C"),
    )
    fig.add_hline(y=35, line_dash="dash", line_color="red",
                  annotation_text="High temp threshold (35°C)")
    fig.update_traces(textposition="outside")
    fig.update_layout(showlegend=False, plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
                      font_color="white", height=350)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("Humidity & Wind Speed")
    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="Humidity (%)",
        x=latest["city_name"],
        y=latest["humidity"],
        marker_color=["#FF6B6B", "#4ECDC4", "#45B7D1"],
        yaxis="y"
    ))
    fig.add_trace(go.Scatter(
        name="Wind Speed (m/s)",
        x=latest["city_name"],
        y=latest["wind_speed"],
        mode="markers+lines",
        marker=dict(size=12, color="orange"),
        line=dict(color="orange", dash="dot"),
        yaxis="y2"
    ))
    fig.update_layout(
        yaxis=dict(title="Humidity (%)", range=[0, 100]),
        yaxis2=dict(title="Wind Speed (m/s)", overlaying="y", side="right"),
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="white", legend=dict(orientation="h"),
        height=350
    )
    st.plotly_chart(fig, use_container_width=True)

# ── Charts row 2 ─────────────────────────────────────────────────────────────

col3, col4 = st.columns(2)

with col3:
    st.subheader("Weather Conditions")
    fig = px.pie(
        latest,
        names="city_name",
        values="temperature",
        color="city_name",
        color_discrete_map=city_colors,
        hole=0.45,
        custom_data=["condition_desc"]
    )
    fig.update_traces(
        textinfo="label+percent",
        hovertemplate="<b>%{label}</b><br>Temp: %{value:.1f}°C<br>%{customdata[0]}"
    )
    fig.update_layout(
        plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
        font_color="white", height=350,
        annotations=[dict(text="Temp Split", x=0.5, y=0.5, font_size=14, showarrow=False)]
    )
    st.plotly_chart(fig, use_container_width=True)

with col4:
    st.subheader("Temperature Over Time")
    if len(df) > 3:
        fig = px.line(
            df.sort_values("reading_time"),
            x="reading_time", y="temperature",
            color="city_name",
            color_discrete_map=city_colors,
            markers=True,
            labels={"reading_time": "Time", "temperature": "Temperature (°C)"}
        )
        fig.update_layout(
            plot_bgcolor="#0e1117", paper_bgcolor="#0e1117",
            font_color="white", height=350,
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Run multiple syncs to see temperature trends over time.")

# ── Operational alerts ────────────────────────────────────────────────────────

st.divider()
st.subheader("⚠️ Operational Alerts")

alerts = []
for _, row in latest.iterrows():
    if row["temperature"] > 35:
        alerts.append(f"🌡️ **{row['city_name']}** — High temperature ({row['temperature']:.1f}°C). Risk of vehicle overheating.")
    if row["wind_speed"] > 10:
        alerts.append(f"💨 **{row['city_name']}** — High wind speed ({row['wind_speed']} m/s). Caution for large vehicles.")
    if row["visibility"] < 3000:
        alerts.append(f"🌫️ **{row['city_name']}** — Low visibility ({row['visibility']/1000:.1f} km). Reduced driving safety.")
    if row["humidity"] > 85:
        alerts.append(f"💧 **{row['city_name']}** — High humidity ({row['humidity']}%). Risk of road fog.")

if alerts:
    for alert in alerts:
        st.warning(alert)
else:
    st.success("✅ All cities — No adverse weather conditions detected. Operations nominal.")

# ── Raw data table ────────────────────────────────────────────────────────────

st.divider()
with st.expander("📋 Raw readings table"):
    display_cols = ["reading_time", "city_name", "temperature", "feels_like",
                    "humidity", "pressure", "wind_speed", "visibility",
                    "cloud_coverage", "condition_desc"]
    st.dataframe(
        df[display_cols].rename(columns={
            "reading_time": "Time", "city_name": "City",
            "temperature": "Temp (°C)", "feels_like": "Feels Like",
            "humidity": "Humidity %", "pressure": "Pressure hPa",
            "wind_speed": "Wind m/s", "visibility": "Visibility m",
            "cloud_coverage": "Cloud %", "condition_desc": "Condition"
        }),
        use_container_width=True
    )
