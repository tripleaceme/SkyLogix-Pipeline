# SkyLogix Weather Data Pipeline

Real-time weather data pipeline for **SkyLogix Transportation**, a logistics company managing 1,200+ delivery trucks across Lagos, Accra, and Johannesburg.

## Architecture

```
OpenWeather API  →  MongoDB (Staging)  →  Airbyte (Incremental | Append)  →  DuckDB (Warehouse)  →  Dashboard
      |                   |                        |                          |                    |
 fetch_weather.py    mongo_loader.py         Incremental |             transform.py         dashboard.py
 (API → JSON)        (JSON → MongoDB)          Append                (raw → star schema)   (Streamlit)
```

## Tech Stack

| Component       | Technology        | Purpose                                          |
|-----------------|-------------------|--------------------------------------------------|
| Ingestion       | Python            | Fetch weather API data, upsert to MongoDB        |
| Staging DB      | MongoDB Atlas     | Store raw weather JSON feeds (cloud-hosted)      |
| Replication     | Airbyte (Docker)  | Sync from MongoDB to DuckDB (Full Refresh)       |
| Warehouse       | DuckDB            | Analytical star schema for dashboarding          |
| BI Dashboard    | Streamlit + Plotly| Real-time operational weather dashboard          |
| Version Control | GitHub            | Code, docs, and configuration                    |

## Data Model (Star Schema)

**Dimension Tables:**
- `dim_city` - city_id, city_name, latitude, longitude, country, timezone_offset
- `dim_weather_condition` - condition_id, main, description, icon

**Fact Table:**
- `fact_weather_readings` - temperature, feels_like, humidity, pressure, wind_speed, visibility, cloud_coverage (linked to dimensions via city_id and condition_id)

## Project Structure

```
SkyLogix-Pipeline/
├── config.py           # Centralized configuration (env variables)
├── fetch_weather.py    # Step 1: Fetch weather data from OpenWeather API
├── mongo_loader.py     # Step 2: Load fetched data into MongoDB
├── transform.py        # Step 3: Transform Airbyte raw data into star schema
├── dashboard.py        # Step 4: Streamlit + Plotly operational dashboard
├── analyse.py          # Pandas analytics on the warehouse (CLI)
├── AIRBYTE_SETUP.md    # Step-by-step Airbyte configuration guide
├── requirements.txt    # Python dependencies
├── .env.example        # Environment variable template
└── README.md
```

## Setup

### Prerequisites
- Python 3.8+
- MongoDB Atlas account (free M0 tier) — [cloud.mongodb.com](https://cloud.mongodb.com)
- Docker Desktop (for Airbyte)
- OpenWeather API key (free tier: https://openweathermap.org/api)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure environment
```bash
cp .env.example .env
# Edit .env with your API key and MongoDB URI
```

### 3. Run the ingestion pipeline
```bash
# Fetches weather for all cities and loads into MongoDB
python mongo_loader.py
```

### 4. Set up Airbyte
See [AIRBYTE_SETUP.md](AIRBYTE_SETUP.md) for the full step-by-step guide covering:
- Installing Airbyte via Docker (`abctl` or Docker Compose)
- Configuring MongoDB Atlas as source
- Configuring DuckDB as destination
- Setting up Full Refresh | Overwrite sync (see Limitations)

### 5. Run the transform
```bash
python transform.py
```
Reads from Airbyte's raw DuckDB output and populates the star schema in `analytics.duckdb`.

### 6. Launch the dashboard
```bash
streamlit run dashboard.py
```
Opens a browser with live KPI cards, charts, operational alerts, and the raw readings table.

### 7. Run CLI analytics (optional)
```bash
python analyse.py
```
Runs pandas-based analysis: temperature stats, wind analysis, weather breakdowns, and extreme condition flags.

## Cities Monitored

| City         | Country      |
|--------------|--------------|
| Lagos        | Nigeria      |
| Accra        | Ghana        |
| Johannesburg | South Africa |

---

## Limitations & Deviations from Original Spec

The following technical constraints were encountered during implementation that required deviations from the originally planned architecture.

### 1. Airbyte DuckDB Destination: No `Incremental | Append + Deduped`

**Planned:** `Incremental | Append + Deduped` sync mode with `updatedAt` as cursor field and `_id` as primary key — so Airbyte would handle deduplication automatically.

**Actual:** The DuckDB destination connector does not support `Incremental | Append + Deduped`. The only available modes are `Full Refresh | Overwrite` and `Incremental | Append`.

**Workaround:** Using `Incremental | Append`, which upload only new data table on every sync. Deduplication is handled downstream in `transform.py` via a `WHERE NOT EXISTS` clause that checks `city_id + updated_at` before inserting into the fact table.

### 2. Airbyte MongoDB Connector Enforces TLS — Cannot Use Local MongoDB

**Planned:** Connect Airbyte to a locally-running MongoDB instance.

**Actual:** The Airbyte MongoDB source connector (v1.0.10) requires a TLS/SSL-encrypted connection and cannot be configured to connect to an unencrypted local MongoDB instance. Attempts resulted in `SSLHandshakeException`.

**Workaround:** Migrated staging database to **MongoDB Atlas** (M0 free tier), which provides TLS out of the box. A dedicated read-only `airbyte_reader` user was created in Atlas for the Airbyte source connection.


### 3. MongoDB Staging Uses Upsert (Current State Only)

**Planned:** MongoDB as an append-style staging store accumulating time-series records.

**Actual:** `mongo_loader.py` performs an `update_one(..., upsert=True)` keyed on `city_id`. This means MongoDB always holds exactly **3 documents** (one per city) reflecting the latest reading.

**Impact:** Historical accumulation happens exclusively in the DuckDB warehouse. Each `mongo_loader.py` run updates `updatedAt`, Airbyte replaces the raw DuckDB table on sync, and `transform.py` appends any new `(city_id, updated_at)` combinations to the fact table, so time-series data is preserved at the warehouse layer.

### 4. Airbyte Raw DuckDB Path is Ephemeral

**Planned:** Persistent storage for the Airbyte-written DuckDB file.

**Actual:** Airbyte writes to `/tmp/airbyte_local/weather_raw.duckdb` on the host machine. The `/tmp/` directory is cleared on system restart, so the path needs to be recreated (`mkdir -p /tmp/airbyte_local`) and a new sync triggered after each reboot. In a production environment, this would be mapped to a persistent volume.
