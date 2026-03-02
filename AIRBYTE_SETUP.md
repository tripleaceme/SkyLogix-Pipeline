# Airbyte Setup Guide: MongoDB Atlas to DuckDB

Step-by-step guide to configure Airbyte locally for the SkyLogix weather pipeline.

## Prerequisites

- Docker Desktop installed and running (at least 4 CPU cores, 8 GB RAM)
- MongoDB Atlas cluster created and populated with data from `mongo_loader.py`
- Ports 8000 and 8001 available

---

## Step 1: Install and Start Airbyte

### Option A: Using `abctl` (Recommended)

```bash
# Install the Airbyte CLI
brew tap airbytehq/tap
brew install abctl

# Start Airbyte (first run takes 5-10 minutes)
abctl local install

# Get your login credentials
abctl local credentials
```

### Option B: Using Docker Compose

```bash
git clone --depth 1 https://github.com/airbytehq/airbyte.git
cd airbyte
./run-ab-platform.sh
```

Once running, open **http://localhost:8000** and log in.

---

## Step 2: Configure MongoDB Atlas as Source

MongoDB Atlas handles TLS/SSL and replica set configuration automatically — no manual setup required.

1. In Airbyte UI, go to **Sources** > **+ New source**
2. Search for **MongoDB** and select it

Fill in these fields:

| Field | Value |
|-------|-------|
| **Cluster Type** | `MongoDB Atlas Replica Set` |
| **Connection String** | `mongodb+srv://airbyte_reader:<password>@skyline-pipeline.6nodhka.mongodb.net/?authSource=admin` |
| **Database Name** | `weather_data` |
| **Username** | `airbyte_reader` |
| **Password** | *(Airbyte reader password)* |
| **Authentication Source** | `admin` |

> **Note on password special characters:** If your password contains `%`, it must be URL-encoded as `%25` in the connection string. Other special characters: `@` → `%40`, `*` → `%2A`.
>
> **Why a separate read-only user?** Airbyte only needs read access to sync data. Using a dedicated `airbyte_reader` account limits exposure if credentials are ever compromised.

3. Click **Set up source**. Airbyte will discover the `weather_raw` collection.

---

## Step 3: Configure DuckDB as Destination

1. Go to **Destinations** > **+ New destination**
2. Search for **DuckDB** and select it

Fill in these fields:

| Field | Value |
|-------|-------|
| **Destination name** | `DuckDB - weather warehouse` |
| **Destination Path** | `/local/weather_raw.duckdb` |

> **Path mapping:** Airbyte maps `/local/` inside the container to `/tmp/airbyte_local/` on your Mac. The file will appear at:
> ```
> /tmp/airbyte_local/weather_raw.duckdb
> ```
> This matches `AIRBYTE_RAW_PATH` in your `.env`.

Make sure the host directory exists:
```bash
mkdir -p /tmp/airbyte_local
```

3. Click **Set up destination**.

---

## Step 4: Create the Connection

1. Go to **Connections** > **+ New connection**
2. Select source: **MongoDB - weather_data**
3. Select destination: **DuckDB - weather warehouse**

### Configure the stream:

| Setting | Value |
|---------|-------|
| Stream | `weather_raw` |
| Sync mode | `Full Refresh \| Overwrite` |
| Cursor field | *(not needed for Full Refresh)* |
| Primary key | `_id` |

> **Note on sync modes:** The DuckDB destination does not support `Incremental | Append + Deduped`. Use `Full Refresh | Overwrite` which replaces the destination table on every sync. Incremental deduplication is handled downstream in `transform.py` via the `WHERE NOT EXISTS` clause.
>
> If you want `Incremental | Append` (supported by DuckDB), you can use that instead — but be aware that without deduplication at the Airbyte layer, `transform.py` becomes the sole deduplication point.

### Connection settings:

| Setting | Value |
|---------|-------|
| Connection name | `MongoDB_Atlas_to_DuckDB` |
| Replication frequency | `Manual` (for testing) or `Every 1 hour` |

4. Click **Set up connection**

---

## Step 5: Run the First Sync

1. Click **Sync now** on the connection page
2. Monitor progress in the **Sync History** tab
3. The first sync is a full refresh (copies all documents)
4. Subsequent syncs only pull records where `updatedAt` has changed

### Verify the sync worked:

```bash
# Check the file exists
ls -la /tmp/airbyte_local/weather_raw.duckdb

# Query it
python3 -c "
import duckdb
con = duckdb.connect('/tmp/airbyte_local/weather_raw.duckdb', read_only=True)
print(con.execute('SHOW TABLES').fetchall())
print(con.execute('SELECT COUNT(*) FROM weather_raw').fetchone())
"
```

---

## Step 6: Run the Transform

After a successful Airbyte sync:

```bash
cd /path/to/SkyLogix-Pipeline
python transform.py   # raw DuckDB -> star schema in analytics.duckdb
python analyse.py      # pandas analysis on the warehouse
```

---

## Complete Pipeline Flow

```
python mongo_loader.py     # Fetch weather API data -> MongoDB Atlas
                           # (Airbyte syncs Atlas -> DuckDB)
python transform.py        # raw DuckDB -> star schema
python analyse.py          # analytics on warehouse
```

---

## Troubleshooting

| Problem | Solution |
|---------|----------|
| "Authentication failed" | Check username/password. Ensure `authSource=admin` is in the connection string. |
| "No collections discovered" | Ensure `mongo_loader.py` has been run and `weather_raw` collection has data. |
| "Connection string invalid" | URL-encode special chars in password: `%` → `%25`, `@` → `%40`, `*` → `%2A`. |
| DuckDB file not appearing | Ensure `/tmp/airbyte_local/` exists. Check Airbyte sync logs for errors. |
| `NullPointerException` on `getGlobal()` | Airbyte 0.50.35 bug with MongoDB CDC state. Switch sync mode to `Full Refresh \| Overwrite` to avoid state management entirely. |
| `Incremental \| Append + Deduped` not available | DuckDB destination does not support this mode. Use `Full Refresh \| Overwrite` or `Incremental \| Append` instead. |
| "File is locked" in transform.py | Wait for the Airbyte sync to complete before running transform.py. |
