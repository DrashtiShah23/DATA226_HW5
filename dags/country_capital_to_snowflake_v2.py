from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests

# constants 

SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_conn")
DATABASE          = Variable.get("SNOWFLAKE_DATABASE", default_var="USER_DB_LEMMING")
SCHEMA            = Variable.get("SNOWFLAKE_SCHEMA",   default_var="RAW")
SYMBOL            = Variable.get("STOCK_SYMBOL",       default_var="GOOG")

TARGET_TABLE = Variable.get("SNOWFLAKE_TARGET_TABLE", default_var=None) \
    or f"{DATABASE}.{SCHEMA}.STOCK_PRICES"

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID, database=DATABASE)
    conn = hook.get_conn()
    return conn.cursor()

@task(task_id="extract_data")
def extract_data():
    api_key = Variable.get("VANTAGE_API_KEY", default_var=None)
    if not api_key:
        raise ValueError("Airflow Variable 'VANTAGE_API_KEY' is missing (Admin → Variables).")

    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={SYMBOL}&apikey={api_key}"
    response = requests.get(url, timeout=60)    
    response.raise_for_status()  # raise for HTTP errors
    data = response.json().get("Time Series (Daily)", {})   # dict keyed by date strings → metric dicts
    return list(data.items())  

@task(task_id="transform_data")
def transform_data(raw: list[tuple[str, dict]], symbol: str, days: int = 90) -> list[tuple]:
    if not raw:                                                                
        raise ValueError("extract_data returned 0 rows — likely rate limit or wrong key/symbol.")       

    latest = max(datetime.strptime(ds, "%Y-%m-%d").date() for ds, _ in raw)
    cutoff = latest - timedelta(days=days)

    out = []
    for date_str, vals in raw:
        #  filter to last 'days' worth of rows (relative to latest date present)
        d = datetime.strptime(date_str, "%Y-%m-%d").date()
        if d >= cutoff:
            # Field names for TIME_SERIES_DAILY are "1. open", "2. high", "3. low", "4. close", "5. volume"
            out.append((
                symbol,
                date_str,
                float(vals["1. open"]),
                float(vals["4. close"]),
                float(vals["2. high"]),
                float(vals["3. low"]),
                int(vals["5. volume"]),
            ))

    #  sort ascending by date
    out.sort(key=lambda r: r[1])
    if not out:
        raise ValueError(f"No trading days remain in last {days} days. "
                         f"Feed range was {min(ds for ds,_ in raw)}..{max(ds for ds,_ in raw)}")
    print(f"[transform_data] kept {len(out)} rows, from {out[0][1]} to {out[-1][1]}")
    return out



@task(task_id="load_data")
def full_refresh_load_data(records: list[tuple]):
    if not records:
        raise ValueError("No records to load.")
    symbol = records[0][0]

    cur = get_snowflake_cursor()
    try:
        # Ensure table exists (DDL auto-commits; keep outside the transaction)
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
                symbol STRING NOT NULL,
                date   DATE   NOT NULL,
                open   FLOAT,
                close  FLOAT,
                high   FLOAT,
                low    FLOAT,
                volume INTEGER,
                CONSTRAINT PK_STOCK PRIMARY KEY(symbol, date)
            )
        """)

        # Transactional full refresh for this symbol
        cur.execute("BEGIN")
        cur.execute(f"DELETE FROM {TARGET_TABLE} WHERE symbol=%s", (symbol,))
        cur.executemany(
            f"INSERT INTO {TARGET_TABLE} (symbol, date, open, close, high, low, volume) "
            f"VALUES (%s, %s, %s, %s, %s, %s, %s)",
            records
        )
        cur.execute("COMMIT")
        print(f"Loaded {len(records)} rows for {symbol} into {TARGET_TABLE}")
    except Exception as e:
        cur.execute("ROLLBACK")
        raise RuntimeError(f"Load failed: {e}")
    finally:
        cur.close()

#  DAG definition
with DAG(
    dag_id="AlphaVantage_Stock_to_Snowflake", # unique identifier for the DAG
    start_date=datetime(2025, 10, 2), # backfill from this date if catchup=True
    schedule="30 2 * * *",
    catchup=False,
    tags=["HW5", "AlphaVantage", "Snowflake"],
):
    raw = extract_data()      # extract
    shaped = transform_data(raw, SYMBOL)  # transform
    full_refresh_load_data(shaped)  # load
