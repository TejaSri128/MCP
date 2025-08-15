import os
import sys
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

# Load environment variables
load_dotenv()
print("[INFO] Loading environment variables...", file=sys.stderr)

# Required environment variables for Snowflake and CSV
REQUIRED_ENV = [
    "CSV_PATH", "SNOWFLAKE_USER", "SNOWFLAKE_PASSWORD", "SNOWFLAKE_ACCOUNT",
    "SNOWFLAKE_WAREHOUSE", "SNOWFLAKE_DATABASE", "SNOWFLAKE_SCHEMA", "SNOWFLAKE_TABLE"
]

# Validate environment variables
for var in REQUIRED_ENV:
    if not os.getenv(var):
        print(f"[ERROR] Missing environment variable: {var}", file=sys.stderr)
        sys.exit(1)

# Check CSV file existence
csv_path = os.getenv("CSV_PATH")
if not os.path.exists(csv_path):
    print(f"[ERROR] CSV file not found: {csv_path}", file=sys.stderr)
    sys.exit(1)
print(f"[INFO] CSV file found: {csv_path}", file=sys.stderr)

# Initialize MCP server
try:
    mcp = FastMCP("snowflake-tools")
    print("[INFO] MCP server initialized", file=sys.stderr)
except Exception as e:
    print(f"[ERROR] Failed to initialize MCP server: {e}", file=sys.stderr)
    sys.exit(1)

@mcp.tool()
def upload_csv_to_snowflake() -> dict:
    """
    Uploads a CSV file to a Snowflake table, normalizing column names to uppercase.
    Returns a dictionary with upload status and row counts.
    """
    print("[INFO] Running upload_csv_to_snowflake tool...", file=sys.stderr)

    # Load environment variables
    csv_path = os.getenv("CSV_PATH")
    sf_config = {key: os.getenv(key) for key in REQUIRED_ENV[1:]}

    # Read CSV file
    try:
        df = pd.read_csv(csv_path)
        print(f"[INFO] Loaded CSV: {len(df)} rows, {len(df.columns)} columns", file=sys.stderr)
        df.columns = [col.upper() for col in df.columns]  # Normalize to uppercase
    except Exception as e:
        print(f"[ERROR] Failed to read CSV: {e}", file=sys.stderr)
        return {"status": "error", "message": f"Error reading CSV: {e}"}

    # Connect to Snowflake
    try:
        conn = snowflake.connector.connect(**sf_config)
        print("[INFO] Connected to Snowflake", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Snowflake connection failed: {e}", file=sys.stderr)
        return {"status": "error", "message": f"Connection failed: {e}"}

    # Upload data to Snowflake
    try:
        success, _, nrows, _ = write_pandas(conn, df, sf_config["SNOWFLAKE_TABLE"])
        if not success:
            print("[ERROR] Failed to upload data to Snowflake", file=sys.stderr)
            return {"status": "error", "message": "Upload to Snowflake failed"}

        # Verify row count
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) FROM {sf_config['SNOWFLAKE_TABLE']}")
        row_count = cur.fetchone()[0]
        cur.close()
        conn.close()
        print(f"[INFO] Uploaded {nrows} rows, total rows: {row_count}", file=sys.stderr)
        return {"status": "success", "rows_uploaded": nrows, "total_rows_in_table": row_count}
    except Exception as e:
        print(f"[ERROR] Upload error: {e}", file=sys.stderr)
        return {"status": "error", "message": f"Upload error: {e}"}

if __name__ == "__main__":
    print("[INFO] Starting MCP server...", file=sys.stderr)
    try:
        mcp.run()
        print("[INFO] MCP server running", file=sys.stderr)
    except KeyboardInterrupt:
        print("[INFO] Server stopped by user", file=sys.stderr)
    except Exception as e:
        print(f"[ERROR] Server failed: {e}", file=sys.stderr)
        sys.exit(1)