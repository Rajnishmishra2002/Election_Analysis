import sqlite3
import pandas as pd

DB_PATH = "elections.db"                 # SQLite DB file
# <-- put your actual CSV filename here
CSV_PATH = r"C:\Users\rajni\OneDrive\Desktop\ELECTION_ANALysis\cleaned_All_StatesGE91-19.csv"

print("Reading CSV...")
df = pd.read_csv(CSV_PATH, low_memory=False)

print("Columns in file:")
print(list(df.columns))

# OPTIONAL: if there's an unnamed index column, drop it
df = df.loc[:, ~df.columns.str.contains('^Unnamed')]

print("Columns after cleaning:")
print(list(df.columns))

# ---- At this point: we trust df.columns as the final schema ----

# Basic cleaning: numeric vs text
numeric_cols = []
text_cols = []

for col in df.columns:
    # crude rule: try numeric, if many NaNs then treat as text
    try:
        tmp = pd.to_numeric(df[col], errors='coerce')
        # if at least 50% non-null after conversion, treat as numeric
        if tmp.notna().mean() > 0.5:
            df[col] = tmp
            numeric_cols.append(col)
        else:
            df[col] = df[col].astype(str).str.strip()
            text_cols.append(col)
    except Exception:
        df[col] = df[col].astype(str).str.strip()
        text_cols.append(col)

print("Numeric columns inferred:", numeric_cols)
print("Text columns inferred:", text_cols)

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()

# Drop old table if it exists (to avoid schema mismatch)
cur.execute("DROP TABLE IF EXISTS fact_election_results;")

# Build CREATE TABLE dynamically from df.columns
cols_sql_parts = ["id INTEGER PRIMARY KEY AUTOINCREMENT"]
for col in df.columns:
    if col in numeric_cols:
        col_type = "REAL"
    else:
        col_type = "TEXT"
    # make sure column name is SQL-safe (no spaces)
    safe_name = col.strip().replace(" ", "_")
    cols_sql_parts.append(f"{safe_name} {col_type}")

create_table_sql = "CREATE TABLE fact_election_results (\n  " + ",\n  ".join(
    cols_sql_parts) + "\n);"

print("\nCREATE TABLE we will run:")
print(create_table_sql)

cur.execute(create_table_sql)
conn.commit()

# To ensure DataFrame column names match table columns, rename df to safe names too
rename_map = {col: col.strip().replace(" ", "_") for col in df.columns}
df = df.rename(columns=rename_map)

print("\nInserting data into fact_election_results...")
df.to_sql("fact_election_results", conn, if_exists="append", index=False)

conn.close()
print("Done! Data loaded into elections.db")
