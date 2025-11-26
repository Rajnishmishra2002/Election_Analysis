import sqlite3
import pandas as pd
import numpy as np
import os

# ============
# 1. LOAD DATA
# ============
DB_PATH = "elections.db"   # change path if needed
OUTPUT_DIR = "powerbi_csv"  # folder where all CSVs will be saved

# Create output folder
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Connect to SQLite
conn = sqlite3.connect(DB_PATH)

# Read main table
df = pd.read_sql("SELECT * FROM fact_election_results;", conn)

conn.close()

print("Data loaded. Rows:", len(df))

# ==========================
# 2. FIX DATA TYPES
# ==========================
numeric_cols = [
    "Year", "Assembly_No", "Constituency_No", "month", "Position", "Votes",
    "Valid_Votes", "Electors", "No_Cand", "Turnout_Percentage",
    "Vote_Share_Percentage", "Margin", "Margin_Percentage", "last_poll",
    "Contested", "No_Terms", "Vote_Share_check"
]

for col in numeric_cols:
    if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

df["Winner"] = (df["Position"] == 1).astype(int)

# ====================================
# 3. GENERATE FINAL POWER BI DATASETS
# ====================================

# --- Dataset 1: Party-wise Seats (per year) ---
party_seats = (
    df[df["Winner"] == 1]
    .groupby(["Year", "Party"])
    .size()
    .reset_index(name="Seats_Won")
)
party_seats.to_csv(f"{OUTPUT_DIR}/party_seats.csv", index=False)

# --- Dataset 2: State-wise Turnout ---
state_turnout = (
    df.groupby(["Year", "State_Name"])["Turnout_Percentage"]
    .mean()
    .reset_index()
)
state_turnout.to_csv(f"{OUTPUT_DIR}/state_turnout.csv", index=False)

# --- Dataset 3: Gender Representation ---
gender_year = (
    df.groupby(["Year", "Sex"])
    .size()
    .reset_index(name="Candidate_Count")
)
gender_year.to_csv(f"{OUTPUT_DIR}/gender_year.csv", index=False)

# --- Dataset 4: Party Vote Share ---
party_votes = (
    df.groupby(["Year", "Party"])["Votes"]
    .sum()
    .reset_index()
)

# FIXED: use transform instead of apply
party_votes["Vote_Share_Percentage"] = (
    party_votes["Votes"] /
    party_votes.groupby("Year")["Votes"].transform("sum") * 100
)

party_votes.to_csv(f"{OUTPUT_DIR}/party_votes.csv", index=False)

# --- Dataset 5: Margin Histogram Bins ---
df["Margin_Bin"] = pd.cut(df["Margin"], bins=10)

margin_bins = (
    df[df["Winner"] == 1]
    .groupby("Margin_Bin")
    .size()
    .reset_index(name="Count")
)
margin_bins.to_csv(f"{OUTPUT_DIR}/margin_bins.csv", index=False)

# --- Dataset 6: Search Table ---
search_table = df[[
    "Year", "State_Name", "Constituency_Name", "Candidate", "Party",
    "Votes", "Margin", "Margin_Percentage", "Winner"
]]
search_table.to_csv(f"{OUTPUT_DIR}/search_table.csv", index=False)

# --- Dataset 7: National vs Regional Vote Share ---
party_type = (
    df.groupby(["Year", "Party_Type_TCPD"])["Votes"]
    .sum()
    .reset_index()
)

party_type["Vote_Share_Percentage"] = (
    party_type["Votes"] /
    party_type.groupby("Year")["Votes"].transform("sum") * 100
)

party_type.to_csv(f"{OUTPUT_DIR}/party_type_vote_share.csv", index=False)

# --- Dataset 8: Education vs Win Rate ---
edu = (
    df.groupby("MyNeta_education")
    .agg(
        Total_Candidates=("Candidate", "count"),
        Wins=("Winner", "sum")
    )
    .reset_index()
)

edu["Win_Rate"] = edu["Wins"] / edu["Total_Candidates"]

edu.to_csv(f"{OUTPUT_DIR}/education_win_rate.csv", index=False)

print("\n🎉 ALL CSV FILES CREATED SUCCESSFULLY!")
print(f"📁 Saved to folder: {OUTPUT_DIR}/")
