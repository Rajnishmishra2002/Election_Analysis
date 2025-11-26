from fastapi import FastAPI
import sqlite3
from typing import Optional

app = FastAPI()
DB_PATH = r"C:\Users\rajni\OneDrive\Desktop\ELECTION_ANALysis\elections.db"


def run_query(query, params=()):
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute(query, params)
    rows = cur.fetchall()
    conn.close()
    return [dict(row) for row in rows]


@app.get("/")
def home():
    return {"message": "Election Data API Running!"}


@app.get("/results")
def get_results(
    year: Optional[int] = None,
    state: Optional[str] = None,
    party: Optional[str] = None,
    gender: Optional[str] = None,
    constituency: Optional[str] = None
):
    query = "SELECT * FROM fact_election_results WHERE 1=1"
    params = []

    if year:
        query += " AND Year = ?"
        params.append(year)

    if state:
        query += " AND State_Name = ?"
        params.append(state)

    if party:
        query += " AND Party = ?"
        params.append(party)

    if gender:
        query += " AND Sex = ?"
        params.append(gender)

    if constituency:
        query += " AND Constituency_Name = ?"
        params.append(constituency)

    return run_query(query, params)
