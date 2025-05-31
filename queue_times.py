import requests
import sqlite3
from datetime import datetime

PARKS = {
    "disneyland": 16,
    "california_adventure": 17
}

DB_NAME = "queue_times.db"

def fetch_ride_data(park_name, park_id):
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"
    response = requests.get(url)
    data = response.json()

    timestamp = datetime.now().isoformat()
    output = []

    for land in data.get("lands", []):
        for ride in land["rides"]:
            output.append((
                timestamp,
                park_name,
                land["name"],
                ride["name"],
                ride["wait_time"]
            ))
    return output

def create_table(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS queue_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            park TEXT,
            land TEXT,
            ride TEXT,
            wait_time INTEGER
        )
    """)
    conn.commit()

def insert_data(conn, ride_data):
    conn.executemany("""
        INSERT INTO queue_times (timestamp, park, land, ride, wait_time)
        VALUES (?, ?, ?, ?, ?)
    """, ride_data)
    conn.commit()

def main():
    all_data = []
    for park_name, park_id in PARKS.items():
        print(f"Fetching data for {park_name}")
        park_data = fetch_ride_data(park_name, park_id)
        all_data.extend(park_data)

    if all_data:
        conn = sqlite3.connect(DB_NAME)
        create_table(conn)
        insert_data(conn, all_data)
        conn.close()
        print(f"Inserted {len(all_data)} rows into {DB_NAME}")
    else:
        print("No data collected.")

if __name__ == "__main__":
    main()
