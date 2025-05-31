import requests
import sqlite3
from datetime import datetime
import os
from dotenv import load_dotenv

# Load .env variables
load_dotenv()

# Settings
PARKS = {
    "disneyland": 16,
    "california_adventure": 17
}
DB_NAME = "queue_times.db"
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
API_KEY = os.getenv("OPENWEATHER_API_KEY")
LOCATION = {"lat": 33.8121, "lon": -117.9190}  # Anaheim, CA

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

def fetch_weather_data():
    params = {
        "lat": LOCATION["lat"],
        "lon": LOCATION["lon"],
        "appid": API_KEY,
        "units": "imperial"
    }
    response = requests.get(WEATHER_URL, params=params)
    data = response.json()
    return (
        datetime.now().isoformat(),
        data["weather"][0]["main"],
        data["main"]["temp"],
        data["main"]["humidity"],
        data["wind"]["speed"]
    )

def create_tables(conn):
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
    conn.execute("""
        CREATE TABLE IF NOT EXISTS weather (
            timestamp TEXT PRIMARY KEY,
            condition TEXT,
            temperature REAL,
            humidity INTEGER,
            wind_speed REAL
        )
    """)
    conn.commit()

def insert_ride_data(conn, ride_data):
    conn.executemany("""
        INSERT INTO queue_times (timestamp, park, land, ride, wait_time)
        VALUES (?, ?, ?, ?, ?)
    """, ride_data)

def insert_weather_data(conn, weather_row):
    conn.execute("""
        INSERT OR IGNORE INTO weather (timestamp, condition, temperature, humidity, wind_speed)
        VALUES (?, ?, ?, ?, ?)
    """, weather_row)

def main():
    conn = sqlite3.connect(DB_NAME)
    create_tables(conn)

    # Pull and insert ride data
    all_ride_data = []
    for park_name, park_id in PARKS.items():
        park_data = fetch_ride_data(park_name, park_id)
        all_ride_data.extend(park_data)
    insert_ride_data(conn, all_ride_data)

    # Pull and insert weather data
    weather = fetch_weather_data()
    insert_weather_data(conn, weather)

    conn.commit()
    conn.close()

    print(f"Inserted {len(all_ride_data)} ride rows and 1 weather row into {DB_NAME}")

if __name__ == "__main__":
    main()
