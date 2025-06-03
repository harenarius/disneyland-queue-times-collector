import os
import sqlite3
import sys
from datetime import date, datetime

import holidays
import requests
from dotenv import load_dotenv

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
DB_NAME = "queue_times.db"
PARKS = {"disneyland": 16, "california_adventure": 17}
WEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"
LOCATION = {"lat": 33.8121, "lon": -117.9190}  # Anaheim, CA

# Load .env variables safely
load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")
if not API_KEY:
    print("[WARNING] OPENWEATHER_API_KEY not found in environment. Weather data will fail.")

# ------------------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------------------
us_holidays = holidays.US()
today = date.today()
is_today_holiday = today in us_holidays
holiday_name = us_holidays.get(today) if is_today_holiday else None


def fetch_ride_data(park_name, park_id):
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"
    try:
        response = requests.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
    except Exception as e:
        print(f"[ERROR] Could not fetch ride data for {park_name}: {e}")
        return []

    timestamp = datetime.now().isoformat()
    output = []
    for land in data.get("lands", []):
        for ride in land["rides"]:
            output.append(
                (
                    timestamp,
                    park_name,
                    land["name"],
                    ride["name"],
                    ride["wait_time"],
                    int(is_today_holiday),
                )
            )
    return output


def fetch_weather_data():
    if not API_KEY:
        return None

    params = {
        "lat": LOCATION["lat"],
        "lon": LOCATION["lon"],
        "appid": API_KEY,
        "units": "imperial",
    }
    try:
        response = requests.get(WEATHER_URL, params=params, timeout=15)
        response.raise_for_status()
        data = response.json()
        return (
            datetime.now().isoformat(),
            data["weather"][0]["main"],
            data["main"]["temp"],
            data["main"]["humidity"],
            data["wind"]["speed"],
        )
    except Exception as e:
        print(f"[ERROR] Could not fetch weather data: {e}")
        return None


# ------------------------------------------------------------------------------
# Database Operations
# ------------------------------------------------------------------------------
def create_tables(conn):
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS queue_times (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            park TEXT,
            land TEXT,
            ride TEXT,
            wait_time INTEGER,
            is_holiday INTEGER
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS weather (
            timestamp TEXT PRIMARY KEY,
            condition TEXT,
            temperature REAL,
            humidity INTEGER,
            wind_speed REAL
        )
    """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS holidays (
            date TEXT PRIMARY KEY,
            name TEXT
        )
    """
    )
    conn.commit()


def insert_ride_data(conn, ride_data):
    if ride_data:
        conn.executemany(
            """
            INSERT INTO queue_times (timestamp, park, land, ride, wait_time, is_holiday)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            ride_data,
        )


def insert_weather_data(conn, weather_row):
    if weather_row:
        conn.execute(
            """
            INSERT OR IGNORE INTO weather
            (timestamp, condition, temperature, humidity, wind_speed)
            VALUES (?, ?, ?, ?, ?)
        """,
            weather_row,
        )


def insert_holiday(conn):
    if is_today_holiday:
        conn.execute(
            """
            INSERT OR IGNORE INTO holidays (date, name)
            VALUES (?, ?)
        """,
            (today.isoformat(), holiday_name),
        )


# ------------------------------------------------------------------------------
# Main
# ------------------------------------------------------------------------------
def main():
    try:
        conn = sqlite3.connect(DB_NAME)
        create_tables(conn)

        insert_holiday(conn)

        all_ride_data = []
        for park_name, park_id in PARKS.items():
            park_data = fetch_ride_data(park_name, park_id)
            all_ride_data.extend(park_data)
        insert_ride_data(conn, all_ride_data)

        weather = fetch_weather_data()
        insert_weather_data(conn, weather)

        conn.commit()

        print(f"[INFO] {len(all_ride_data)} ride rows inserted.")
        if weather:
            print("[INFO] 1 weather row inserted.")
        if is_today_holiday:
            print(f"[INFO] Holiday recorded: {holiday_name}")
        print(f"[INFO] Data written to {DB_NAME}")

    except Exception as e:
        print(f"[CRITICAL] Script failed: {e}")
        sys.exit(1)
    finally:
        try:
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
