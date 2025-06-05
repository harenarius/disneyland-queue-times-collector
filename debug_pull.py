import requests
import os
from dotenv import load_dotenv

# Load API key if needed
load_dotenv()
API_KEY = os.getenv("QUEUE_TIMES_API_KEY")

# Park ID mapping (adjust if needed)
PARKS = {
    "disneyland": 16,
    "california_adventure": 17,
}

def fetch_rides(park_id):
    url = f"https://queue-times.com/en-US/parks/{park_id}/queue_times.json"
    headers = {"User-Agent": "Mozilla/5.0"}  # Optional but often helps

    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise RuntimeError(f"Failed to fetch data: {response.status_code}")

    return response.json()

def debug_print(park="disneyland"):
    data = fetch_rides(PARKS[park])
    lands = data.get("lands", [])
    
    for land in lands:
        for ride in land.get("rides", []):
            name = ride.get("name")
            wait = ride.get("wait_time")
            status = ride.get("status")
            print(f"{name:<50} | Wait: {wait:>3} min | Status: {status}")

if __name__ == "__main__":
    debug_print("disneyland")  # or "california_adventure"
