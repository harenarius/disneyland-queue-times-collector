import requests
import csv
import os
from datetime import datetime

PARKS = {
    "disneyland": 16,
    "california_adventure": 67
}

def fetch_ride_data(park_name, park_id):
    url = f"https://queue-times.com/parks/{park_id}/queue_times.json"
    response = requests.get(url)
    data = response.json()
    
    output = []
    timestamp = datetime.now().isoformat()

    for land in data["lands"]:
        for ride in land["rides"]:
            output.append({
                "timestamp": timestamp,
                "park": park_name,
                "land": land["name"],
                "ride": ride["name"],
                "wait_time": ride["wait_time"]
            })
    return output

def save_to_csv(data):
    filename = "queue_times_master.csv"
    file_exists = os.path.isfile(filename)

    with open(filename, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        if not file_exists:
            writer.writeheader()
        writer.writerows(data)

def main():
    all_data = []
    for park_name, park_id in PARKS.items():
        all_data.extend(fetch_ride_data(park_name, park_id))
    save_to_csv(all_data)

if __name__ == "__main__":
    main()
