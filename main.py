import json
import sqlite3
import time
import asyncio
import aiohttp
from random import randint
from aiohttp import ClientSession
from datetime import datetime

# Database setup
conn = sqlite3.connect("police_alerts.db")
cursor = conn.cursor()

# Modified schema to include a timestamp column
cursor.execute('''
CREATE TABLE IF NOT EXISTS alerts (
    uuid TEXT PRIMARY KEY,
    country TEXT,
    inscale BOOLEAN,
    city TEXT,
    reportRating INTEGER,
    reportByMunicipalityUser BOOLEAN,
    confidence INTEGER,
    reliability INTEGER,
    type TEXT,
    speed INTEGER,
    reportMood INTEGER,
    roadType INTEGER,
    magvar INTEGER,
    street TEXT,
    additionalInfo TEXT,
    location_x REAL,
    location_y REAL,
    pubMillis INTEGER,
    timestamp TEXT
)
''')
conn.commit()


# Function to insert police alerts in batch with timestamp
def insert_police_alerts_batch(alerts):
    if alerts:
        timestamp = datetime.utcnow().isoformat()
        count = len(alerts)

        cursor.executemany('''
            INSERT OR IGNORE INTO alerts (
                uuid, country, inscale, city, reportRating, reportByMunicipalityUser,
                confidence, reliability, type, speed, reportMood, roadType, magvar,
                street, additionalInfo, location_x, location_y, pubMillis, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', [
            (
                alert["uuid"], alert["country"], alert["inscale"], alert["city"], alert["reportRating"],
                alert["reportByMunicipalityUser"] == "true", alert["confidence"], alert["reliability"],
                alert["type"], alert["speed"], alert["reportMood"], alert["roadType"], alert["magvar"],
                alert["street"], alert["additionalInfo"], alert["location"]["x"], alert["location"]["y"],
                alert["pubMillis"], timestamp
            ) for alert in alerts
        ])
        conn.commit()
        print(f"Inserted {count} new police alerts with timestamp {timestamp}.")


# Set grid box dimensions and ranges
lat_change = 0.5505419906547857
long_change = 0.5174560546875
urls = []
lat = 23
while lat <= 51:
    long = -127
    while long <= -62:
        top = lat
        bottom = lat - lat_change
        left = long
        right = long + long_change

        url = f"https://www.waze.com/live-map/api/georss?top={top}&bottom={bottom}&left={left}&right={right}&env=na&types=alerts,traffic"
        urls.append(url)
        long += long_change
    lat += lat_change

print(f"Total URLs to process: {len(urls)}")


# Asynchronous function to fetch data from URL and collect police alerts
async def fetch_alerts(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                json_data = await response.text()
                data = json.loads(json_data)

                # Check if 'alerts' key exists and contains data
                if "alerts" in data and data["alerts"]:
                    # Extract police alerts specifically
                    police_alerts = [alert for alert in data["alerts"] if alert.get("type") == "POLICE"]
                    print(f"Fetched {len(police_alerts)} police alerts from URL: {url}")
                    return police_alerts
                else:
                    print(f"No alerts found for URL: {url}")
                    return []
            elif response.status == 429:
                print(f"Rate limit hit for URL: {url}. Waiting before retrying.")
                await asyncio.sleep(4)  # Wait 2 seconds before retrying
                return await fetch_alerts(session, url)  # Retry the request
            else:
                print(f"Failed to retrieve data for URL: {url}, Status Code: {response.status}")
                return []
    except Exception as e:
        print(f"Error fetching data from URL {url}: {e}")
        return []


# Main asynchronous function to handle all requests with delay between each one
async def main():
    async with aiohttp.ClientSession() as session:
        print(f"Total URLs to process: {len(urls)}")

        # Sequentially process each URL with a delay
        all_police_alerts = []
        for url in urls:
            alerts = await fetch_alerts(session, url)
            all_police_alerts.extend(alerts)
            await asyncio.sleep(0.3)  # Delay of 0.3 seconds between each request

        # Insert the collected alerts in a batch with timestamp
        insert_police_alerts_batch(all_police_alerts)
        print(f"Inserted {len(all_police_alerts)} new police alerts.")


# Run the asynchronous main function
try:
    asyncio.run(main())
except RuntimeError:
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
