import asyncio
import json
import sqlite3
from datetime import datetime

import aiohttp
from shapely.geometry import Polygon, Point

# Define North America polygon with coordinates
north_america_polygon = Polygon([
    (-167.112472, 66.142750), (-141.338990, 64.849412), (-133.751810, 61.959240),
    (-124.034403, 59.923587), (-98.886950, 60.024861), (-89.134663, 54.191839),
    (-78.002418, 52.392400), (-68.923741, 52.262380), (-64.749177, 60.317732),
    (-55.453238, 52.251482), (-50.877762, 46.855167), (-65.601410, 43.080521),
    (-70.294206, 40.142875), (-77.333400, 32.299939), (-77.333400, 25.157298),
    (-82.084856, 23.983664), (-85.545793, 27.989875), (-92.878287, 28.558147),
    (-95.869945, 26.109243), (-99.624181, 26.948968), (-117.750106, 31.702996),
    (-126.197139, 37.585581), (-124.789300, 46.209501), (-133.646953, 53.315787),
    (-139.630268, 58.501270), (-146.024202, 59.557690), (-160.571870, 57.412078),
    (-170.426742, 61.602696)
])

# Set grid box dimensions and ranges
lat_change = 0.5505419906547857
long_change = 0.5174560546875
urls = []
lat = 66  # Starting from the north-most latitude

# Generate URLs
while lat >= 23:  # Move south until the southern boundary
    long = -167  # Start from the west-most longitude
    row_started = False

    while long <= -50:  # Move east until the eastern boundary
        top_left_point = Point(long, lat)

        if north_america_polygon.contains(top_left_point):
            row_started = True
            top = lat
            bottom = lat - lat_change
            left = long
            right = long + long_change

            url = f"https://www.waze.com/live-map/api/georss?top={top}&bottom={bottom}&left={left}&right={right}&env=na&types=alerts,traffic"
            urls.append(url)

        elif row_started:
            break

        long += long_change
    lat -= lat_change

print(f"Total URLs to process: {len(urls)}")


# Asynchronous function to fetch data from URL and collect police alerts
async def fetch_alerts(session, url):
    try:
        async with session.get(url) as response:
            if response.status == 200:
                json_data = await response.text()
                data = json.loads(json_data)

                if "alerts" in data and data["alerts"]:
                    police_alerts = [alert for alert in data["alerts"] if alert.get("type") == "POLICE"]
                    print(f"Fetched {len(police_alerts)} police alerts from URL: {url}")
                    return police_alerts
                else:
                    print(f"No alerts found for URL: {url}")
                    return []
            elif response.status == 429:
                print(f"Rate limit hit for URL: {url}. Waiting before retrying.")
                await asyncio.sleep(4)  # Wait before retrying
                return await fetch_alerts(session, url)
            else:
                print(f"Failed to retrieve data for URL: {url}, Status Code: {response.status}")
                return []
    except Exception as e:
        print(f"Error fetching data from URL {url}: {e}")
        return []


# Main asynchronous function to handle all requests with delay between each one
async def main():
    # Database setup inside main function
    conn = sqlite3.connect("police_alerts.db")
    cursor = conn.cursor()

    # Ensure tables exist
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
        pubMillis INTEGER
    )
    ''')
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS import_log (
        session_id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        alert_count INTEGER
    )
    ''')
    conn.commit()

    # Function to insert police alerts in batch
    def insert_police_alerts_batch(alerts):
        if alerts:
            cursor.executemany('''
                INSERT OR IGNORE INTO alerts (
                    uuid, country, inscale, city, reportRating, reportByMunicipalityUser,
                    confidence, reliability, type, speed, reportMood, roadType, magvar,
                    street, additionalInfo, location_x, location_y, pubMillis
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', [
                (
                    alert["uuid"], alert.get("country"), alert.get("inscale"), alert.get("city"),
                    alert.get("reportRating"),
                    alert.get("reportByMunicipalityUser") == "true", alert.get("confidence"), alert.get("reliability"),
                    alert.get("type"), alert.get("speed"), alert.get("reportMood"), alert.get("roadType"),
                    alert.get("magvar"),
                    alert.get("street"), alert.get("additionalInfo"), alert["location"]["x"], alert["location"]["y"],
                    alert.get("pubMillis")
                ) for alert in alerts
            ])
            conn.commit()

    # Log the session with timestamp and alert count
    def log_import_session(alert_count):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        cursor.execute('''
            INSERT INTO import_log (timestamp, alert_count) VALUES (?, ?)
        ''', (timestamp, alert_count))
        conn.commit()

    async with aiohttp.ClientSession() as session:
        print(f"Total URLs to process: {len(urls)}")

        all_police_alerts = []
        for url in urls:
            alerts = await fetch_alerts(session, url)
            all_police_alerts.extend(alerts)
            await asyncio.sleep(0.3)  # Delay between requests

        # Insert the collected alerts in a batch
        insert_police_alerts_batch(all_police_alerts)

        # Log the import session
        log_import_session(len(all_police_alerts))
        print(f"Inserted {len(all_police_alerts)} new police alerts.")

    conn.close()  # Close the database connection after all operations complete


while True:
    asyncio.run(main())
