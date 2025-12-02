import sqlite3
import unittest
import requests
import os
from datetime import datetime, timedelta


def generate_dates(start, end):
    """
    generate a list of dates depending on a date range from a starting date to an ending date
    INPUT: start (string), end (string)
    OUTPUT: all_dates (list)
    """
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return all_dates

def get_weather_data(lat, long, date, timezone):
    """
    query openmeteo api to get weather data depending on a date, location, and timezone
    INPUT: long (integer), lat (integer), date (string), timezone (string)
    OUTPUT: data (dictionary)
    """
    url = f"https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": long,
        "start_date": date,
        "end_date": date,
        "daily": (
            "temperature_2m_mean,"
            "wind_speed_10m_mean,"
            "cloud_cover_mean",
            "precipitation_sum"
        ),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": timezone
    }
    data = requests.get(url, params=params).json()
    return data

def process_weather_data(conditions):
    """
    process query response from get_weather_data 
    INPUT: conditions (dictionary)
    OUTPUT: weather (dictionary)
    """
    weather = {
        'date': conditions['daily']['time'][0],
        'temp_mean': conditions['daily']['temperature_2m_mean'][0],
        'wind_speed': conditions['daily']['wind_speed_10m_mean'][0],
        'cloud_cover': conditions['daily']['cloud_cover_mean'][0],
        'precipitation': conditions['daily']['precipitation_sum'][0]
    }
    return weather

def setup_db(db_name):
    """
    setup database connection
    INPUT: db_name (string)
    OUTPUT: None
    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def make_table(cur, conn):
    """
    create a table for weather conditions
    INPUT: cur (cursor), conn (connection)
    OUTPUT: None
    """
    cur.execute("""
            CREATE TABLE IF NOT EXISTS Weather (
                weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                temp_mean INTEGER,
                wind_speed INTEGER NULL,
                cloud_cover INTEGER,
                precipitation INTEGER
                )    
                """)
    conn.commit()

def grab_dates(curr):
    """
    grab dates from weather database to see current dates already added
    INPUT: curr (cursor)
    OUTPUT: None
    """
    curr.execute("""
        SELECT date FROM weather
    """)
    results = curr.fetchall()
    return {result[0] for result in results}


def add_data(days, curr, conn, lat, long, timezone):
    current_dates = grab_dates(curr)
    remaining = [d for d in days if d not in current_dates]
    batch = remaining[:25]
    
    for day in batch:
        raw_weather = get_weather_data(lat, long, day, timezone)
        cleaned = process_weather_data(raw_weather)
        curr.execute("""
        INSERT OR IGNORE INTO weather (date, temp_mean, wind_speed, cloud_cover, precipitation) VALUES (?, ?, ?, ?, ?)
        """, (
            cleaned['date'],
            cleaned['temp_mean'],
            cleaned['wind_speed'],
            cleaned['cloud_cover'],
            cleaned['precipitation']
        ))
    conn.commit()
    return batch

class TestCases(unittest.TestCase):
    def setUp(self):
        pass
    def test_weather(self):
        raw = get_weather_data(42.2808, -83.7430, '2025-09-02', "America/New_York")
        w_data = process_weather_data(raw)
        expected = {'date': '2025-09-02', 'temp_mean': 66.4, 'wind_speed': 3.6, 'cloud_cover': 5, 'precipitation': 0.0}
        self.assertEqual(w_data, expected)
    def test_generate_dates(self):
        expected_dates = ['2025-09-01', '2025-09-02']
        dates = generate_dates('2025-09-01', '2025-09-02')
        self.assertEqual(dates, expected_dates)
        
def main():
    dates = generate_dates("2025-09-01", "2025-11-25")
    cur, conn = setup_db("weather.db")
    make_table(cur, conn)
    added = add_data(dates, cur, conn, 42.2808, -83.7430, "America/New_York")
    print(f"added {len(added)} rows")
    conn.close()

if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)