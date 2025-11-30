import sqlite3
import unittest
import requests
import os
from datetime import datetime, timedelta


def generate_dates(start, end):
    start_date = datetime.strptime(start, "%Y-%m-%d").date()
    end_date = datetime.strptime(end, "%Y-%m-%d").date()
    all_dates = []
    current = start_date
    while current <= end_date:
        all_dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)
    return all_dates

def get_weather_data(long, lat, date, timezone):
    url = f"https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": long,
        "start_date": date,
        "end_date": date,
        "daily": (
            "temperature_2m_mean,"
            "wind_speed_10m_mean,"
            "cloud_cover_mean,",
            "precipitation_sum"
        ),
        "temperature_unit": "fahrenheit",
        "wind_speed_unit": "mph",
        "precipitation_unit": "inch",
        "timezone": timezone
    }
    data = requests.get(url, params=params).json()
    return data

def process_weather_data(longitude, latitude, day, timezone):
    conditions = get_weather_data(longitude, latitude, day, timezone)
    weather = {
        'date': day,
        'temp_mean': conditions['daily']['temperature_2m_mean'],
        'wind_speed': conditions['daily']['wind_speed_10m_mean'],
        'cloud_cover': conditions['daily']['cloud_cover_mean'],
        'precipitation': conditions['daily']['precipitation_sum']
    }
    return weather

def setup_db(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    return cur, conn

def make_table(cur, conn):
    cur.execute("""
            CREATE TABLE IF NOT EXISTS Weather (
                weather_id INTEGER PRIMARY KEY AUTOINCREMENT,
                date INTEGER,
                temp_mean INTEGER,
                wind_speed INTEGER NULL,
                cloud_cover INTEGER,
                precipitation INTEGER
                )    
                """)
    conn.commit()

def grab_dates(curr, conn):
    

    pass
class TestCases(unittest.TestCase):
    def setUp(self):
        pass
    def test_weather(self):
        w_data = process_weather_data(42.2808, 83.7430, '2025-09-02', "America/New_York")
        expected = {'date': '2025-09-02', 'temp_mean': [33.5], 'wind_speed': [10.8], 'cloud_cover': [100], 'precipitation': [0.039]}
        self.assertEqual(w_data, expected)
        
def main():
    dates = generate_dates("2025-09-01", "2025-11-25")
    cur, conn = setup_db("weather.db")
    make_table(cur, conn)  

    

if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)