import sqlite3
import unittest
import requests
import os
from datetime import datetime, timedelta

#ann arbor coordinates to query weather api
ANN_ARBOR = (42.2808, -83.7430)

#eastern timezone name to query weather api 
TIMEZONE = "America/New_York"

#Set the limit to how many items can be added to the database
BATCH_SIZE = 25

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
        current += timedelta(days=7)
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
    resp = requests.get(url, params=params)
    data = resp.json()
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
                date_id INTEGER,
                temp_mean INTEGER,
                wind_speed INTEGER NULL,
                cloud_cover INTEGER,
                precipitation INTEGER,
                FOREIGN KEY(date_id) REFERENCES dates(id)
                )    
                """)
    conn.commit()


def create_dates_table(cur, conn):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS dates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            day TEXT UNIQUE
        )
    ''')
    conn.commit()

def insert_dates(cur, conn, days):
    for day in days:
        cur.execute("""INSERT OR IGNORE INTO dates (day) VALUES (?)""", (day,))
    conn.commit()

def get_date_id(cur, date):
    cur.execute("SELECT id FROM dates WHERE day = ?", (date,))
    result = cur.fetchone()
    if result is None:
        return None
    return result[0]


def grab_dates(curr):
    """
    grab dates from weather table to see current dates already added
    INPUT: curr (cursor)
    OUTPUT: None
    """
    curr.execute("""
        SELECT dates.day FROM dates JOIN Weather ON Weather.date_id = dates.id WHERE Weather.date_id IS NOT NULL
    """)
    results = curr.fetchall()
    return {result[0] for result in results}


def add_data(days, curr, conn, lat, long, timezone):
    current_dates = grab_dates(curr)
    remaining = [d for d in days if d not in current_dates]
    batch = remaining[:BATCH_SIZE]
    
    for day in batch:
        raw_weather = get_weather_data(lat, long, day, timezone)
        cleaned = process_weather_data(raw_weather)
        date_id = get_date_id(curr, day)
        curr.execute("""
        INSERT OR IGNORE INTO weather (date_id, temp_mean, wind_speed, cloud_cover, precipitation) VALUES (?, ?, ?, ?, ?)
        """, (
            date_id,
            cleaned['temp_mean'],
            cleaned['wind_speed'],
            cleaned['cloud_cover'],
            cleaned['precipitation']
        ))
        
    conn.commit()
    return batch

class TestCases(unittest.TestCase):
    def test_weather(self):
        raw = get_weather_data(42.2808, -83.7430, '2025-09-02', "America/New_York")
        w_data = process_weather_data(raw)
        expected = {'date': '2025-09-02', 'temp_mean': 66.4, 'wind_speed': 3.6, 'cloud_cover': 5, 'precipitation': 0.0}
        self.assertEqual(w_data, expected)
    def test_generate_dates(self):
        expected_dates = ['2025-09-01']
        dates = generate_dates('2025-09-01', '2025-09-02')
        self.assertEqual(dates, expected_dates)
        
def main():
    all_days = []
    dates = [('2023-09-02', '2023-12-03'), ('2024-01-01', '2024-01-09'), ('2022-01-01', '2022-12-31'), ('2021-01-01', '2021-12-05'), 
             ('2020-01-01', '2020-11-28'), ('2019-08-31', '2019-11-30'), ('2018-09-01', '2018-09-01'), ('2018-01-01', '2018-12-29'), ('2017-09-02', '2017-11-25'),
             ('2016-09-03', '2016-12-31')]
    for start, end in dates:
        all_days.extend(generate_dates(start, end))
    
    cur, conn = setup_db("temp.db")
    make_table(cur, conn)
    create_dates_table(cur, conn)
    insert_dates(cur, conn, all_days)

    added = add_data(all_days, cur, conn, ANN_ARBOR[0], ANN_ARBOR[1], TIMEZONE)
    print(f"added {len(added)} rows")
    conn.close()

if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)