import sqlite3
import unittest
import requests
import os

def get_api_key(filename):
    '''
    loads in API key from file 

    ARGUMENTS:  
        file: file that contains your API key
    
    RETURNS:
        your API key
    '''
    base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, filename)
    with open(full_path) as f:
        api_key = f.read()
    return api_key

API_KEY = get_api_key('apikey.txt')

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


class TestCases(unittest.TestCase):
    def setUp(self):
        self.weather_key = get_api_key('apikey.txt')

    def test_get_api_key(self):                     
        hidden_key = get_api_key('apikey.txt')
        self.assertEqual(API_KEY, hidden_key)
    
    def test_weather(self):
        w_data = process_weather_data(42.2808, 83.7430, '2025-09-02', "America/New_York")
        expected = {'date': '2025-09-02', 'temp_mean': [33.5], 'wind_speed': [10.8], 'cloud_cover': [100], 'precipitation': [0.039]}
        self.assertEqual(w_data, expected)
        
def main():
    w_data = process_weather_data(42.2808, 83.7430, '2025-09-02', "America/New_York")
    print(w_data)
    

if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)