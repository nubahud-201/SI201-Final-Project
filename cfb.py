import os
import requests
import sqlite3
import unittest
from weather import create_dates_table, insert_dates, get_date_id, grab_dates


def get_api_key(filename):
    '''
    Loads the College Football API key from a file.

    ARGUMENTS:
        filename: the file that contains your API key

    RETURNS:
        the API key as a string
    '''

    base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, filename)
    with open(full_path) as f:
        return f.read().strip()



API_KEY = get_api_key('cfb_key.txt')


def get_cfb_data(team, year):
    '''
    Makes a request to the College Football API to get game data 
    for a specific team and season.

    ARGUMENTS:
        team: name of the team (string)
        year: season year (int)

    RETURNS:
        JSON data from the API as a Python dict
    '''
    url = "https://api.collegefootballdata.com/games"
    headers = {"Authorization": f"Bearer {API_KEY}"}
    params = {"year": year, "team": team}

    response = requests.get(url, headers=headers, params=params)
    print(response)
    return response.json()



def process_cfb_data(raw_data, TEAM="Michigan"):
    games = []

    for g in raw_data:
        # Clean date
        date_raw = g.get("startDate", "unknown")
        date_clean = date_raw.split("T")[0] if "T" in date_raw else date_raw

        # Teams and scores
        home_team = g.get("homeTeam", "unknown")
        away_team = g.get("awayTeam", "unknown")

        if home_team == TEAM:
            opponent = away_team
            points_for = g.get("homePoints", 0)
            points_against = g.get("awayPoints", 0)
            home = 1
        else:
            opponent = home_team
            points_for = g.get("awayPoints", 0)
            points_against = g.get("homePoints", 0)
            home = 0

        games.append({
            "date": date_clean,
            "opponent": opponent,
            "points_for": points_for,
            "points_against": points_against,
            "home": home
        })

    return games




def setup_db(db_name):
    '''
    Sets up the SQLite database connection.

    ARGUMENTS:
        db_name: database filename

    RETURNS:
        cursor, connection
    '''
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + db_name)
    cur = conn.cursor()
    print(f"[SETUP] Connected to database: {db_name}")
    return cur, conn




def create_opponent_table(cur):
    cur.execute('''
        CREATE TABLE IF NOT EXISTS opponents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')

#helpers for storing data with foreign keys
def get_opponent_id(cur, opponent_name):
    # Check if opponent exists
    cur.execute("SELECT id FROM opponents WHERE name = ?", (opponent_name,))
    row = cur.fetchone()

    if row:
        return row[0]

    # Insert new opponent
    cur.execute("INSERT INTO opponents (name) VALUES (?)", (opponent_name,))
    return cur.lastrowid


def store_cfb_data(games, db_name):
    cur, conn = setup_db(db_name)

    # Create required tables
    create_opponent_table(cur)

    cur.execute('''
        CREATE TABLE IF NOT EXISTS cfb_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            opponent_id INTEGER,
            points_for INTEGER,
            points_against INTEGER,
            home INTEGER,
            FOREIGN KEY(opponent_id) REFERENCES opponents(id)
        )
    ''')

    # Batch size
    batch_size = 25


    # Process in batches of 25
    for i in range(0, len(games), batch_size):
        batch = games[i:i + batch_size]

        inserts = []
        for g in batch:
            opponent_id = get_opponent_id(cur, g["opponent"])
            inserts.append((
                g["date"],
                opponent_id,
                g["points_for"],
                g["points_against"],
                g["home"]
            ))

        # Insert the batch into the databasel
        cur.executemany('''
            INSERT INTO cfb_games (date, opponent_id, points_for, points_against, home)
            VALUES (?, ?, ?, ?, ?)
        ''', inserts)

        print(f"Inserted batch {i//batch_size + 1}")

    conn.commit()
    conn.close()



    

   


def load_cfb_data(db_name):
    cur, conn = setup_db(db_name)

    cur.execute('''
        SELECT c.date, o.name, c.points_for, c.points_against, c.home
        FROM cfb_games AS c
        JOIN opponents AS o ON c.opponent_id = o.id
    ''')

    rows = cur.fetchall()
    conn.close()

    return [
        {
            "date": r[0],
            "opponent": r[1],
            "points_for": r[2],
            "points_against": r[3],
            "home": r[4]
        }
        for r in rows
    ]




# UNIT TESTS 

class TestCases(unittest.TestCase):

    def setUp(self):
        # ensure we can read the key
        self.cfb_key = get_api_key("cfb_key.txt")

    def test_get_api_key(self):
        hidden_key = get_api_key("cfb_key.txt")
        self.assertEqual(API_KEY, hidden_key)

    def test_process_cfb_data(self):
        # mock API sample
        sample_raw = [
            {
                "start_date": "2023-10-14T19:00Z",
                "home_team": "Michigan",
                "away_team": "Indiana",
                "home_points": 52,
                "away_points": 7
            }
        ]

        processed = process_cfb_data(sample_raw)

        expected = {
            "date": "2023-10-14",
            "opponent": "Indiana",
            "points_for": 52,
            "points_against": 7,
            "home": 1
        }

        self.assertEqual(processed[0], expected)


def main():
    print("MAIN IS RUNNING")

    team = "Michigan"
    years = [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]   # <-- add/remove years as needed

    all_games = []

    for y in years:
        print(f"\nPulling data for {team} in {y}...")
        raw = get_cfb_data(team, y)

        if not raw:
            print(f"No data found for {y}")
            continue

        processed = process_cfb_data(raw)
        print(f"Processed {len(processed)} games from {y}")

        all_games.extend(processed)

    print("\nTotal games across all years:", len(all_games))

    # Store all games in the database
    store_cfb_data(all_games, "temp.db")

    # Confirm database row count
    count = load_cfb_data("temp.db")
    print("\nRows currently stored in the database:", count)


    


if __name__ == "__main__":
    main()
    #unittest.main(verbosity=2)
