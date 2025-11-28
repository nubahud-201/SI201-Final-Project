import os
import requests
import sqlite3
import unittest


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
        api_key = f.read().strip()
    return api_key


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


def process_cfb_data(raw_data):
    '''
    Processes the retrieved raw game data and extracts the fields we need.

    ARGUMENTS:
        raw_data: list of game dictionaries from the API

    RETURNS:
        A list of simplified dictionaries containing:
        - date
        - opponent
        - points_for
        - points_against
        - home (1 or 0)
    '''
    games = []
    TEAM = "Michigan"   # you can parameterize this if needed

    for g in raw_data:

        # ---- Safe date extraction (covers ALL CFB API variations) ----
        date_raw = (
            g.get("start_date") or
            g.get("start_time") or
            g.get("kickoff") or
            "unknown"
        )

        # clean something like "2023-10-14T19:00Z"
        if isinstance(date_raw, str) and "T" in date_raw:
            date_clean = date_raw.split("T")[0]
        else:
            date_clean = date_raw

        # ---- Opponent logic ----
        home_team = g.get("home_team", "unknown")
        away_team = g.get("away_team", "unknown")

        if home_team == TEAM:
            opponent = away_team
            points_for = g.get("home_points", 0)
            points_against = g.get("away_points", 0)
            home = 1
        else:
            opponent = home_team
            points_for = g.get("away_points", 0)
            points_against = g.get("home_points", 0)
            home = 0

        # ---- Build cleaned dictionary ----
        game = {
            "date": date_clean,
            "opponent": opponent,
            "points_for": points_for,
            "points_against": points_against,
            "home": home
        }

        games.append(game)

    print(raw_data[0].keys())


    return games



def setup_db(db_name):
    '''
    Sets up the SQLite database connection.

    ARGUMENTS:
        db_name: database filename

    RETURNS:
        cursor, connection
    '''
    import sqlite3
    conn = sqlite3.connect(db_name)
    cur = conn.cursor()
    print(f"[SETUP] Connected to database: {db_name}")
    return cur, conn





def store_cfb_data(games, db_name):
    '''
    Stores processed game data into a SQLite database.

    ARGUMENTS:
        games: list of processed game dicts
        db_name: database filename

    RETURNS:
        None
    '''
    cur, conn = setup_db(db_name)

    cur.execute('''
        CREATE TABLE IF NOT EXISTS cfb_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date TEXT,
            opponent TEXT,
            points_for INTEGER,
            points_against INTEGER,
            home INTEGER
        )
    ''')

    for g in games:
        cur.execute('''
            INSERT INTO cfb_games (date, opponent, points_for, points_against, home)
            VALUES (?, ?, ?, ?, ?)
        ''', (g["date"], g["opponent"], g["points_for"], g["points_against"], g["home"]))

    conn.commit()
    conn.close()


def load_cfb_data(db_name):
    '''
    Loads game data from the SQLite database.

    ARGUMENTS:
        db_name: database filename

    RETURNS:
        a list of game dictionaries
    '''
    cur, conn = setup_db(db_name)

    cur.execute("SELECT date, opponent, points_for, points_against, home FROM cfb_games")
    rows = cur.fetchall()

    conn.close()

    return len( [
        {
            "date": r[0],
            "opponent": r[1],
            "points_for": r[2],
            "points_against": r[3],
            "home": r[4]
        }
        for r in rows
    ])



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
    store_cfb_data(all_games, "cfb.db")

    # Confirm database row count
    count = load_cfb_data("cfb.db")
    print("\nRows currently stored in the database:", count)


    


if __name__ == "__main__":
    main()
    #unittest.main(verbosity=2)
