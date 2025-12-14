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
    print(f"[DEBUG] API response for {team} {year}: {response}")
    return response.json()



def process_cfb_data(raw_data, TEAM="Michigan"):
    """
    Cleans and processes API JSON into a list of games.
    """
    games = []
    for g in raw_data:
        date_raw = g.get("startDate", "unknown")
        date_clean = date_raw.split("T")[0] if "T" in date_raw else date_raw

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

    print(f"[DEBUG] First 3 processed games: {games[:3]}")
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
    """
    Create a table for various opponents in college football
    ARGUMENTS: 
        cur: cursor to execute SQL commands (cursor)
    RETURNS: 
        None
    """
    cur.execute('''
        CREATE TABLE IF NOT EXISTS opponents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        )
    ''')
    print("[DEBUG] Opponents table ensured.")

#helpers for storing data with foreign keys
def get_opponent_id(cur, opponent_name):

    """
    Find a specific opponent id for an opponent in opponents table
    ARGUMENTS: 
        cur: cursor to execute SQL commands (cursor)
        opponent_name: opponent to find (string)
    RETURNS: 
        None
    """

    cur.execute("SELECT id FROM opponents WHERE name = ?", (opponent_name,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("INSERT INTO opponents (name) VALUES (?)", (opponent_name,))
    return cur.lastrowid

def get_date_id(cur, date_str):
    """
    Find a specific date id for a date in dates table
    ARGUMENTS: 
        cur: cursor to execute SQL commands (cursor)
        date_str: date to find (string)
    RETURNS: 
        None
    """
    cur.execute("SELECT id FROM dates WHERE day = ?", (date_str,))
    row = cur.fetchone()
    if row:
        return row[0]
    print(f"[DEBUG] Date not found in dates table: {date_str}")
    return None

def load_cfb_data(cur):
    """
    Load current data about University of Michigan football games from database
    ARGUMENTS: 
        cur: cursor to execute SQL commands (cursor)
    RETURNS: 
        None
    """
    cur.execute('''
        SELECT c.id, d.day, o.name, c.points_for, c.points_against, c.home
        FROM cfb_games AS c
        JOIN opponents AS o ON c.opponent_id = o.id
        JOIN dates AS d ON c.date_id = d.id
    ''')
    rows = cur.fetchall()
    print(f"[DEBUG] Loaded {len(rows)} rows from cfb_games")
    return [
        {"game_id": r[0], "date": r[1], "opponent": r[2], "points_for": r[3], "points_against": r[4], "home": r[5]}
        for r in rows
    ]



def store_cfb_data(games, cur, conn):
    """
    Add data from College Football Data api to database in batches of 25 items
    ARGUMENTS:
        games: different University of Michigan football games with their statistics (dictionary)
        curr: cursor to execute SQL commands (cursor)
        conn: connection to link to database file  (connection)
    RETURNS:
        batch: a list of 25 items added to database

    """
    # Ensure opponents and cfb_games tables exist
    create_opponent_table(cur)
    cur.execute('''
        CREATE TABLE IF NOT EXISTS cfb_games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            date_id INTEGER,
            opponent_id INTEGER,
            points_for INTEGER,
            points_against INTEGER,
            home INTEGER,
            FOREIGN KEY(date_id) REFERENCES dates(id),
            FOREIGN KEY(opponent_id) REFERENCES opponents(id)
        )
    ''')
    rows_db = cur.fetchall()
    games_db = set()
    for row in rows_db:
        current = (row[0], row[1], row[2], row[3], row[4])
        games_db.add(current)

    remaining = []
    for g in games:
        current = (g["date"], g["opponent"], g["points_for"], g["points_against"], g["home"])
        if current not in games_db:
            remaining.append(g)
    

    batch_size = 25
    batch = remaining[:batch_size]
    inserts = []
    for g in batch:
        opponent_id = get_opponent_id(cur, g["opponent"])
        date_id = get_date_id(cur, g["date"])
        if date_id is None:
            # Insert missing date
            cur.execute("INSERT INTO dates (day) VALUES (?)", (g["date"],))
            date_id = cur.lastrowid
            print(f"[INFO] Added missing date {g['date']}")

        inserts.append((date_id, opponent_id, g["points_for"], g["points_against"], g["home"]))

    if inserts:
        cur.executemany('''
            INSERT INTO cfb_games (date_id, opponent_id, points_for, points_against, home)
            VALUES (?, ?, ?, ?, ?)
        ''', inserts)
        print(f"Inserted {len(inserts)} rows")

    conn.commit()
    return batch

# UNIT TESTS 

import unittest

class TestCFBFunctions(unittest.TestCase):

    def setUp(self):
        # Ensure the API key loads correctly
        self.api_key = get_api_key("cfb_key.txt")

    def test_get_api_key(self):
        # Make sure the global API_KEY matches what we read from the file
        key_from_file = get_api_key("cfb_key.txt")
        self.assertEqual(API_KEY, key_from_file)

    def test_process_cfb_data_home_game(self):
        # Mock raw data from API for a home game
        sample_raw = [
            {
                "startDate": "2023-10-14T19:00Z",
                "homeTeam": "Michigan",
                "awayTeam": "Indiana",
                "homePoints": 52,
                "awayPoints": 7
            }
        ]
        processed = process_cfb_data(sample_raw, TEAM="Michigan")

        expected = {
            "date": "2023-10-14",
            "opponent": "Indiana",
            "points_for": 52,
            "points_against": 7,
            "home": 1
        }

        self.assertEqual(processed[0], expected)

    def test_process_cfb_data_away_game(self):
        # Mock raw data from API for an away game
        sample_raw = [
            {
                "startDate": "2023-09-20T19:00Z",
                "homeTeam": "Ohio State",
                "awayTeam": "Michigan",
                "homePoints": 24,
                "awayPoints": 30
            }
        ]
        processed = process_cfb_data(sample_raw, TEAM="Michigan")

        expected = {
            "date": "2023-09-20",
            "opponent": "Ohio State",
            "points_for": 30,
            "points_against": 24,
            "home": 0
        }

        self.assertEqual(processed[0], expected)

    def test_process_cfb_data_missing_keys(self):
        # Handles case when API response is missing expected keys
        sample_raw = [{}]  # empty dict
        processed = process_cfb_data(sample_raw, TEAM="Michigan")

        expected = {
            "date": "unknown",
            "opponent": "unknown",
            "points_for": 0,
            "points_against": 0,
            "home": 0
        }

        self.assertEqual(processed[0], expected)

def main():
    print("MAIN IS RUNNING")
    db_name = "temp.db"
    cur, conn = setup_db(db_name)

    # Step 1: Check if dates table exists before CFB insert
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='dates'")
    if cur.fetchone():
        print("[CHECK] Dates table exists.")
    else:
        print("[CHECK] Dates table does NOT exist!")

    cur.execute("SELECT COUNT(*) FROM dates")
    print("[CHECK] Dates table row count:", cur.fetchone()[0])

    # Step 2: Pull CFB data
    team = "Michigan"
    years = [2023, 2022, 2021, 2020, 2019, 2018, 2017, 2016]
    all_games = []

    for y in years:
        print(f"\n[API] Pulling data for {team} in {y}...")
        raw = get_cfb_data(team, y)
        if not raw:
            print(f"[API] No data found for {y}")
            continue
        processed = process_cfb_data(raw)
        print(f"[API] Processed {len(processed)} games from {y}")
        all_games.extend(processed)

    print("\n[INFO] Total games across all years:", len(all_games))

    # Step 3: Store games
    inserted = store_cfb_data(all_games, cur, conn)
    print(f"[INFO] Added {len(inserted)} new games in this run")



    # Step 4: Verify inserted rows
    all_rows = load_cfb_data(cur)
    print("\n[INFO] Rows currently stored in cfb_games table:", len(all_rows))

    conn.close()
if __name__ == '__main__':
    main()
    unittest.main(verbosity=2)
