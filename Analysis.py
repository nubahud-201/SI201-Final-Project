import sqlite3

DB_NAME = "temp.db"

def fetch_game_weather_data():
    """Fetches CFB games joined with weather info."""
    conn = sqlite3.connect(DB_NAME)
    cur = conn.cursor()

    query = '''
        SELECT c.points_for, c.points_against, c.home, w.precipitation, w.wind_speed
        FROM cfb_games AS c
        JOIN weather AS w ON c.date_id = w.date_id
    '''

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    # Convert to list of dicts
    games = []
    for r in rows:
        games.append({
            "points_for": r[0],
            "points_against": r[1],
            "home": r[2],
            "precipitation": r[3] or 0,
            "wind_speed": r[4] or 0
        })
    return games


def precipitation_analysis(games):
    total_points = sum(g['points_for'] for g in games)
    rain_points = sum(g['points_for'] for g in games if g['precipitation'] > 0)

    rain_percentage = (rain_points / total_points) * 100 if total_points else 0
    print(f"Percentage of points scored in games with precipitation: {rain_percentage:.2f}%\n")


def wind_home_advantage(games):
    categories = {
        "low_wind_home": [g['points_for'] for g in games if g['home'] == 1 and g['wind_speed'] <= 15],
        "high_wind_home": [g['points_for'] for g in games if g['home'] == 1 and g['wind_speed'] > 15],
        "low_wind_away": [g['points_for'] for g in games if g['home'] == 0 and g['wind_speed'] <= 15],
        "high_wind_away": [g['points_for'] for g in games if g['home'] == 0 and g['wind_speed'] > 15],
    }

    for key, points in categories.items():
        avg_points = sum(points)/len(points) if points else 0
        print(f"Average points for {key.replace('_', ' ')}: {avg_points:.2f}")


def main():
    print("ANALYSIS STARTING...\n")
    games = fetch_game_weather_data()
    print(f"Total games fetched with weather data: {len(games)}\n")

    precipitation_analysis(games)
    wind_home_advantage(games)


if __name__ == "__main__":
    main()
