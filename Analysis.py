import sqlite3
import matplotlib.pyplot as plt
import os
DB_NAME = "temp.db"

def fetch_game_weather_data():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + DB_NAME)
    cur = conn.cursor()

    query = '''
        SELECT c.points_for, c.points_against, c.home, w.precipitation, w.wind_speed
        FROM cfb_games AS c
        JOIN weather AS w ON c.date_id = w.date_id
    '''

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

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




# ------------------ ANALYSIS FUNCTIONS ------------------

def precipitation_analysis(games):
    total_points = sum(g['points_for'] for g in games)
    rain_points = sum(g['points_for'] for g in games if g['precipitation'] > 0)

    rain_percentage = (rain_points / total_points) * 100 if total_points else 0
    print(f"Percentage of points scored in games with precipitation: {rain_percentage:.2f}%\n")

    return rain_percentage


def wind_home_advantage(games):
    categories = {
        "low_wind_home": [g['points_for'] for g in games if g['home'] == 1 and g['wind_speed'] <= 15],
        "high_wind_home": [g['points_for'] for g in games if g['home'] == 1 and g['wind_speed'] > 15],
        "low_wind_away": [g['points_for'] for g in games if g['home'] == 0 and g['wind_speed'] <= 15],
        "high_wind_away": [g['points_for'] for g in games if g['home'] == 0 and g['wind_speed'] > 15],
    }

    averages = {}
    for key, points in categories.items():
        avg_points = sum(points)/len(points) if points else 0
        averages[key] = avg_points
        print(f"Average points for {key.replace('_', ' ')}: {avg_points:.2f}")

    return averages


# ------------------ GRAPHING FUNCTIONS ------------------

def plot_precipitation_graph(rain_percentage):
    plt.bar(["Precipitation", "No Precipitation"], [rain_percentage, 100 - rain_percentage])
    plt.title("Percent of Points Scored in Precipitation Games")
    plt.ylabel("Percentage (%)")
    plt.show()
    plt.close()

def plot_wind_graph(averages):
    labels = ["Home Low Wind", "Home High Wind", "Away Low Wind", "Away High Wind"]
    values = [
        averages["low_wind_home"],
        averages["high_wind_home"],
        averages["low_wind_away"],
        averages["high_wind_away"]
    ]

    plt.bar(labels, values)
    plt.xticks(rotation=20)
    plt.ylabel("Average Points Scored")
    plt.title("Points Scored by Wind Speed + Home/Away")
    plt.show()
    plt.close()


# ------------------ MAIN ------------------

def main():
    print("ANALYSIS STARTING...\n")
    games = fetch_game_weather_data()
    print(f"Total games fetched with weather data: {len(games)}\n")

    rain_percentage = precipitation_analysis(games)
    averages = wind_home_advantage(games)

    
    print("DEBUG averages =", averages)


    # ---- HERE is where your graph functions go ----
    plot_precipitation_graph(rain_percentage)
    plot_wind_graph(averages)


if __name__ == "__main__":
    main()

