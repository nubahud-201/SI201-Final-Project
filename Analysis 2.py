import sqlite3
import matplotlib.pyplot as plt
import seaborn as sns
import os

DB_NAME = "temp.db"

def fetch_game_weather_data():
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + DB_NAME)
    cur = conn.cursor()

    query = '''
        SELECT 
            c.points_for, 
            c.points_against, 
            c.home, 
            w.temp_mean, 
            w.cloud_cover,
            d.day AS game_date
        FROM cfb_games AS c
        JOIN weather AS w ON c.date_id = w.date_id
        JOIN dates AS d ON c.date_id = d.id
        ORDER BY d.day
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
            "temp_mean": r[3] or 0,
            "cloud_cover": r[4] or 0,
            "date": r[5]
        })

    return games

def pts_by_temp(games):
    categories = {
        "below_temp": [g['points_for'] for g in games if g['home'] == 1 and g['temp_mean'] < 45],
        "above_temp": [g['points_for'] for g in games if g['home'] == 1 and g['temp_mean'] > 45],
    }
    avgs = {}
    for key, points in categories.items():
        avg_pts = sum(points) / len(points) if points else 0
        avgs[key] = round(avg_pts, 2)
    return avgs

def plot_pts_graph(avg_d):
    colors = ['tab:red', 'tab:blue', 'tab:green', 'tab:orange']
    plt.barh(list(avg_d.keys()), list(avg_d.values()), color=colors)
    plt.xlabel('Mean Temperature')
    plt.ylabel('Points Scored')
    plt.title('Average Points Scored by Michigan')
    plt.show()
    plt.close()


def main():
    print("ANALYSIS STARTING...\n")
    games = fetch_game_weather_data()
    print(f"Total games fetched with weather data: {len(games)}\n")
    averages = pts_by_temp(games)
    print(averages)

if __name__ == "__main__":
    main()