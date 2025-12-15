import sqlite3
import matplotlib.pyplot as plt
import os

# Save name of database to use in functions
DB_NAME = "temp.db"

def fetch_game_weather_data():
    """
    Query database and find data for football games and their weather conditions 
    ARGUMENTS:
        None
    RETURNS:
        games: different football games with their weather conditions (list)

    """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path + "/" + DB_NAME)
    cur = conn.cursor()

    query = '''
        SELECT 
            d.day AS game_date,
            o.name AS opponent,
            c.points_for,
            c.points_against,
            c.home,
            w.temp_mean,
            w.cloud_cover,
            w.wind_speed,
            w.precipitation
        FROM cfb_games AS c
        JOIN dates AS d ON c.date_id = d.id
        JOIN opponents AS o ON c.opponent_id = o.id
        JOIN weather AS w ON c.date_id = w.date_id;

    '''

    cur.execute(query)
    rows = cur.fetchall()
    conn.close()

    games = []
    for r in rows:
        games.append({
            "points_for": r[2],
            "points_against": r[3],
            "home": r[4],
            "opponent": r[1],
            "temp_mean": r[5] or 0,
            "cloud_cover": r[6] or 0,
            "precipitation": r[8] or 0,
            "wind_speed": r[7] or 0,
            "date": r[0]
        })

    return games

# Analysis functions to create calculations

def precipitation_analysis(games):

    """
    Find percentage of points scored in games with precipitation
    ARGUMENTS:
        games: different football games with their weather conditions (list)
    RETURNS:
        rain_percentage: percentage of rain (float)
 
    """
    total_points = sum(g['points_for'] for g in games)
    rain_points = sum(g['points_for'] for g in games if g['precipitation'] > 0)

    rain_percentage = (rain_points / total_points) * 100 if total_points else 0
    print(f"Percentage of points scored in games with precipitation: {rain_percentage:.2f}%\n")

    return rain_percentage


def wind_home_advantage(games):
    """
    Find the home team advantages when wind speeds are above 15 mph and below 15mph
    ARGUMENTS:
        games: different football games with their weather conditions (list)
    RETURNS:
        averages: average points scored for home and away under different wind speeds (dictionary)
 
    """
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

def pts_by_temp(games):
    """
    Find the average points scored for a game when the mean temperature is above 45 degrees and below 45 degrees Fahrenheit
    ARGUMENTS:
        games: different football games with their weather conditions (list)
    RETURNS:
        averages: average points scored for home and away under different temperatues (dictionary)
 
    """
    categories = {
        "Below 45": [g['points_for'] for g in games if g['home'] == 1 and g['temp_mean'] < 45],
        "Above 45": [g['points_for'] for g in games if g['home'] == 1 and g['temp_mean'] > 45],
    }
    avgs = {}
    for key, points in categories.items():
        avg_pts = sum(points) / len(points) if points else 0
        avgs[key] = round(avg_pts, 2)
    return avgs

def pts_by_cloud(games):
    """
    Find the average points scored for a game when the cloud coverage is above 50 percent and below 50 percent
    ARGUMENTS:
        games: different football games with their weather conditions (list)
    RETURNS:
        averages: average points scored for home and away under different cloud covers (dictionary)
 
    """
    cates = {
    "Below 50%": [g['points_for'] for g in games if g['home'] == 1 and g['cloud_cover'] < 50],
    "Above 50%": [g['points_for'] for g in games if g['home'] == 1 and g['cloud_cover'] > 50],
    "Above 50% Against": [g['points_against'] for g in games if g['home'] == 1 and g['cloud_cover'] > 50],
    "Below 50% Against": [g['points_against'] for g in games if g['home'] == 1 and g['cloud_cover'] < 50],
    }
    avg = {}
    for key, points in cates.items():
        pts = sum(points) / len(points) if points else 0
        avg[key] = round(pts, 2)
    return avg

# Graphing functions for calculations

def plot_precipitation_graph(rain_percentage):
    """
    Graph the precipiation and no precipitation scores for Michigan football games
    ARGUMENTS:
        rain_percentage: percentage of rain (float)
    RETURNS:
        None 
    """

    plt.bar(["Precipitation", "No Precipitation"], [rain_percentage, 100 - rain_percentage])
    plt.title("Percent of Points Scored in Precipitation Michigan Football Games")
    plt.ylabel("Points Scored")
    plt.show()
    plt.close()

def plot_wind_graph(averages):
    """
    Graph the wind speed score averages for Michigan football games
    ARGUMENTS:
        averages: average scores under different wind speeds (dictionary)
    RETURNS:
        None 
    """
    labels = ["Home Low Wind", "Home High Wind", "Away Low Wind", "Away High Wind"]
    values = [
        averages["low_wind_home"],
        averages["high_wind_home"],
        averages["low_wind_away"],
        averages["high_wind_away"]
    ]

    plt.bar(labels, values, color='orange')
    plt.xticks(rotation=20)
    plt.ylabel("Average Points Scored")
    plt.title("Points Scored by Wind Speed + Home/Away")
    plt.show()
    plt.close()

def plot_pts_temp(avg_d):
    """
    Graph the mean temperature averages for Michigan football games
    ARGUMENTS:
        avg_d: average scores under different mean temperatures (dictionary)
    RETURNS:
        None 
    """
    colors = ["skyblue", "green"]
    plt.barh(list(avg_d.keys()), list(avg_d.values()), color=colors)
    plt.ylabel('Mean Temperature Fahrenheit')
    plt.xlabel('Points Scored')
    plt.title('Michigan Football Performance Under Temperature')
    plt.show()
    plt.close()

def plot_pts_cloud(avg):
    """
    Graph the cloud coverage score averages for Michigan football games
    ARGUMENTS:
        avg: average scores under different cloud coverages (dictionary)
    RETURNS:
        None 
    """

    categories = ["Below 50%", "Above 50%"]
    points_for = [avg["Below 50%"], avg["Above 50%"]]
    points_against = [avg["Below 50% Against"], avg["Above 50% Against"]]
    plt.figure(figsize=(8,6))
    colors = ["orange", "#319E84"]
    plt.bar(categories, points_for, label="Home Points", color=colors[1])
    plt.bar(categories, points_against, bottom=points_for, label="Away Points", color=colors[0])  

    plt.ylabel("Average Points")
    plt.xlabel("Cloud Coverage")
    plt.title("Average Points Scored By Michigan Football vs. Opponent")
    plt.legend()
    plt.show()            



def main():

    # Compute results for calculation questions
    print("ANALYSIS STARTING...\n")
    filename = 'results.txt'
    games = fetch_game_weather_data()
    total_games = f"Total games fetched with weather data: {len(games)}\n"
    print(total_games)
    rain_percentage = precipitation_analysis(games)
    averages = wind_home_advantage(games)

    averages_temp = pts_by_temp(games)
    cloud = pts_by_cloud(games)

    # Write calculation results to a text file
    base_path = os.path.abspath(os.path.dirname(__file__))
    full_path = os.path.join(base_path, filename)
    with open(full_path, "w") as file:
        file.write(f"{total_games}")
        file.write(f"Average points for low wind home: {averages['low_wind_home']}\n")
        file.write(f"Average points for high wind home: {averages['high_wind_home']}\n")
        file.write(f"Average points for low wind away: {averages['low_wind_away']}\n")
        file.write(f"Average points for high wind away: {averages['high_wind_away']}\n")
        file.write(f"Percentage of points scored in games with precipitation: {rain_percentage:.2f}%\n")
        file.write(f"Average points for temperatures below 45°F: {averages_temp['Below 45']}\n")
        file.write(f"Average points for temperatures above 45°F: {averages_temp['Above 45']}\n")
        file.write(f"Average home points for cloud coverage below 50%: {cloud['Below 50%']}\n")
        file.write(f"Average home points for cloud coverage above 50%: {cloud['Above 50%']}\n")
        file.write(f"Average away points for cloud coverage below 50%: {cloud['Below 50% Against']}\n")
        file.write(f"Average away points for cloud coverage Above 50%: {cloud['Above 50% Against']}\n")
  
    print("DEBUG averages =", averages)


    # Graph calculation results
    plot_precipitation_graph(rain_percentage)
    plot_wind_graph(averages)
    plot_pts_temp(averages_temp)
    plot_pts_cloud(cloud)



if __name__ == "__main__":
    main()

