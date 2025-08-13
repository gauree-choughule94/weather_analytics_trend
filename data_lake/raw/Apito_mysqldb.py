import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv
import os
 
dotenv_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.env"))
load_dotenv(dotenv_path)
 
API_KEY = "JED5ML322LAKGZYX23NXP8ALB"
 
FILESS_MYSQL_HOST = os.getenv("FILESS_MYSQL_HOST")
FILESS_MYSQL_PORT = int(os.getenv("FILESS_MYSQL_PORT"))
FILESS_MYSQL_USER = os.getenv("FILESS_MYSQL_USER")
FILESS_MYSQL_PASSWORD = os.getenv("FILESS_MYSQL_PASSWORD")
FILESS_MYSQL_DATABASE = os.getenv("FILESS_MYSQL_DATABASE")
 
cities = ["Jabalpur"]
 
# Date range
start_date = "2024-08-01"
end_date = "2025-08-12"
 
conn = mysql.connector.connect(
    host=FILESS_MYSQL_HOST,
    port=FILESS_MYSQL_PORT,
    user=FILESS_MYSQL_USER,
    password=FILESS_MYSQL_PASSWORD,
    database=FILESS_MYSQL_DATABASE,
)
cursor = conn.cursor()
 
BASE_URL = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
 
for city in cities:
    url = f"{BASE_URL}{city}/{start_date}/{end_date}"
    params = {
        "unitGroup": "metric",
        "key": API_KEY,
        "include": "days",
        "contentType": "json"
    }
 
    print(f"Fetching weather for: {city}")
    response = requests.get(url, params=params)
 
    if response.status_code == 200:
        data = response.json()
 
        for day in data.get("days", []):
            # Check if record already exists (city + datetime)
            check_sql = "SELECT COUNT(*) FROM city_weather WHERE name=%s AND datetime=%s"
            cursor.execute(check_sql, (city, day.get("datetime")))
            if cursor.fetchone()[0] > 0:
                continue  
 
            sql = """
                INSERT INTO city_weather
                (name, datetime, tempmax, tempmin, temp, feelslikemax, feelslikemin, feelslike, dew, humidity,
                 precip, precipprob, precipcover, preciptype, snow, snowdepth, windgust, windspeed, winddir,
                 sealevelpressure, cloudcover, visibility, solarradiation, solarenergy, uvindex, severerisk,
                 sunrise, sunset, moonphase, conditions, description, icon, stations)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s)
            """
 
            values = (
                city,
                day.get("datetime"),
                day.get("tempmax"),
                day.get("tempmin"),
                day.get("temp"),
                day.get("feelslikemax"),
                day.get("feelslikemin"),
                day.get("feelslike"),
                day.get("dew"),
                day.get("humidity"),
                day.get("precip"),
                day.get("precipprob"),
                day.get("precipcover"),
                ",".join(day.get("preciptype", [])) if isinstance(day.get("preciptype"), list) else day.get("preciptype"),
                day.get("snow"),
                day.get("snowdepth"),
                day.get("windgust"),
                day.get("windspeed"),
                day.get("winddir"),
                day.get("sealevelpressure"),
                day.get("cloudcover"),
                day.get("visibility"),
                day.get("solarradiation"),
                day.get("solarenergy"),
                day.get("uvindex"),
                day.get("severerisk"),
                day.get("sunrise"),
                day.get("sunset"),
                day.get("moonphase"),
                day.get("conditions"),
                day.get("description"),
                day.get("icon"),
                ",".join(day.get("stations", [])) if isinstance(day.get("stations"), list) else day.get("stations")
            )
 
            cursor.execute(sql, values)
        conn.commit()
    else:
        print(f"Failed to fetch data for {city}: {response.status_code} - {response.text}")
 
cursor.close()
conn.close()
print(" Data inserted into MySQL (Filess.io) successfully!")
 