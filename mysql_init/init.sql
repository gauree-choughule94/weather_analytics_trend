-- USE mysql_weather_db;

USE RawWeatherDB_variousarm;

CREATE TABLE IF NOT EXISTS city_weather (
  name VARCHAR(200),
  datetime DATETIME,
  tempmax FLOAT,
  tempmin FLOAT,
  temp FLOAT,
  feelslikemax FLOAT,
  feelslikemin FLOAT,
  feelslike FLOAT,
  dew FLOAT,
  humidity FLOAT,
  precip FLOAT,
  precipprob FLOAT,
  precipcover FLOAT,
  preciptype VARCHAR(100),
  snow FLOAT,
  snowdepth FLOAT,
  windgust FLOAT,
  windspeed FLOAT,
  winddir FLOAT,
  sealevelpressure FLOAT,
  cloudcover FLOAT,
  visibility FLOAT,
  solarradiation FLOAT,
  solarenergy FLOAT,
  uvindex FLOAT,
  severerisk FLOAT,
  sunrise TIME,
  sunset TIME,
  moonphase FLOAT,
  conditions VARCHAR(455),
  description VARCHAR(455),
  icon VARCHAR(100),
  stations VARCHAR(500)
);
