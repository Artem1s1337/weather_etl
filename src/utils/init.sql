CREATE TABLE IF NOT EXISTS dim_cities (
  city_id INTEGER PRIMARY KEY,
  city_name VARCHAR(50),
  latitude DECIMAL(9, 6),
  longitude DECIMAL(9, 6)
);

CREATE TABLE IF NOT EXISTS dim_weather (
  weather_id INTEGER PRIMARY KEY,
  main VARCHAR(25),
  description VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw_weather (
  id SERIAL PRIMARY KEY,
  dt BIGINT,
  temp DECIMAL(7, 2),
  feels_like DECIMAL(7, 2),
  temp_min DECIMAL(7, 2),
  temp_max DECIMAL(7, 2),
  pressure INT,
  humidity INT,
  weather_id INT,
  clouds INT,
  wind_speed DECIMAL(7, 2),
  wind_deg INT,
  wind_gust DECIMAL(7, 2),
  visibility INT,
  city_id INT,
  population BIGINT,
  sunrise BIGINT,
  sunset BIGINT,
  FOREIGN KEY (weather_id) REFERENCES dim_weather(weather_id),
  FOREIGN KEY (city_id) REFERENCES dim_cities(city_id)
);

CREATE UNIQUE INDEX city_dt_idx 
ON raw_weather (city_id, dt);
