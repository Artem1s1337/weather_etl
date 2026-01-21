CREATE TABLE IF NOT EXISTS dim_cities (
    city_id INT PRIMARY KEY,
    city_name VARCHAR(50),
    latitude DECIMAL(6,4),
    longitude DECIMAL(6,4)
);

CREATE TABLE IF NOT EXISTS dim_weather (
    weather_id INT PRIMARY KEY,
    main VARCHAR(20),
    description VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS raw_weather (
    id SERIAL PRIMARY KEY,
    dt INTEGER,
    temp DECIMAL(5,2),
    feels_like DECIMAL(5,2),
    temp_min DECIMAL(5,2),
    temp_max DECIMAL(5,2),
    pressure INTEGER,
    humidity INTEGER,
    weather_id INTEGER,
    clouds INTEGER,
    wind_speed DECIMAL(5,2),
    wind_deg INTEGER,
    wind_gust DECIMAL(5,2),
    visibility INTEGER,
    city_id BIGINT,
    population BIGINT,
    sunrise INTEGER,
    sunset INTEGER,
    FOREIGN KEY(city_id) REFERENCES dim_cities(city_id),
    FOREIGN KEY(weather_id) REFERENCES dim_weather(weather_id)
);
