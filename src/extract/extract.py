import os  # to get .env credentials
from dotenv import load_dotenv  # to load .env credentials
import requests  # main library to get data from the web
# Libraries to control timeouts between requests
import time
import random
import psycopg2  # main library to establish connection with PSQL
import pandas as pd  # to get weather conditions
import logging  # to log process of extraction and handle errors
import traceback  # to log traces

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)

class ExtractWeather:

    CITIES=[
        "Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan", 
        "Nizhny Novgorod", "Chelyabinsk", "Krasnoyarsk", "Samara", "Ufa", 
        "Rostov-on-Don", "Omsk", "Krasnodar", "Voronezh", "Perm", "Volgograd",
        "Saratov", "Tyumen", "Tolyatti", "Izhevsk", "Barnaul", "Ulyanovsk", 
        "Irkutsk", "Khabarovsk", "Yaroslavl", "Vladivostok", "Makhachkala", 
        "Tomsk", "Orenburg", "Kemerovo", "Novokuznetsk", "Ryazan", 
        "Naberezhnye Chelny", "Astrakhan", "Penza", "Kirov", "Lipetsk", 
        "Balashikha", "Cheboksary", "Kaliningrad", "Tula", "Stavropol", 
        "Kursk", "Ulan-Ude", "Tver", "Magnitogorsk", "Sochi", "Donetsk", 
        "Belgorod", "Bryansk"
    ]

    def __init__(self, api_key):

        self.api_key = api_key
        self.host = os.getenv('DB_HOST')
        self.port = os.getenv('DB_PORT')
        self.user = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.dbname = os.getenv('DB_NAME')
        self.conn = None
        self.cur = None
        logger.info(f'Intialization: host={self.host}, port={self.port}, user={self.user}, password={self.password}, dbname={self.dbname}')

    def connection(self):
        try:
            self.conn = psycopg2.connect(
                dbname=self.dbname,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password
            )
            self.cur = self.conn.cursor()
            logger.info(f'Connection with {self.dbname} established')
            self.check_tables()    
            return True
        except Exception as e:
            print(f'Failed to connect to {self.dbname}: {e}')
            return False
    
    def check_tables(self):
        tables_to_check = ['dim_cities', 'dim_weather', 'raw_weather']
        for table in tables_to_check:
            try:
                self.cur.execute(f"""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = '{table}'
                    );
                """)
                exists = self.cur.fetchone()[0]
                logger.info(f"Table {table}: {'exists' if exists else 'doesn\'t exist'}")
            except Exception as e:
                print(f"Failed to check table {table}: {e}")

    def get_forecast(self, cnt=40):
        time.sleep(random.uniform(0.5, 1.5))

        data = []
        url = 'https://api.openweathermap.org/data/2.5/forecast'
        for city in self.CITIES:
            params = {
                'q': f'{city},RU',
                'appid': self.api_key,
                'cnt': cnt,
            }
            try:
                resp = requests.get(url, params=params)
                if resp.status_code == 200:
                    jsn = resp.json()
                    
                    city_sunrise = jsn['city']['sunrise']
                    city_sunset = jsn['city']['sunset']
                    city_id = jsn['city'].get('id')
                    pop = jsn['city'].get('population')
                    logger.info(f'ID: {city_id}, population: {pop}')
                    logger.info(f'Forecasts for {city}: {len(jsn['list'])}')


                    for i, item in enumerate(jsn['list'], start=1):
                        row = (
                            item['dt'],
                            item['main'].get('temp'),
                            item['main'].get('feels_like'),
                            item['main'].get('temp_min'),
                            item['main'].get('temp_max'),
                            item['main'].get('pressure'),
                            item['main'].get('humidity'),
                            item['weather'][0].get('id'),
                            item['clouds'].get('all'),
                            item['wind'].get('speed'),
                            item['wind'].get('deg'),
                            item['wind'].get('gust'),
                            item.get('visibility'),
                            city_id,
                            pop,
                            city_sunrise,
                            city_sunset
                        )
                        data.append(row)
                        if i == 1:
                            logger.info(f'Data example: dt={row[0]}, temp={row[1]}, feels_like={row[2]}')
                    logger.info(f'Data for {city} added to list.\n')

                else:
                    logger.error(f'Error for {city}: {resp.status_code}')
            except Exception as e:
                print(f'Request error for {city}: {e}')
                continue
          
        logger.info(f'Records collected: {len(data)}')
        return data
  
    def get_cities(self):
        data = []
        url = 'https://api.openweathermap.org/data/2.5/forecast'
        for city in self.CITIES:
            params = {
                'q': f'{city},RU',
                'appid': self.api_key,
            }
            try:
                resp = requests.get(url, params=params)
                if resp.status_code == 200:
                    jsn = resp.json()

                    city_id = jsn['city'].get('id')
                    city_name = jsn['city'].get('name')
                    latitude = jsn['city']['coord'].get('lat')
                    longitude = jsn['city']['coord'].get('lon')

                    row = (city_id, city_name, latitude, longitude)
                    data.append(row)
                    logger.info(f'{city} data gathered: {row}')
                else:
                    logger.error(f'Error for {city}: {resp.status_code}')
            except Exception as e:
                print(f'Request error for {city}: {e}')
                continue

        logger.info(f'Records collected: {len(data)}')      
        return data
    
    def get_conditions(self):

        url = 'https://openweathermap.org/weather-conditions'
        try:
            df = pd.concat(pd.read_html(url)[1:])
            df.drop(columns=[3], inplace=True)
            df.rename(columns={0: 'weather_id', 1: 'main', 2: 'description'}, inplace=True)
            df.reset_index(inplace=True)
            df.drop(columns=['index'], inplace=True)
            logger.info(f'Shape of data: {df.shape}')
            records = list(df.itertuples(index=False, name=None))
        except Exception as e:
            print(f'Unexpected error: {e}')

        logger.info(f'Records collected: {len(records)}')    
        return records
    
    def insert_dim_tables(self):
        print("\n" + "=" * 30)
        print("Load dimension tables data...")
        print("=" * 30)

        if not self.connection():
            logger.info(f"Failed to connect to database.")
            return
        
        try:
            logger.info('\nGet dimensions for cities...')
            cities_data = self.get_cities()

            logger.info('\nGet dimensions for weather conditions...')
            conditions_data = self.get_conditions()

            logger.info('Collector\'s report:')
            logger.info(f'Cities: {len(cities_data)}')
            logger.info(f'Conditions: {len(conditions_data)}')

            # SQL queries
            q1 = '''
            INSERT INTO dim_cities (city_id, city_name, latitude, longitude) 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (city_id) DO NOTHING;
            '''

            q2 = '''
            INSERT INTO dim_weather (weather_id, main, description) 
            VALUES (%s, %s, %s)
            ON CONFLICT (weather_id) DO NOTHING;
            '''

            logger.info('Insert data into tables...')
            if cities_data:
                logger.info(f'Insert {len(cities_data)} cities')
                self.cur.executemany(q1, cities_data)
                logger.info('Cities inserted')
            
            if conditions_data:
                logger.info(f'Insert {len(conditions_data)} weather conditions pairs')
                self.cur.executemany(q2, conditions_data)
                logger.info('Weather conditions inserted')
            
            # Commit changes
            self.conn.commit()
            logger.info('Data successfully uploaded. Changes are commited.')

            # Check number of records
            self.cur.execute("SELECT COUNT(*) FROM dim_cities;")
            cities_count = self.cur.fetchone()[0]  # 50
            
            self.cur.execute("SELECT COUNT(*) FROM dim_weather;")
            weather_count = self.cur.fetchone()[0]  # 55

            # DB results
            logger.info('Records at database')
            logger.info(f'Cities: {cities_count}')
            logger.info(f'Weather conditions: {weather_count}')
        
        except Exception as e:
            self.conn.rollback()  # cancel transaction
            print(f"Error during inserting data: {e}")
            traceback.print_exc()
        finally:
            self.close()
    
    def insert_fact_table(self):
        print("\n" + "=" * 30)
        print("Load fact table data...")
        print("=" * 30)

        if not self.connection():
            logger.info(f"Failed to connect to database.")
            return
        
        try:
            logger.info('\nGet fact forecast data...')
            forecast_data = self.get_forecast(cnt=40)  # max for 5 days

            logger.info('Collector\'s report:')
            logger.info(f'Forecast: {len(forecast_data)}')

            if not forecast_data:
                logger.warning('No data to insert.')
                return

            # SQL query
            q = '''
            INSERT INTO raw_weather (
                dt, temp, feels_like, temp_min, temp_max, pressure, humidity, weather_id,
                clouds, wind_speed, wind_deg, wind_gust, visibility, city_id,
                population, sunrise, sunset
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (city_id, dt) DO NOTHING;
            '''
        
            logger.info('Insert data into tables...')
            if forecast_data:
                logger.info(f'Insert {len(forecast_data)} cities')
                if forecast_data:
                    test_record = forecast_data[0]
                    logger.info(f'Test record: {test_record}')
                    try:
                        self.cur.execute(q, test_record)
                        logger.info('Test record inserted.')
                    except Exception as e:
                        print(f'Failed to insert test record: {e}')
                        print(f'Check structure of the bale.')
                
                self.cur.executemany(q, forecast_data)
                logger.info('Forecast inserted')

            # Commit changes
            self.conn.commit()
            logger.info('Data successfully uploaded. Changes are commited.')

            # Check number of records
            self.cur.execute("SELECT COUNT(*) FROM raw_weather;")
            forecast_count = self.cur.fetchone()[0]

            # DB results
            logger.info('Records at database')
            logger.info(f'Forecasts: {forecast_count}')
        
        except Exception as e:
            self.conn.rollback()  # cancel transaction
            print(f"Error during inserting data: {e}")
            traceback.print_exc()
        finally:
            self.close()
    
    def close(self):

        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        logger.info('Connection is closed.')

def main():
    load_dotenv()

    api_key = os.getenv('API_KEY')
    if not api_key:
        logger.info('API key has not found. Check .env file')
        return
    
    extract = ExtractWeather(api_key)
    extract.insert_dim_tables()
    extract.insert_fact_table()

if __name__ == "__main__":
    main()
