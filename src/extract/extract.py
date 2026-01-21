import os
from dotenv import load_dotenv
import requests
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

class ExtractWeather:

    CITIES=["Moscow", "Saint Petersburg", "Novosibirsk", "Yekaterinburg", "Kazan",
            "Nizhny Novgorod", "Chelyabinsk", "Krasnoyarsk", "Samara", "Ufa",
            "Rostov-on-Don", "Omsk", "Krasnodar", "Voronezh", "Perm",
            "Volgograd", "Saratov", "Tyumen", "Tolyatti", "Izhevsk",
            "Barnaul", "Ulyanovsk", "Irkutsk", "Khabarovsk", "Yaroslavl",
            "Vladivostok", "Makhachkala", "Tomsk", "Orenburg", "Kemerovo",
            "Novokuznetsk", "Ryazan", "Naberezhnye Chelny", "Astrakhan", "Penza",
            "Kirov", "Lipetsk", "Balashikha", "Cheboksary", "Kaliningrad",
            "Tula", "Stavropol", "Kursk", "Ulan-Ude", "Tver",
            "Magnitogorsk", "Sochi", "Donetsk", "Belgorod", "Sevastopol"
            ]

    def __init__(self, api_key):

        self.api_key = api_key
        self.base_url = os.getenv('BASE_URL')
        self.add_url = os.getenv('ADD_URL')
        self.host = os.getenv('DB_HOST')
        self.database = os.getenv('DB_NAME')
        self.username = os.getenv('DB_USER')
        self.password = os.getenv('DB_PASSWORD')
        self.port = os.getenv('DB_PORT')

    def connection(self):

        db_url = f'postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}'
        return create_engine(db_url)

    def get_forecast(self, cnt=3):
        data = []

        for city in self.CITIES:
            url = self.base_url
            params = {
                'q': city,
                'appid': self.api_key,
                'cnt': cnt,
                }

            resp = requests.get(url, params)
            if resp.status_code == 200:
                jsn = resp.json()

                city_sunrise = jsn['city']['sunrise']
                city_sunset = jsn['city']['sunset']
                city_id = jsn['city'].get('id')
                pop = jsn['city'].get('population')

                for item in jsn['list']:
                    row = {
                        'dt': item['dt'],
                        'temp': item['main'].get('temp'),
                        'feels_like': item['main'].get('feels_like'),
                        'temp_min': item['main'].get('temp_min'),
                        'temp_max': item['main'].get('temp_max'),
                        'pressure': item['main'].get('pressure'),
                        'humidity': item['main'].get('humidity'),
                        'weather_id': item['weather'][0].get('id'),
                        'clouds': item['clouds'].get('all'),
                        'speed': item['wind'].get('speed'),
                        'deg': item['wind'].get('deg'),
                        'gust': item['wind'].get('gust'),
                        'visibility': item['visibility'],
                        'city_id': city_id,
                        'population': pop,
                        'sunrise': city_sunrise,
                        'sunset': city_sunset
                        }
                    data.append(row)
            else:
                print(f"Error for {city}: {resp.status_code}")
                continue
            
        if data:
            df = pd.DataFrame(data)
            engine = self.connection()
            df.to_sql(
                name='raw_weather',
                con=engine,
                index=False,
                if_exists='append'
            )
            print(f"Inserted {len(data)} weather records")
        else:
            print("No weather data collected")

    def get_cities(self):

        data = []

        for city in self.CITIES:
            url = self.base_url
            params = {
                'q': city,
                'appid': self.api_key,
                }

            resp = requests.get(url, params)
            if resp.status_code == 200:
                jsn = resp.json()

                city_id = jsn['city'].get('id')
                city_name = jsn['city'].get('name')
                latitude = jsn['city']['coord'].get('lat')
                longitude = jsn['city']['coord'].get('lon')

                dim_cities_dict = {
                    'city_id': city_id,
                    'city_name': city_name,
                    'latitude': latitude,
                    'longitude': longitude,
                }
            else:
                print(f'Error for {city}: {resp.status_code}')
                continue

        if data:
            df = pd.DataFrame(data)
            engine = self.connection()
            df.to_sql(
                name='dim_cities',
                con=engine,
                index=False,
                if_exists='append'
            )
            print(f"Inserted {len(data)} cities")
        else:
            print("No city data collected")

    def get_conditions(self):

        conditions_url = self.add_url
        df = pd.concat(pd.read_html(conditions_url)[1:])
        df.drop(columns=[3], inplace=True)
        df.rename(columns={0: 'weather_id', 1: 'main', 2: 'description'}, inplace=True)
        df.reset_index(inplace=True)
        df.drop(columns=['index'], inplace=True)

        engine = self.connection()
        df.to_sql(
            name='dim_weather',
            con=engine,
            index=False,
            if_exists='replace'
        )
        print(f"Inserted {len(df)} weather conditions")

if __name__ == "__main__":
    c = ExtractWeather(os.getenv('API_KEY'))
    c.get_cities()
    c.get_conditions()
    c.get_forecast(cnt=40)
