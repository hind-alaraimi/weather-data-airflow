from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

API_KEY = '57dcf5b87a49595dfc161d49791de1ff'
CITIES = {
    "Muscat": (23.5880, 58.3829),
    "London": (51.5074, -0.1278),
    "Paris": (48.8566, 2.3522),
    "Riyadh": (24.7136, 46.6753),
    "Dubai": (25.276987, 55.296249),
    "Tokyo": (35.6895, 139.6917),
    "Los Angeles": (34.0522, -118.2437)
}

CSV_FILE = '/home/iman/airflow/weatherCities10h.csv' 

def fetch_weather_data():
    records = []
    for city in CITIES.keys():
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric'
        response = requests.get(url)
        data = response.json()
        dt_object = datetime.fromtimestamp(data['dt'])
        record = {
            "City": data['name'],
            "Date": dt_object.strftime('%Y-%m-%d'),
            "Time": dt_object.strftime('%H:%M:%S'),
            "Temperature (°C)": data['main']['temp'],
            "Weather": data['weather'][0]['description'],
            "Humidity (%)": data['main']['humidity'],
            "Pressure (hPa)": data['main']['pressure'],
            "Wind Speed (m/s)": data['wind']['speed'],
            "Cloudiness (%)": data['clouds']['all']
        }
        records.append(record)

    df = pd.DataFrame(records)

    if os.path.exists(CSV_FILE):
        df.to_csv(CSV_FILE, mode='a', index=False, header=False)
    else:
        df.to_csv(CSV_FILE, index=False)

default_args = {
    'owner': 'iman',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='weather_data_collector',
    default_args=default_args,
    description='Collect weather data every hour and save to CSV',
    schedule_interval='@hourly',  # <-- now runs every 1 hour
    start_date=datetime(2025, 5, 27),
    catchup=False,
) as dag:
    task = PythonOperator(
        task_id='fetch_weather',
        python_callable=fetch_weather_data
    )
