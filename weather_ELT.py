import pandas as pd
import sqlalchemy
import json

def extract():
    df = pd.read_csv("/opt/airflow/dags/weather.csv")
    return df.to_json(orient='records')  # returns JSON string

def transform(json_data):
    # Convert JSON string back to DataFrame
    df = pd.read_json(json_data, orient='records')

    # Convert 'Date' and 'Time' to datetime
    df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
    df['Time'] = pd.to_datetime(df['Time'], format="%H:%M:%S", errors='coerce')

    # Drop rows with invalid datetime
    df = df.dropna(subset=['Date', 'Time'])

    # Extract day and hour
    df['Day'] = df['Date'].dt.day
    df['Hour'] = df['Time'].dt.hour

    # Standardize city names
    df['City'] = df['City'].str.title()

    # Keep only date and time parts
    df['Date'] = df['Date'].dt.date
    df['Time'] = df['Time'].dt.time

    return df

def load(df, table_name="weather_data"):
    """Load the cleaned dataframe to MySQL table."""

    if df.isnull().any().any():
        raise ValueError("Null values found in data")

    engine = sqlalchemy.create_engine("mysql+mysqlconnector://root:root@host.docker.internal:3306/weatherDB")

    with engine.begin() as connection:
        df.to_sql(
            name=table_name,
            con=connection,
            if_exists='replace',  # This still works
            index=False,
            method='multi'
        )

    # Create the table directly without reflecting schema
    df.to_sql(name=table_name, con=engine, if_exists="replace", index=False, method='multi')

# For running locally
if __name__ == "__main__":
    raw_data = extract()
    cleaned_data = transform(raw_data)
    load(cleaned_data)
