from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
import json

LATITUDE="38.9695545"
LONGITUDE= "-77.3860976"
POSTGRES_CONN_ID="postgres_default"
API_CONN_ID="open_meteo_api"

default_args = {
    'owner':"airflow",
    "start_date":days_ago(1)
}

## DAG:
with DAG(dag_id="etlweather",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    @task()
    def extract_weather_data():
        """
        Extracts weather data from OpenMeteo API using airflow connection
        """
        request = HttpHook(http_conn_id=API_CONN_ID, method='GET')

        ## Build the API endpoint:
        endpoint =f"/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"

        response = request.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to extract data from the OpenMeteo API {response.status_code}")


    @task()
    def transform_weather_data(weather_data):
        """
        Transform weather data from OpenMeteo API using airflow connection
        """
        current_weather = weather_data['current_weather']
        transformed_weather = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode'],
        }
        return transformed_weather


    @task()
    def load_weather_data(transformed_weather):
        """
        Load weather data from OpenMeteo API using airflow connection into postgres database
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create the table if not exists:
        cursor.execute("""
                        create table if not exists weather_data (
                        latitude double precision,
                        longitude double precision,
                        temperature double precision,
                        windspeed double precision,
                        winddirection double precision,
                        weathercode int,
                        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""")

        # Insert transformed data into the table:
        cursor.execute("""
        INSERT INTO weather_data(latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,(transformed_weather['latitude'],
             transformed_weather['longitude'],
             transformed_weather['temperature'],
             transformed_weather['windspeed'],
             transformed_weather['winddirection'],
             transformed_weather['weathercode']
            ))

        conn.commit() # Commits the inserts to the database.
        cursor.close() # Closes the cursor


    ## DAG workflow(ETL Pipeline): Chaining the tasks to the DAG:
    weather_data = extract_weather_data()
    transformed_weather = transform_weather_data(weather_data)
    load_weather_data(transformed_weather)