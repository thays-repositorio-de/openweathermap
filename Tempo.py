import requests
import json
from prefect import task, flow
from google.cloud import bigquery

# Chave da API
API_KEY = "7fcee41f9bc8988e16fca9fb9422be96"  

# Lista de capitais do Brasil com coordenadas
capitais = {
    "Rio Branco": (-9.97499, -67.8243),
    "Maceió": (-9.66599, -35.735),
    "Macapá": (0.03456, -51.0694),
    "Manaus": (-3.1181, -60.0217),
    "Salvador": (-12.9711, -38.5108),
    "Fortaleza": (-3.71722, -38.5433),
    "Brasília": (-15.7801, -47.9292),
    "Goiânia": (-16.6864, -49.2643),
    "São Luís": (-2.5303, -44.3027),
    "Teresina": (-5.0892, -42.8018),
    "Natal": (-5.7945, -35.211),
    "João Pessoa": (-7.1154, -34.8641),
    "Recife": (-8.0476, -34.8777),
    "Aracaju": (-10.9472, -37.0731),
    "Belém": (-1.4556, -48.5046),
    "Rio de Janeiro": (-22.9068, -43.1729),
    "Vitória": (-20.3155, -40.3128),
    "Campo Grande": (-20.442, -54.6463),
    "Cuiabá": (-15.601, -56.0977),
    "Porto Velho": (-8.7612, -63.8999),
    "Boa Vista": (2.8236, -60.6753),
    "Palmas": (-10.3215, -48.3542),
    "Florianópolis": (-27.5954, -48.5480),
    "Curitiba": (-25.4284, -49.2733),
    "Porto Alegre": (-30.0346, -51.2177)
}

@task
def fetch_weather_data(city, coords):
    lat, lon = coords
    url = f"http://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar dados para {city}: {response.status_code} - {response.text}")
        return None

@task
def insert_into_bigquery(data):
    client = bigquery.Client()
    table_id = "repsthays.repositorios.openweathermap"  
    rows_to_insert = []

    for entry in data:
        if entry: 
            rows_to_insert.append({
                "city": data["name"],
                "temperature": data["main"]["temp"],
                "weather": data["weather"][0]["description"],
                "timestamp": data["dt"],
                "humidity": data["main"]["humidity"],
                "wind_speed": data["wind"]["speed"],
                "pressure": data["main"]["pressure"],     
                "cloudiness": data["clouds"]["all"],          
                "sunrise": datetime.fromtimestamp(data["sys"]["sunrise"]), 
                "sunset": datetime.fromtimestamp(data["sys"]["sunset"]),    
                "feels_like": data["main"]["feels_like"],      
                "visibility": data["visibility"],             
            })

    if rows_to_insert:
        errors = client.insert_rows_json(table_id, rows_to_insert) 
        if errors == []:
            print("Dados inseridos com sucesso.")
        else:
            print(f"Erros ao inserir dados: {errors}")
    else:
        print("Nenhum dado para inserir.")

@flow
def weather_to_bigquery():
    weather_data = []
    for city, coords in capitais.items():
        data = fetch_weather_data(city, coords)
        weather_data.append(data)

    insert_into_bigquery(weather_data)

if __name__ == "__main__":
    weather_to_bigquery()