# importing libraries
import json
import requests
import time
from kafka import KafkaProducer

# Define Kafka urrl and Kafka topic names
kafka_bootstrap_servers = "localhost:9092"
kafka_topic_temp = "temperature"
kafka_topic_hum = "humidity"

# Connect to Kafka
producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)


url = "https://weatherapi-com.p.rapidapi.com/current.json"

querystring = {"q": "Alexandria"}

headers = {
    "X-RapidAPI-Key": "3aec5547b3mshd01998f6780fd4bp187fcajsn04adb713e9de",
    "X-RapidAPI-Host": "weatherapi-com.p.rapidapi.com"
}


while True:
    # get current weather data
    response = requests.get(url, headers=headers, params=querystring)
    data = response.json()
    get_time = int(time.time())
    # get temperature data from weather
    temperature_data = {
        "time": get_time,
        "temperature": data["current"]["temp_c"]
    }
    humidity_data = {
        "time": get_time,
        "humidity": data["current"]["humidity"]
    }
    # send temperature data to Kafka server
    producer.send(kafka_topic_temp, value=json.dumps(
        temperature_data).encode('utf-8'))
    # print serialized temperature data
    print(json.dumps(temperature_data).encode('utf-8'))
    # send humidity data to kafka server
    producer.send(kafka_topic_hum, value=json.dumps(
        humidity_data).encode("utf-8"))
    # print serialized humidity data
    print(json.dumps(humidity_data).encode("utf-8"))
    producer.flush()
    # wait 2 seconds
    time.sleep(1)
