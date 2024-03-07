import os
from datetime import datetime

from confluent_kafka import SerializingProducer
import simplejson as json

LONDON_COORDINATES = {
    "latitude": 51.5074,
    "longitude": -0.1278
}

BIRMINGHAM_COORDINATES = {
    "latitude": 52.4862,
    "longitude": -1.8904
}

# calculate movement increments

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] -
                      LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] -
                       LONDON_COORDINATES['longitude']) / 100

KAFKA_BOOTSTRAP_SERVER = os.getenv('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def simulate_vehicle_movement():
    pass


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)


if __name__ == "_main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-archit')

    except KeyboardInterrupt:
        print('Simulate ended by the user')
    except Exception as e:
        print(f'Unexpected error: {e}')
