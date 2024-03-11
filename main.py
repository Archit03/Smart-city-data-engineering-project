import os
import uuid
from datetime import datetime, timedelta
import random
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

random.seed(1)
start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def simulate_vehicle_movement():
    global start_location
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weathercondition': random.choice(['Sunny', 'Rainy', 'Cloudy', 'Snowy']),
        'precipitation': random.uniform(0, 25),
        'windspeed': random.uniform(0, 30),  # km/h
        'humidity': random.uniform(0, 100),  # percentage
        'airqualityIndex': random.uniform(0, 500)  # AQI value goes here

    }


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),  # km/h
        'direction': 'North-East',
        'vehicle_type': vehicle_type

    }


def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedImage'
    }


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 20),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'X5',
        'year': 2019,
        'fueltype': 'hybrid'
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def produce_data_kafta(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode(('utf-8')),
        on_delivery=delivery_report
    )


def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'incident_type': uuid.uuid4(),
        'type': random.choice(['Fire', 'Flood', 'Earthquake', 'Tornado', 'road_accident']),
        'Timestamp': timestamp,
        'location': location,
        'Status': random.choice(['Active', 'resolved'])

    }


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id,
                                                           vehicle_data['timestamp'], vehicle_data['location'],
                                                           camera_id='Camera123')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incidents = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],

                                                               vehicle_data['location'])

        break


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVER,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-archit')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected error: {e}')
