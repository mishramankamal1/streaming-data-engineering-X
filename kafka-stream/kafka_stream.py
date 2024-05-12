import json

import requests
import uuid
from kafka import KafkaProducer
import logging


def get_data_from_api():
    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res


def format_data(res):
    data = {}
    location = res['location']
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data


def stream_data():
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)

    while True:
        try:
            res = get_data_from_api()
            res = format_data(res)
            producer.send('user_created', json.dumps(res).encode('utf-8'))
            print(f"Message produced for {res.get('last_name')}, {res.get('first_name')}")
        except Exception as e:
            logging.error(f'An error occurred: {e}')
            continue


if __name__ == '__main__':
    stream_data()
