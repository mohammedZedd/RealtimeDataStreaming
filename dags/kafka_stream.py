from datetime import datetime
import uuid
import requests # type: ignore
import json
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
import time 

default_args = {
    'owner': 'airscholar',
    'start_date': datetime.utcnow()
}



def get_data():

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res =res['results'][0]

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


def stream_data():
    print('data')
    from kafka import KafkaProducer # type: ignore
    import logging
    
    logging.basicConfig(level=logging.DEBUG)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    end_time = time.time() + 60  # Exécuter pendant 60 secondes
    
    while time.time() < end_time:
        try:
            res = get_data()
            res = format_data(res)
            producer.send('users_created', json.dumps(res).encode('utf-8'))
            logging.info(f"Sent data: {res}")
            time.sleep(5)  # Attendre 5 secondes entre les envois
        except Exception as e:
            logging.error(f'An error occurred: {e}')



with DAG('user_automation',
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False) as dag:
    
        streaming_task = PythonOperator(
            task_id ='stream_data_from_api',
            python_callable =stream_data
        )

