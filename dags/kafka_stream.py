import json
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import requests


default_args = {
    'owner': 'DmytroKuriptya',
    'start_date': datetime(2024, 4, 12, 10, 00)
}


def get_data():
    url = 'https://randomuser.me/api/'
    response = requests.get(url, timeout=10)
    response_json = response.json()
    res = response_json['results'][0]

    return res


def format_data(data):
    result = {}

    location = data['location']
    result['first_name'] = data['name']['first']
    result['last_name'] = data['name']['last']
    result['gender'] = data['gender']
    result['address'] = f"{location['street']['number']} {location['street']['name']}, {location['city']}, {location['state']}, {location['country']}"
    result['postcode'] = location['postcode']
    result['email'] = data['email']
    result['username'] = data['login']['username']
    result['dob'] = data['dob']['date']
    result['resigtered_date'] = data['registered']['date']
    result['phone'] = data['phone']
    result['picture'] = data['picture']['medium']

    return result


def stream_data():
    data = get_data()
    res = format_data(data)
    print(json.dumps(res, indent=3))


stream_data()

with DAG(
    dag_id='user_automation',
    default_args=default_args,
    schedule_interval='@daily',
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
