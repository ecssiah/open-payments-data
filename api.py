import json
import math
import os
import pandas as pd
import requests
import time
from src.database_manager import DatabaseManager
from typing import Any, Dict

HOST = 'https://openpaymentsdata.cms.gov/api/1'
ENDPOINT = '/datastore/query'
DATASET = '2022_general'

DATASET_IDS = {
    '2022_general': '66dfcf9a-2a9e-54b7-a0fe-cae3e42f3e8f',
    '2021_general': '0e4bd5b3-eb80-57b3-9b49-3db89212d7c5',
    '2020_general': 'e51be53b-ed10-5fa5-819b-7c2474fbdea9',
    '2019_general': '5e08488e-eadd-5e82-a4f0-01a540ea0917',
    '2018_general': 'd6a4c192-42c9-5f36-85eb-4ab2f16bb8da',
    '2017_general': 'fd7e68cb-8e96-516d-817a-ab42c022ffd3',
    '2016_general': '02ed78a8-85e9-53a3-b1ec-2869cfc236fd',
}

STATES = ['NM', 'MA', 'NY', 'IL', 'MI', 'TX', 'NJ', 'PA', 'AZ', 'WA']

LIMIT = 500
BATCH = 100


def build_request(
    method: str, 
    hostname: str, 
    endpoint: str,
    headers: Dict[str, Any] = None,
    params: Dict[str, Any] = None,
    body: Dict[str, Any] = None
):
    method = method.upper()
    base_url = f"{hostname}{endpoint}"

    if method.upper() == 'GET':
        request = requests.Request(
            method, 
            base_url, 
            params=params,
            headers=headers
        ).prepare()

        return request
    elif method.upper() == 'POST':
        request = requests.Request(
            method,
            base_url,
            json=body,
            headers=headers
        ).prepare()
        
        return request


def get_request_body(offset = 0, limit = 100, format = 'json'):
    if limit > 500: limit = 500 

    body = {
        "conditions": [
            {
                "resource": "t",
                "property": "recipient_state",
                "value": STATES,
                "operator": "in"
            },
            {
                "resource": "t",
                "property": "covered_recipient_type",
                "value": "Covered Recipient Physician",
                "operator": "="
            }
        ],
        "format": format,
        "limit": limit,
        "offset": offset,
        "resources": [
            {
                "id": DATASET_IDS['2022_general'],
                "alias": "t"
            }
        ]
    }

    return body


def get_metadata():
    body = get_request_body(0, 1)

    request = build_request('POST', HOST, ENDPOINT, {}, {}, body)

    with requests.Session() as session:
        response = session.send(request)

        try:
            json_output = response.json()
            log_output = json.dumps(response.json(), indent=4)

            with open('data/log/request.log', 'w') as log_file:
                log_file.write(log_output)

            metadata = {
                'count': json_output['count'],
            }

            return metadata

        except requests.exceptions.RequestException as error:
            print('Error: ', error)


def fetch_data(start_page: int, total_pages: int, database_manager: DatabaseManager) -> None:
    if start_page == 1:
        database_manager.create_table()

    database_manager.open()

    with requests.Session() as session:
        for page in range(start_page, total_pages + 1):
            print(f'{page}/{total_pages}')

            body = get_request_body(page * LIMIT, LIMIT)

            request = build_request('POST', HOST, ENDPOINT, {}, {}, body)

            response = session.send(request)
            response.raise_for_status()

            data = response.json()['results']

            for index, row in enumerate(data):
                database_manager.insert_row(row)

                if index % BATCH == 0:
                    database_manager.commit()

            database_manager.write_progress(page)

            time.sleep(0.5)

    database_manager.close()


def run():
    os.makedirs('data/json', exist_ok=True)
    os.makedirs('data/log', exist_ok=True)

    database_manager = DatabaseManager()

    metadata = get_metadata()
    total_pages = math.ceil(metadata['count'] / LIMIT)

    print(f'Total Pages: {total_pages}')

    fetch_data(1, total_pages, database_manager)


if __name__ == '__main__':
    run()
