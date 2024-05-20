import json
import math
import os
import pandas as pd
import requests
import time
from src.constants import *
from src.db_manager import DBManager
from typing import Any, Dict


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
    return {
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
                "id": DATASET_IDS[DATASET],
                "alias": "t"
            }
        ]
    }


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


def fetch_data(start_page: int, total_pages: int, db_manager: DBManager) -> None:
    if start_page == 1:
        db_manager.create_table()

    db_manager.open()

    with requests.Session() as session:
        for page in range(start_page, total_pages + 1):
            print(f'\r{" " * 50}\r{page}/{total_pages} GET ', end='')

            body = get_request_body(page * LIMIT, LIMIT)
            request = build_request('POST', HOST, ENDPOINT, {}, {}, body)

            response = session.send(request)
            response.raise_for_status()

            data = response.json()['results']

            print(f'\r{" " * 50}\r{page}/{total_pages} WRITE ', end='')

            for index, row in enumerate(data):
                db_manager.insert_row(row)

                if index % BATCH == 0:
                    db_manager.commit()

            db_manager.write_progress(page)

            time.sleep(0.5)

    print('\r{" " * 50}\rCompleted all {total_pages} pages.', end='')

    db_manager.close()


def run():
    os.makedirs('data/json', exist_ok=True)
    os.makedirs('data/log', exist_ok=True)

    db_manager = DBManager()

    metadata = get_metadata()
    total_pages = math.ceil(metadata['count'] / LIMIT)

    print(f'Total Pages: {total_pages}')

    start_page = db_manager.read_progress()

    fetch_data(start_page, total_pages, db_manager)


if __name__ == '__main__':
    run()
