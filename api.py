import json
import math
import requests
from src.constants import *
from src.db_manager import DBManager
from src.paths import Paths
import time
from typing import Any, Dict


def build_request(
    method: str, 
    hostname: str, 
    endpoint: str,
    headers: Dict[str, Any] = None,
    params: Dict[str, Any] = None,
    body: Dict[str, Any] = None
):
    request = None
    method = method.upper()
    base_url = f"{hostname}{endpoint}"

    if method == 'GET':
        request = requests.Request(
            method, 
            base_url, 
            params=params,
            headers=headers
        ).prepare()
    elif method == 'POST':
        request = requests.Request(
            method,
            base_url,
            json=body,
            headers=headers
        ).prepare()

    return request


def get_request_body(offset = 0, limit = LIMIT, format = 'json'):
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

    request = build_request('POST', HOST, ENDPOINT, body=body)

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
    if start_page < 2:
        start_page = 1
        db_manager.create_table()

    db_manager.open()

    with requests.Session() as session:
        for page in range(start_page, total_pages + 1):
            print(f'\r{" " * 50}\r{page}/{total_pages} REQUEST ', end='')

            body = get_request_body(page * LIMIT)
            request = build_request('POST', HOST, ENDPOINT, body=body)

            response = session.send(request)
            response.raise_for_status()

            data = response.json()['results']

            print(f'\r{" " * 50}\r{page}/{total_pages} WRITE ', end='')

            for row in data:
                db_manager.insert_row(row)

            db_manager.commit()
            db_manager.write_progress(page)

            time.sleep(DELAY)

    print(f'\nComplete')

    db_manager.close()


def run():
    Paths.initialize()

    db_manager = DBManager()

    # metadata = get_metadata()
    # total_pages = math.ceil(metadata['count'] / LIMIT)

    # print(f'Total Pages: {total_pages}')

    # last_page = db_manager.read_progress()

    # fetch_data(last_page + 1, total_pages, db_manager)

    db_manager.write_csv()


if __name__ == '__main__':
    run()
