import os
import sqlite3
from src.constants import *
from typing import List, Dict

class DBManager:
    paths: Dict[str, str]
    fields: List[str]
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self) -> None:
        self._setup_paths()

        self.connection = None
        self.cursor = None


    def open(self) -> None:
        if self.connection is None:
            self.connection = sqlite3.connect(self.paths['db'])
            self.cursor = self.connection.cursor()


    def close(self) -> None:
        if self.connection:
            self.connection.commit()
            self.connection.close()

            self.connection = None
            self.cursor = None


    def commit(self) -> None:
        self.connection.commit()


    def _setup_paths(self) -> str:
        src_directory = os.path.dirname(__file__)

        self.paths = {}

        self.paths['root'] = os.path.dirname(src_directory)
        self.paths['db'] = os.path.join(self.paths['root'], 'data', 'payments.db')
        self.paths['log'] = os.path.join(self.paths['root'], 'data/log', 'progress.log')


    def write_progress(self, page) -> None:
        with open(self.paths['log'], 'w') as log_file:
            log_file.write(f"{page}\n")


    def read_progress(self) -> int:
        try:
            with open(self.paths['log'], 'r') as log_file:
                max_page = int(log_file.readline().strip())
                return max_page
        except Exception as e:
            print(f"Read Progress Error: {e}")
            return 1


    def create_table(self) -> None:
        self.delete()
        self.open()

        field_definitions = ",\n".join([f"    {field} TEXT" for field in FIELDS])

        query = (
            f"CREATE TABLE IF NOT EXISTS payments (\n"
            f"    id INTEGER PRIMARY KEY AUTOINCREMENT,\n"
            f"    {field_definitions}\n"
            f")"
        )

        self.cursor.execute(query)
        self.close()

    
    def insert_row(self, data: Dict[str, str]) -> None:
        try:
            query = (
                f"INSERT INTO payments (\n"
                f"    " + ",\n    ".join(FIELDS) + "\n"
                f")\n"
                f"VALUES (" + ", ".join(["?" for _ in FIELDS]) + ")"
            )

            values = tuple(data[field] for field in FIELDS)

            self.cursor.execute(query, values)
        except sqlite3.Error as err:
            self.connection.rollback()
            print(f"Error: {err}")


    def delete(self) -> None:
        if os.path.exists(self.paths['db']):
            os.remove(self.paths['db'])
            print(f"Deleted database: {self.paths['db']}")


    def print(self, rows=10) -> None:
        self.open()
        self.cursor.execute(f"SELECT * FROM payments LIMIT {rows}")

        rows = self.cursor.fetchall()

        for row in rows:
            print(row)
        
        self.close()
