import csv
import os
import sqlite3
from src.constants import *
from src.paths import Paths
from typing import Dict


class DBManager:
    def __init__(self) -> None:
        self.connection: sqlite3.Connection = None
        self.cursor: sqlite3.Cursor = None

    
    def open(self) -> None:
        if self.connection is None:
            self.connection = sqlite3.connect(Paths.files['payments_db'])
            self.cursor = self.connection.cursor()


    def close(self) -> None:
        if self.connection:
            self.connection.commit()
            self.connection.close()

            self.connection = None
            self.cursor = None


    def commit(self) -> None:
        self.connection.commit()


    def write_progress(self, page) -> None:
        with open(Paths.files['progress_log'], 'w') as log_file:
            log_file.write(f"{page}\n")


    def read_progress(self) -> int:
        try:
            with open(Paths.files['progress_log'], 'r') as log_file:
                line = log_file.readline().strip()
                last_page = int(line)

                return last_page
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


    def remove_duplicates(self) -> None:
        self.open()

        fields_str = ', '.join(FIELDS)

        query = f'''
        WITH RankedPayments AS (
            SELECT
                id,
                {fields_str},
                ROW_NUMBER() OVER (PARTITION BY {fields_str} ORDER BY id) AS row_num,
                LAG(id) OVER (PARTITION BY {fields_str} ORDER BY id) AS prev_id
            FROM payments
        ),
        ConsecutiveDuplicates AS (
            SELECT id
            FROM RankedPayments
            WHERE row_num > 1
              AND prev_id IS NOT NULL
              AND prev_id = id - 1
        )
        DELETE FROM payments
        WHERE id IN (SELECT id FROM ConsecutiveDuplicates)
        '''
        
        self.cursor.execute(query)
        self.connection.commit()

        self.close()


    def delete(self) -> None:
        if os.path.exists(Paths.files['payments_db']):
            os.remove(Paths.files['payments_db'])
            print(f"Deleted: {Paths.files['payments_db']}")


    def write_csv(self) -> None:
        self.open()

        self.cursor.execute(f"SELECT * FROM payments")

        column_names = [description[0] for description in self.cursor.description]

        with open(Paths.files['payments_csv'], 'w', newline='') as csv_file:
            csv_writer = csv.writer(csv_file)

            csv_writer.writerow(column_names)
            csv_writer.writerows(self.cursor)

        self.close()


    def print(self, rows=10) -> None:
        self.open()

        self.cursor.execute(f"SELECT * FROM payments LIMIT {rows}")

        rows = self.cursor.fetchall()

        for row in rows:
            print(row)
        
        self.close()
