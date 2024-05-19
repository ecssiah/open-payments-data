import os
import sqlite3
from typing import List, Dict

class DatabaseManager:
    paths: Dict[str, str]
    fields: List[str]
    connection: sqlite3.Connection
    cursor: sqlite3.Cursor

    def __init__(self) -> None:
        self._setup_paths()

        self.fields = [
            'covered_recipient_type',
            'covered_recipient_profile_id',
            'covered_recipient_npi',
            'covered_recipient_first_name',
            'covered_recipient_middle_name',
            'covered_recipient_last_name',
            'covered_recipient_name_suffix',
            'recipient_primary_business_street_address_line1',
            'recipient_primary_business_street_address_line2',
            'recipient_city',
            'recipient_state',
            'recipient_zip_code',
            'covered_recipient_primary_type_1',
            'submitting_applicable_manufacturer_or_applicable_gpo_name',
            'applicable_manufacturer_or_applicable_gpo_making_payment_id',
            'applicable_manufacturer_or_applicable_gpo_making_payment_name',
            'applicable_manufacturer_or_applicable_gpo_making_payment_state',
            'applicable_manufacturer_or_applicable_gpo_making_payment_country',
            'total_amount_of_payment_usdollars',
            'date_of_payment',
            'nature_of_payment_or_transfer_of_value',
            'indicate_drug_or_biological_or_device_or_medical_supply_1',
            'product_category_or_therapeutic_area_1',
            'name_of_drug_or_biological_or_device_or_medical_supply_1',
        ]

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

        field_definitions = ",\n".join([f"    {field} TEXT" for field in self.fields])

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
                f"    " + ",\n    ".join(self.fields) + "\n"
                f")\n"
                f"VALUES (" + ", ".join(["?" for _ in self.fields]) + ")"
            )

            values = tuple(data[field] for field in self.fields)

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
