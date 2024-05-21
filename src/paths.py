import os
from typing import Dict


class Paths:
    dirs: Dict[str, str]
    files: Dict[str, str]

    @classmethod
    def initialize(cls) -> None:
        src_directory = os.path.dirname(__file__)

        cls.dirs = {
            'root': os.path.dirname(src_directory)
        }

        cls.dirs['data'] = os.path.join(cls.dirs['root'], 'data')
        cls.dirs['csv'] = os.path.join(cls.dirs['data'], 'csv')
        cls.dirs['json'] = os.path.join(cls.dirs['data'], 'json')
        cls.dirs['log'] = os.path.join(cls.dirs['data'], 'log')
        cls.dirs['sql'] = os.path.join(cls.dirs['data'], 'sql')
        cls.dirs['xls'] = os.path.join(cls.dirs['data'], 'xls')

        os.makedirs(cls.dirs['data'], exist_ok=True)
        os.makedirs(cls.dirs['csv'], exist_ok=True)
        os.makedirs(cls.dirs['json'], exist_ok=True)
        os.makedirs(cls.dirs['log'], exist_ok=True)
        os.makedirs(cls.dirs['sql'], exist_ok=True)
        os.makedirs(cls.dirs['xls'], exist_ok=True)

        cls.files = {
            'progress_log': os.path.join(cls.dirs['log'], 'progress.log'),
            'payments_csv': os.path.join(cls.dirs['csv'], 'payments.csv'),
            'payments_db': os.path.join(cls.dirs['sql'], 'payments.db'),
        }
