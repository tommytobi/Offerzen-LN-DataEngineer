import os
from pathlib import Path
from sqlalchemy import create_engine, Table, MetaData
from datetime import datetime


class Settings:
    _instance = None
    _initialized = False

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(Settings, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        # Prevent re-running __init__ every time Settings() is called
        if self.__class__._initialized:
            return

        # database settings
        self.db_name = os.environ["POSTGRES_DB"]
        self.db_user = os.environ["POSTGRES_USER"]
        self.db_password = os.environ["POSTGRES_PASSWORD"]
        self.db_host = os.environ.get("POSTGRES_HOST", "localhost")
        self.db_port = int(os.environ.get("POSTGRES_PORT", 5432))
        self.db_type = os.environ.get("POSTGRES_TYPE", "postgresql")
        self.db_driver = os.environ.get("POSTGRES_DRIVER", "psycopg")

        self.database_url = (
            f"{self.db_type}+{self.db_driver}://"
            f"{self.db_user}:{self.db_password}@{self.db_host}:{self.db_port}/{self.db_name}"
        )

        self.engine = create_engine(self.database_url)
        self.metadata = MetaData()

        # data file paths
        _data_dir = Path(__file__).parent / "data"
        
        self.customers_path = _data_dir / "customers.csv"
        self.orders_path = _data_dir / "orders.jsonl"
        self.order_items_path = _data_dir / "order_items.csv"

        # model settings
        self.start_date = datetime(2023,1,1)
        self.end_date_x = datetime(2023, 6,30)
        self.end_date_y = datetime(2023,12,31)
        self.num_skus = 10000

        self.__class__._initialized = True

    def get_engine(self):
        return self.engine