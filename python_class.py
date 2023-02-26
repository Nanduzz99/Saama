import csv
import psycopg2
from typing import List

class CsvToDbWriter:
    def __init__(self, csv_paths: List[str], db_host: str, db_schema: str):
        self.csv_paths = csv_paths
        self.db_host = db_host
        self.db_schema = db_schema
        self.conn = psycopg2.connect(
            host=self.db_host,
            database=self.db_schema,
            user="your_username",
            password="your_password"
        )

    def read_csv(self, csv_path: str):
        with open(csv_path, 'r') as f:
            reader = csv.reader(f)
            header = next(reader)
            rows = []
            for row in reader:
                rows.append(row)
            return header, rows

    def create_table(self, header):
        cur = self.conn.cursor()
        columns = ', '.join([f"{column} VARCHAR(255)" for column in header])
        create_table_query = f"CREATE TABLE IF NOT EXISTS target_table ({columns})"
        cur.execute(create_table_query)
        self.conn.commit()

    def write_to_db(self, header, rows):
        cur = self.conn.cursor()
        placeholders = ','.join(['%s'] * len(header))
        columns = ','.join(header)
        insert_query = f"INSERT INTO target_table ({columns}) VALUES ({placeholders})"
        cur.executemany(insert_query, rows)
        self.conn.commit()

    def run(self):
        for csv_path in self.csv_paths:
            header, rows = self.read_csv(csv_path)
            self.create_table(header)
            self.write_to_db(header, rows)

    def close(self):
        self.conn.close()
