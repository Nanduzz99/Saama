import csv
import psycopg2
from typing import List

class CsvToDbWriter:
    def __init__(self, csv_paths: List[str], db_host: str, db_schema: str):
        self.csv_paths = csv_paths
        self.db_host = db_host
        self.db_schema = db_schema
        try:
            self.conn = psycopg2.connect(
            host=self.db_host,
            database=self.db_schema,
            user="@@@@",
            password="*******"
        )
        except psycopg2.Error as e:
            logging.error(f"Error connecting to database: {e}")
            raise e

    def read_csv(self, csv_path: str):
        try:
            with open(csv_path, 'r') as f:
                reader = csv.reader(f)
                header = next(reader)
                rows = []
                for row in reader:
                    rows.append(row)
                return header, rows
        except FileNotFoundError as e:
            logging.error(f"File not found: {csv_path}")
            raise e
        except csv.Error as e:
            logging.error(f"Error reading CSV file: {csv_path}")
            raise e

    def create_table(self, header):
        cur = self.conn.cursor()
        columns = ', '.join([f"{column} VARCHAR(255)" for column in header])
        create_table_query = f"CREATE TABLE IF NOT EXISTS target_table ({columns})"
        try:
            cur.execute(create_table_query)
            self.conn.commit()
        except psycopg2.Error as e:
            logging.error(f"Error creating table: {e}")
            raise e

    def write_to_db(self, header, rows):
        cur = self.conn.cursor()
        placeholders = ','.join(['%s'] * len(header))
        columns = ','.join(header)
        insert_query = f"INSERT INTO target_table ({columns}) VALUES ({placeholders})"
        try:
            cur.executemany(insert_query, rows)
            self.conn.commit()
        except psycopg2.Error as e:
            logging.error(f"Error writing to database: {e}")
            raise e

    def run(self):
        for csv_path in self.csv_paths:
            try:
                header, rows = self.read_csv(csv_path)
                self.create_table(header)
                self.write_to_db(header, rows)
            except Exception as e:
                logging.error(f"Error processing file {csv_path}: {e}")

    def close(self):
        self.conn.close()

