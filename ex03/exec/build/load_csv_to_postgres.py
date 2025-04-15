import pandas as pd
import re
import psycopg2
import os
import time
from io import StringIO
from datetime import datetime

class CSVToPostgres:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()

  def connect_to_postgres(self):
    time.sleep(5)  # Wait for PostgreSQL to be ready
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "your_password"),
        port=5432
    )
    return conn

  def pandas_to_postgres(self, dtype, column_name):
      print(f"pandas_to_postgres column : {column_name}, dtype: {dtype}")
      if column_name == self.df.columns[0] or pd.api.types.is_datetime64_any_dtype(dtype):  # Force DATETIME for first column
          return 'TIMESTAMP'
      if pd.api.types.is_integer_dtype(dtype) or dtype == 'int64':
          return 'INTEGER'
      elif pd.api.types.is_float_dtype(dtype) or dtype == 'float64':
          return 'FLOAT'
      elif pd.api.types.is_bool_dtype(dtype) or dtype == 'bool':
          return 'BOOLEAN'
      elif pd.api.types.is_timedelta64_dtype(dtype):
          return 'INTERVAL'
      else:
          return 'TEXT'

  def get_column_types(self):
      try:
          column_types = {}
      
          for column in self.df.columns:
              inferred_type = self.pandas_to_postgres(self.df[column].dtype, column)
              column_types[column] = inferred_type
          return (True, column_types)
      except pd.errors.ParserError:
          return (False, str(e))

  def create_table(self, column_types):
    mysql_command = f"CREATE TABLE {self.filename} ("
    for column, dtype in column_types.items():
        mysql_command += f"{column} {dtype}, "
    mysql_command = mysql_command.rstrip(', ') + ");"
    self.cur.execute(mysql_command)
    self.conn.commit()
    return mysql_command

  def copy_from_csv(self):
    copy_sql = f"COPY {self.filename} FROM STDIN WITH CSV HEADER"
    with open(self.filepath, 'r') as f:
      self.cur.copy_expert(copy_sql, f)
    self.conn.commit()


  def run(self):
    for filename in os.listdir('/app/build/customer/'):
      if not filename.endswith('.csv'):
          continue
      self.filepath = os.path.join('/app/build/customer/', filename)
      self.filename = filename.split('.')[0]
      self.df = pd.read_csv(self.filepath, sep=',')
      try:
          self.df.iloc[:, 0] = pd.to_datetime(self.df.iloc[:, 0], format='%Y-%m-%d %H:%M:%S', errors='coerce')
      except Exception as e:
          print(f"Error converting first column to datetime: {e}")
      success, column_types = self.get_column_types()
      if success:
          self.create_table(column_types)
          self.copy_from_csv()
      else:
          print(f"Error inferring column types: {column_types}")
    self.cur.close()
    self.conn.close()

def main():
  a = CSVToPostgres()
  a.run()