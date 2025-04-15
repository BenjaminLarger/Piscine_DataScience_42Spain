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

  def check_first_column_is_datetime(self):
      first_column = self.df.columns[0]
      if pd.api.types.is_datetime64_any_dtype(self.df[first_column]):
          return True
      else:
          # Try to convert the first column to datetime
          try:
              print(f"Converting first column {first_column} to datetime.")
              self.df[first_column] = pd.to_datetime(self.df[first_column], format='%Y-%m-%d %H:%M:%S')
              return True
          except Exception as e:
            print(f"Error converting first column to datetime: {e}")
            return False

  def pandas_to_postgres(self, dtype, column_name):
      if column_name == self.df.columns[0]:
          return 'TIMESTAMP'
      if pd.api.types.is_integer_dtype(dtype) or dtype == 'int64':
          return 'INTEGER'
      elif pd.api.types.is_float_dtype(dtype) or dtype == 'float64':
          return 'FLOAT'
      elif pd.api.types.is_bool_dtype(dtype) or dtype == 'bool':
          return 'BOOLEAN'
      elif pd.api.types.is_timedelta64_dtype(dtype):
          return 'INTERVAL'
      elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
      else:
          return 'TEXT'

  def get_column_types(self, has_headers=True):
      try:
          column_types = {}
          for column in self.df.columns:
              inferred_type = self.pandas_to_postgres(self.df[column].dtype, column)
              column_types[column] = inferred_type
          return (True, column_types)
      except pd.errors.ParserError:
          print(f"Error parsing CSV file: {self.filepath}")
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
      self.filepath = os.path.join('/app/build/customer/', filename)
      self.filename = filename.split('.')[0]
      self.df = pd.read_csv(self.filepath, sep=',')
      if not filename.endswith('.csv') or self.check_first_column_is_datetime() == False:
          continue
      success, column_types = self.get_column_types()
      if success:
        try:
          self.create_table(column_types)
          self.copy_from_csv()
        except Exception as e:
          print(f"Error creating table or copying data: {e}")
          pass
      else:
          print(f"Error inferring column types: {column_types}")
    self.cur.close()
    self.conn.close()

def main():
  a = CSVToPostgres()
  a.run()