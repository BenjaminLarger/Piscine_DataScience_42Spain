import pandas as pd
import re
import psycopg2
import os
import time
from psycopg2.extras import execute_values

class CSVToPostgres:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    self.has_int = True
    self.has_text = True
    self.table_name = 'customers'
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    self.csv_dir = os.path.join(cur_dir, 'customers')
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    
  def connect_to_postgres(self):
    time.sleep(5)  # Wait for PostgreSQL to be ready
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "172.18.0.2"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
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
              self.df[first_column] = pd.to_datetime(self.df[first_column], infer_datetime_format=True, errors='coerce')
              return True
          except Exception as e:
            print(f"Error converting first column to datetime: {e}")
            return False

  def pandas_to_postgres(self, dtype, column_name):
      if column_name == self.df.columns[0]:
          return 'TIMESTAMP'
      elif pd.api.types.is_integer_dtype(dtype) and self.has_int == True:
          self.has_int = False
          return 'INTEGER'
      elif pd.api.types.is_float_dtype(dtype):
          return 'FLOAT'
      elif pd.api.types.is_bool_dtype(dtype):
          return 'BOOLEAN'
      elif pd.api.types.is_datetime64_any_dtype(dtype):
          return 'TIMESTAMP'
      elif pd.api.types.is_timedelta64_dtype(dtype) or 'membership_duration' in column_name.lower():
        return 'INTERVAL'
      # Identify bigint
      elif pd.api.types.is_integer_dtype(dtype):
          return 'BIGINT'
      elif pd.api.types.is_string_dtype(dtype) and self.has_text == True:
          self.has_text = False
          return 'VARCHAR(255)'
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
    mysql_command = f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
    for column, dtype in column_types.items():
        mysql_command += f"{column} {dtype}, "
    mysql_command = mysql_command.rstrip(', ') + ");"
    self.cur.execute(mysql_command)
    self.conn.commit()
    return True

  def insert_df_to_postgres(self, customers_df):
    data_tuples = [tuple(x) for x in customers_df.to_numpy()]
    columns = ', '.join(customers_df.columns)

    query = f"INSERT INTO {self.table_name} ({columns}) VALUES %s"
    print(f"insert query : {query}")
    print(f"data_tuples : {data_tuples}")
    execute_values(self.cur, query, data_tuples)

  def run(self):
    df_list = []
    table = None
    for filename in os.listdir(self.csv_dir):
      self.filepath = os.path.join(self.csv_dir, filename)
      self.filename = filename.split('.')[0]
      self.df = pd.read_csv(self.filepath, sep=',')
      print(f"{filename} has been read")
      if not filename.endswith('.csv') or self.check_first_column_is_datetime() == False:
          continue
      success, column_types = self.get_column_types()
      if success and table is None:
        df_list.append(self.df)
        table = self.create_table(column_types)
      else:
        continue
    if df_list:
      customers_df = pd.concat(df_list, ignore_index=True)
      self.insert_df_to_postgres(customers_df)
      self.conn.commit()
      print(f"END: Data from {self.filename} has been inserted into {self.table_name} table.")
    self.cur.close()
    self.conn.close()

def main():
  a = CSVToPostgres()
  a.run()

if __name__ == "__main__":
  main()