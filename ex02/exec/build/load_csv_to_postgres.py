import pandas as pd
import re
import psycopg2
import os
import time
from io import StringIO

class CSVToPostgres:
  def __init__(self, filepath):
    self.filepath = filepath
    self.filename = filepath.split('/')[-1].split('.')[0]
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    self.df = pd.read_csv(self.filepath, sep=',', header=0)

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
      if column_name == self.df.columns[0]:  # Force DATETIME for first column
          return 'TIMESTAMP'
      if pd.api.types.is_integer_dtype(dtype):
          return 'INTEGER'
      elif pd.api.types.is_float_dtype(dtype):
          return 'FLOAT'
      elif pd.api.types.is_bool_dtype(dtype):
          return 'BOOLEAN'
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
              print(f"Column: {column}, Inferred type: {inferred_type}")

          return (True, column_types)
      except pd.errors.ParserError:
          return (False, str(e))

  def create_table(self, column_types):
    mysql_command = f"CREATE TABLE IF NOT EXISTS {self.filename} ("
    for column, dtype in column_types.items():
        mysql_command += f"{column} {dtype}, "
    mysql_command = mysql_command.rstrip(', ') + ");"
    print(mysql_command)
    self.cur.execute(mysql_command)
    self.conn.commit()
    return mysql_command

  def fill_table_from_df(self, column_types):
    for _, row in self.df.iterrows():
      placeholders = ','.join(['%s'] * len(row))
      insert_sql = f"INSERT INTO {self.filename} VALUES ({placeholders})"
      print(f"Insert SQL: {insert_sql}")
    #   self.cur.execute(insert_sql, tuple(row))
    # self.conn.commit()

  def copy_from_csv(self):
    buffer = StringIO()
    self.df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    print(f"Buffer content: {buffer.getvalue()}")

    sql = f"COPY {self.filename} FROM STDIN WITH CSV"
    self.cur.copy_expert(sql, buffer)
    self.conn.commit()



  def run(self):
      success, column_types = self.get_column_types()
      if success:
          print("Column types inferred successfully:")
          for column, dtype in column_types.items():
              print(f"{column}: {dtype}")
          self.create_table(column_types)
          # self.fill_table_from_df(column_types)
          self.copy_from_csv()
          
      else:
          print(f"Error inferring column types: {column_types}")
      self.cur.close()
      self.conn.close()

def main():
  filepath = '/app/build/customer/data_2022_oct.csv'
  a = CSVToPostgres(filepath)
  a.run()