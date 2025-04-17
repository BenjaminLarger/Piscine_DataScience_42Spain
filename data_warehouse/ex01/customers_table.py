import pandas as pd
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
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
        port=5432
    )
    print(f"Connected to PostgreSQL database: {os.getenv('POSTGRES_DB', 'piscineds')}")
    return conn

  def pandas_to_postgres(self, dtype, column_name, df):
      if column_name == df.columns[0]:
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
      elif pd.api.types.is_integer_dtype(dtype):
          return 'BIGINT'
      elif pd.api.types.is_string_dtype(dtype) and self.has_text == True:
          self.has_text = False
          return 'VARCHAR(255)'
      else:
          return 'TEXT'

  def get_column_types(self, df):
      try:
          column_types = {}
          for column in df.columns:
              inferred_type = self.pandas_to_postgres(df[column].dtype, column, df)
              column_types[column] = inferred_type
          return (True, column_types)
      except pd.errors.ParserError as e:
          print(f"Error parsing CSV file: {self.filepath}")
          return (False, str(e))

  def create_table(self, column_types):
    mysql_command = f"CREATE TABLE IF NOT EXISTS {self.table_name} ("
    for column, dtype in column_types.items():
        mysql_command += f"{column} {dtype}, "
    mysql_command = mysql_command.rstrip(', ') + ");"
    self.cur.execute(mysql_command)
    self.conn.commit()
    print(f"Table {self.table_name} created with columns: {', '.join(column_types.keys())}")
    return True

  def insert_df_to_postgres(self, customers_df):
    data_tuples = [tuple(x) for x in customers_df.to_numpy()]
    columns = ', '.join(customers_df.columns)

    query = f"INSERT INTO {self.table_name} ({columns}) VALUES %s"
    print(f"insert query : {query}")
    execute_values(self.cur, query, data_tuples)
    print(f"Inserted {len(data_tuples)} records into {self.table_name}.")
  
  def get_df_from_table(self, table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, self.conn)
    return df

  def close_postgres_connection(self):
    self.cur.close()
    self.conn.close()
    print("PostgreSQL connection closed.")

  def get_tables_names(self):
    self.cur.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME LIKE 'data_202%_%';")
    tables = self.cur.fetchall()
    return [table[0] for table in tables]

  def run(self):
    data_tables = self.get_tables_names()
    table_created = False
    print(f"data_tables found: {data_tables}")
    for table in data_tables:
      df = self.get_df_from_table(table)
      print(f"Data from {table} has been read")
      success, column_types = self.get_column_types(df)
      if success and table_created is False:
        table = self.create_table(column_types)
        self.conn.commit()
        print(f"Table {self.table_name} created.")
      if table and success:
        self.insert_df_to_postgres(df)
        print(f"Data from {table} has been inserted into {self.table_name} table.")
      else:
        print(f"Error creating table {self.table_name}.")
        self.close_postgres_connection()
    self.conn.commit()
    self.close_postgres_connection()

def main():
  a = CSVToPostgres()
  a.run()

if __name__ == "__main__":
  main()