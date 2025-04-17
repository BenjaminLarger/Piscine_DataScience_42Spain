import pandas as pd
import psycopg2
import os
import time
from psycopg2.extras import execute_values

class CSVToPostgres:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    self.table_name1 = 'customers'
    self.table_name2 = 'items'
    self.has_int = True
    self.has_text = True
    
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

  def get_df_from_table(self, table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, self.conn)
    return df

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

  def insert_df_to_postgres(self, customers_df, table_name):
    data_tuples = [tuple(x) for x in customers_df.to_numpy()]
    print(f"data_tuples : {data_tuples}")
    columns = ', '.join(customers_df.columns)
    print(f"columns : {columns}")

    query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
    print(f"insert query : {query}")
    execute_values(self.cur, query, data_tuples)
    self.conn.commit()
    print(f"Inserted {len(data_tuples)} records into {table_name}.")

  def create_table(self, table_name, column_types):
    mysql_command = f"CREATE TABLE IF NOT EXISTS {table_name} ("
    for column, dtype in column_types.items():
        mysql_command += f"{column} {dtype}, "
    mysql_command = mysql_command.rstrip(', ') + ");"
    self.cur.execute(mysql_command)
    self.conn.commit()
    print(f"Table {table_name} created with columns: {', '.join(column_types.keys())}")
    return True

  def end_postgres_connection(self):
    self.cur.close()
    self.conn.close()
    print("PostgreSQL connection closed.")
  
  def run(self):
    customers_df = self.get_df_from_table(self.table_name1)
    items_df = self.get_df_from_table(self.table_name2)

    merged_df = pd.merge(customers_df, items_df, 
                     on=['product_id'],
                     how='outer')
    try:
      self.cur.execute(f"DROP TABLE {self.table_name1};")
      print(f"Deleted table {self.table_name1}.")
      self.conn.commit()
      success, column_types = self.get_column_types(merged_df)
      if success:
        self.create_table(self.table_name1, column_types)
        print(f"Table {self.table_name1} created successfully.")
        self.insert_df_to_postgres(merged_df, self.table_name1)
      print(f"Inserted records into {self.table_name1}.")
    except Exception as e:
      print(f"Error mainpulating postgres tables: {e}")
    self.end_postgres_connection()

def main():
  a = CSVToPostgres()
  a.run()

if __name__ == "__main__":
  main()


# WITH ranked AS (
#   SELECT 
#     *,
#     LAG(event_time) OVER (PARTITION BY user_id, user_session, event_type, product_id ORDER BY event_time) AS prev_event_time
#   FROM customers
# )
# SELECT *
# FROM ranked
# WHERE prev_event_time IS NOT NULL
#   AND EXTRACT(EPOCH FROM (event_time - prev_event_time)) <= 1;

# SELECT COUNT(*) FROM (
#   -- same CTE as above
#   WITH ranked AS (
#     SELECT 
#       *,
#       LAG(event_time) OVER (PARTITION BY user_id, user_session, event_type, product_id ORDER BY event_time) AS prev_event_time
#     FROM customers
#   )
#   SELECT *
#   FROM ranked
#   WHERE prev_event_time IS NOT NULL
#     AND EXTRACT(EPOCH FROM (event_time - prev_event_time)) <= 1
# ) AS duplicates;
