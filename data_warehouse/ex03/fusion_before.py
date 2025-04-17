import pandas as pd
import os
from sqlalchemy import (
   Table,
   Column,
   MetaData,
   Integer,
   Float,
   String,
   Boolean,
   DateTime,
   Interval,
   BigInteger,
   Text,
   create_engine
   )
class CSVToPostgres:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.table_name1 = 'customers'
    self.table_name2 = 'items'
    self.has_int = True
    self.has_text = True
    self.type_map = {
        'INTEGER': Integer,
        'FLOAT': Float,
        'BOOLEAN': Boolean,
        'TIMESTAMP': DateTime,
        'INTERVAL': Interval,
        'BIGINT': BigInteger,
        'VARCHAR(255)': String(255),
        'TEXT': Text
    }
    
  def connect_to_postgres(self):
    db_url = (
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER', 'blarger')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'mysecretpassword')}@"
        f"{os.getenv('PGHOST', 'localhost')}:5432/"
        f"{os.getenv('POSTGRES_DB', 'piscineds')}"
    )
    engine = create_engine(db_url)
    print(f"Connected to PostgreSQL database: {os.getenv('POSTGRES_DB', 'piscineds')}")
    return engine

  def get_df_from_table(self, table_name):
    query = f"SELECT * FROM {table_name};"
    df = pd.read_sql_query(query, self.conn)
    return df
  
  # def get_df_from_table(self, table_name, chunksize=10_000):
  #   query = f"SELECT * FROM {table_name};"
  #   chunks = pd.read_sql_query(query, self.conn, chunksize=chunksize)
  #   df_list = []
  #   for chunk in chunks:
  #       df_list.append(chunk)
  #   df = pd.concat(df_list, ignore_index=True)
  #   return df

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
    # Use pandas to_sql with SQLAlchemy engine
    customers_df.to_sql(table_name, self.conn, if_exists='append', index=False)
    print(f"Inserted {len(customers_df)} records into {table_name}.")

  def create_table(self, table_name, column_types):
    metadata = MetaData()
    columns = []
    for column, dtype in column_types.items():
        col_type = self.type_map.get(dtype, Text)
        columns.append(Column(column, col_type))
    self.table = Table(table_name, metadata, *columns)
    metadata.create_all(self.conn)
    print(f"Table {table_name} created with columns: {', '.join(column_types.keys())}")
    return True

  def end_postgres_connection(self):
    self.conn.dispose()
    print("SQLAlchemy engine disposed and connections closed.")
  
  def run(self):
    print("Starting the process...")
    customers_df = self.get_df_from_table(self.table_name1)
    items_df = self.get_df_from_table(self.table_name2)
    print(f"customers_df and items_df : {customers_df.shape} and {items_df.shape} have been loaded.")
    merged_df = pd.merge(customers_df, items_df, 
                     on=['product_id'],
                     how='outer')
    try:
      with self.engine.connect() as conn:
            conn.execute(f"DROP TABLE IF EXISTS {self.table_name1};")
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
