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
    
  def connect_to_postgres(self):
    time.sleep(5)  # Wait for PostgreSQL to be ready
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "172.18.0.2"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "your_password"),
        port=5432
    )
    print(f"Connected to PostgreSQL database: {os.getenv('POSTGRES_DB', 'piscineds')}")
    return conn

  def get_df_from_table(self):
    query = f"SELECT * FROM {self.table_name};"
    df = pd.read_sql_query(query, self.conn)
    return df

  def remove_near_duplicate(self, df, key_columns, datetime_column):
    if isinstance(key_columns, pd.Index):
        print(f"key_columns is pd.Index : {key_columns}")
        key_columns = key_columns.tolist()

    print(f"key columns : {key_columns}, {all(col in df.columns for col in key_columns)}, {datetime_column} type : {type(datetime_column)}")
    df[datetime_column] = pd.to_datetime(df[datetime_column])
    df = df.sort_values(by=datetime_column)
    df['prev_datetime'] = df.groupby(key_columns)[datetime_column].shift()
    df['delta'] = (df[datetime_column] - df['prev_datetime']).dt.total_seconds()
    df = df[ (df['delta'].isna()) | (df['delta'] > 1) ]
    
    df = df.drop(columns=['prev_datetime', 'delta'])
    return df

  def insert_df_to_postgres(self, customers_df):
    data_tuples = [tuple(x) for x in customers_df.to_numpy()]
    columns = ', '.join(customers_df.columns)

    query = f"INSERT INTO {self.table_name} ({columns}) VALUES %s"
    print(f"insert query : {query}")
    print(f"data_tuples : {data_tuples}")
    execute_values(self.cur, query, data_tuples)
    self.conn.commit()

  def end_postgres_connection(self):
    self.cur.close()
    self.conn.close()
  
  def run(self):
    customers_df = self.get_df_from_table()
    datetime_column = customers_df.columns[0]

    customers_df = customers_df.drop_duplicates()
    key_columns = customers_df.columns[1:]
    customers_df = self.remove_near_duplicate(customers_df, key_columns, datetime_column)
    self.cur.execute("delete from customers")
    self.conn.commit()
    try:
      self.insert_df_to_postgres(customers_df)
      print(f"END: Removed duplicates and inserted {len(customers_df)} records into {self.table_name}.")
    except Exception as e:
      print(f"Error inserting data: {e}")
    self.end_postgres_connection()

def main():
  print("START: Removing duplicates from customers table.")
  a = CSVToPostgres()
  a.run()

if __name__ == "__main__":
  main()