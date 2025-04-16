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
    
  def connect_to_postgres(self):
    time.sleep(5)  # Wait for PostgreSQL to be ready
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "postgres"),
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

  def insert_df_to_postgres(self, customers_df, table_name):
    data_tuples = [tuple(x) for x in customers_df.to_numpy()]
    columns = ', '.join(customers_df.columns)

    query = f"INSERT INTO {table_name} ({columns}) VALUES %s"
    execute_values(self.cur, query, data_tuples)
    self.conn.commit()
    print(f"Inserted {len(data_tuples)} records into {table_name}.")

  def end_postgres_connection(self):
    self.cur.close()
    self.conn.close()
    print("PostgreSQL connection closed.")
  
  def run(self):
    customers_df = self.get_df_from_table(self.table_name1)
    items_df = self.get_df_from_table(self.table_name2)

    merged_df = pd.merge(customers_df, items_df, 
                     on=['user_id', 'user_session'],
                     how='outer')
    try:
      self.cur.execute(f"TRUNCATE TABLE {self.table_name1};")
      self.conn.commit()
      print(f"Deleted all records from {self.table_name1}.")
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
