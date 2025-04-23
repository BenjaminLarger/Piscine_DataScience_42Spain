from scipy.stats import pointbiserialr
import pandas as pd
import os
import psycopg2

class Histogram:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    print(f"Parent directory: {parent_dir}")
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_train = "Train_knight.csv"

  def connect_to_postgres(self):
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
        port=5432
    )
    return conn
  
  def close_connection(self):
    if self.cur:
        self.cur.close()
    if self.conn:
        self.conn.close()
    print("PostgreSQL connection closed.")
  
  def compute_correlations(self, df, target='knight'):
      correlations = {}
      for col in df.columns:
          if pd.api.types.is_numeric_dtype(df[col]):
              corr, _ = pointbiserialr(df[col], df[target])
              correlations[col] = corr
      return sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)

  def print_correlation(self, sorted_correlation):
    print("Correlation with target variable 'knight':")
    for col, corr in sorted_correlation:
        print(f"{col}: {corr:.6f}")

  def run(self):
    filepath2 = os.path.join(self.csv_dir, self.filename_train)
    df_train = pd.read_csv(filepath2, sep=',')
    df_train['knight'] = df_train['knight'].map({'Sith': 0, 'Jedi': 1})
    sorted_correlation = self.compute_correlations(df_train)
    self.print_correlation(sorted_correlation)

    self.close_connection()

def main():
  a = Histogram()
  a.run()

if __name__ == "__main__":
  main()