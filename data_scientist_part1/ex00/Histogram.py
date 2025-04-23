import pandas as pd
import os
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

class Histogram:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    self.csv_dir = os.path.join(cur_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename1 = "Test_knight.csv"
    self.filename2 = "Test_knight2.csv"

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

  def plot_distribution(self, df):
      numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
      n = len(numeric_cols)

      if n == 0:
          print("No numeric columns to plot.")
          return
      fig, axes = plt.subplots(5, 6, figsize=(5 * 5, 4 * 6))
      axes = axes.flatten()

      for i, col in enumerate(numeric_cols):
          # cast as int if dispersion

          data = df[col].dropna()
          bin_edges = np.histogram_bin_edges(data, bins='auto')
          hist, edges = np.histogram(data, bins=bin_edges)

          bin_centers = 0.5 * (edges[1:] + edges[:-1])
          axes[i].bar(bin_centers, hist, width=(edges[1] - edges[0]), align='center', color="lightgreen", label="Knight")  # changed from plot to bar
          axes[i].set_title(col)
          axes[i].legend()

      # Hide unused subplots
      for j in range(i + 1, len(axes)):
          fig.delaxes(axes[j])

      plt.tight_layout()
      plt.show()
  


  def run(self):
    filepath = os.path.join(self.csv_dir, self.filename1)
    df = pd.read_csv(filepath, sep=',')
    # Iterate through each column in the DataFrame
    self.plot_distribution(df)
    self.close_connection()

def main():
  a = Histogram()
  a.run()

if __name__ == "__main__":
  main()