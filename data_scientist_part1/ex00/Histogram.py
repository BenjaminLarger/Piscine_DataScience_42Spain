import pandas as pd
import os
import psycopg2
import matplotlib.pyplot as plt
import numpy as np

class Histogram:
  def __init__(self):
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_test = "Test_knight.csv"
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


  def plot_test_distribution(self, df):
      numeric_cols = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]
      n = len(numeric_cols)

      if n == 0:
          print("No numeric columns to plot.")
          return
      fig, axes = plt.subplots(5, 6, figsize=(3 * 3, 2.5 * 3))  # Reduced subplot size
      axes = axes.flatten()

      for i, col in enumerate(numeric_cols):
          data = df[col].dropna()
          bin_edges = np.histogram_bin_edges(data, bins='doane')
          hist, edges = np.histogram(data, bins=bin_edges)

          bin_centers = 0.5 * (edges[1:] + edges[:-1])
          axes[i].bar(bin_centers, hist, width=((edges[1] - edges[0]) * 0.5), align='center', color="lightgreen", label="Knight")
          axes[i].set_title(col)
          axes[i].legend(fontsize=5)

      # Hide unused subplots
      for j in range(i + 1, len(axes)):
          fig.delaxes(axes[j])

      plt.tight_layout()
      plt.show()
  
  def plot_test_and_train_distribution(self, df_test, df_train):
      numeric_cols_test = [col for col in df_test.columns if pd.api.types.is_numeric_dtype(df_test[col])]
      numeric_cols_train = [col for col in df_train.columns if pd.api.types.is_numeric_dtype(df_train[col])]
      n_test = len(numeric_cols_test)
      n_train = len(numeric_cols_train)


      if n_test == 0 or n_train == 0 or n_test != n_train:
          print("No numeric columns to plot, or dataframes have different numeric columns.")
          return
      fig, axes = plt.subplots(5, 6, figsize=(3 * 3, 2.5 * 3))
      axes = axes.flatten()

      for i, col in enumerate(numeric_cols_test):
          data_test = df_test[col].dropna()
          data_train = df_train[col].dropna()
          bin_edges_test = np.histogram_bin_edges(data_test, bins='auto')
          bin_edges_train = np.histogram_bin_edges(data_train, bins='auto')
          hist_test, edges_test = np.histogram(data_test, bins=bin_edges_test)
          hist_train, edges_train = np.histogram(data_train, bins=bin_edges_train)
          bin_centers_test = 0.5 * (edges_test[1:] + edges_test[:-1])
          bin_centers_train = 0.5 * (edges_train[1:] + edges_train[:-1])
          axes[i].bar(bin_centers_test, hist_test, width=(edges_test[1] - edges_test[0]), align='center', color="lightcoral", label="Jedi")
          axes[i].bar(bin_centers_train, hist_train, width=(edges_train[1] - edges_train[0]), align='center', color="lightblue", alpha=0.5, label="Sith")
          axes[i].set_title(col)
          axes[i].legend(fontsize=5)

      # Hide unused subplots
      for j in range(i + 1, len(axes)):
          fig.delaxes(axes[j])

      plt.tight_layout()
      plt.show()

  def run(self):
    filepath_test = os.path.join(self.csv_dir, self.filename_test)
    filepath_train = os.path.join(self.csv_dir, self.filename_train)
    df_test = pd.read_csv(filepath_test, sep=',')
    df_train = pd.read_csv(filepath_train, sep=',')
    # Iterate through each column in the DataFrame
    self.plot_test_distribution(df_test)
    sith_df = df_train[df_train['knight'] == 'Sith']
    jedi_df = df_train[df_train['knight'] == 'Jedi']
    self.plot_test_and_train_distribution(sith_df, jedi_df)

def main():
  a = Histogram()
  a.run()

if __name__ == "__main__":
  main()