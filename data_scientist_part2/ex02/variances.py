import pandas as pd
import os
import psycopg2
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pointbiserialr
import seaborn as sb
from sklearn import preprocessing

class Variances:
  def __init__(self):
    self.conn = self.connect_to_postgres()
    self.cur = self.conn.cursor()
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(cur_dir)
    self.csv_dir = os.path.join(parent_dir, 'sources/')
    self.table = 'items'
    if not os.path.exists(self.csv_dir):
        os.makedirs(self.csv_dir)
    self.filename_test = "Test_knight.csv"
    self.filename_train = "Train_knight.csv"
    self.filename_prediction = "predictions.txt"
    self.filename_truth = "truth.txt"
    self.strong_features = ['Agility', 'Strength']
    self.weak_features = ['Grasping', 'Burst']
    self.filename_test = os.path.join(self.csv_dir, self.filename_test)
    self.filepath_train = os.path.join(self.csv_dir, self.filename_train)
    self.filepath_pred = os.path.join(self.csv_dir, self.filename_prediction)
    self.filepath_truth = os.path.join(self.csv_dir, self.filename_truth)

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


  def normalize_df(self, df):
    x = df.values
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    df_normalized = pd.DataFrame(x_scaled, columns=df.columns)
    return df_normalized

  
  def clean_txt_file(self, filepath):
    with open(filepath, "r") as file:
        content = file.read()
    array = content.split()
    array = [1 if x == 'Jedi' else 0 for x in array]
    print(f"clean_txt_file Array: {array}")
    return array

  def select_features(self, sorted_variances, total_variance):
    explained = 0
    num_components = 0
    for var in sorted_variances:
         explained += var
         num_components += 1
         print(f"Explained variance: {explained / total_variance:.2%}")
         if explained / total_variance >= 0.9:
             break
    return num_components

  def run(self):
    df_train = pd.read_csv(self.filepath_train, sep=',')

    try:
        df_train['knight'] = df_train['knight'].map({'Sith': 1, 'Jedi': 0})
        normalized_df_train = self.normalize_df(df_train)
        variances = normalized_df_train.var()
        sorted_variances = variances.sort_values(ascending=False)
        print(f"sorted_variances: {sorted_variances}")
        total_variance = sorted_variances.sum()
        num_components = self.select_features(sorted_variances, total_variance)
        print(f"Number of components to explain 90% variance: {num_components} ou of {len(sorted_variances)}")
    except Exception as e:
      print(f"Error: {e}")

def main():
  a = Variances()
  a.run()

if __name__ == "__main__":
  main()