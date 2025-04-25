import pandas as pd
import os
import psycopg2
import numpy as np
import matplotlib.pyplot as plt
from scipy.stats import pointbiserialr
import seaborn as sb
from sklearn import preprocessing

class ConfusionMatrix:
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
    # return 2 * (x - x.min()) / (x.max() - x.min()) - 1
    x = df.values
    min_max_scaler = preprocessing.MinMaxScaler()
    x_scaled = min_max_scaler.fit_transform(x)
    df_normalized = pd.DataFrame(x_scaled, columns=df.columns)
    return df_normalized

  
  def clean_txt_file(self, filepath):
    with open(filepath, "r") as file:
        content = file.read()
    array = content.split()
    # Replace Jedi with 1 and Sith with 0
    array = [1 if x == 'Jedi' else 0 for x in array]
    print(f"clean_txt_file Array: {array}")
    return array

  def compute_correlations(self, df, target='knight'):
        correlations = {}
        for col in df.columns:
            if pd.api.types.is_numeric_dtype(df[col]):
                corr, _ = pointbiserialr(df[col], df[target])
                correlations[col] = corr
        return sorted(correlations.items(), key=lambda x: abs(x[1]), reverse=True)

  def plot_correlation_heatmap(self, df):
    corr = df.corr()
    sb.heatmap(corr, annot=False, cmap='coolwarm')
    plt.title("Correlation Heatmap")
    plt.show()
    print("HEOEMAP")

  def run(self):
    df_train = pd.read_csv(self.filepath_train, sep=',')

    try:
        df_train['knight'] = df_train['knight'].map({'Sith': 1, 'Jedi': 0})
        df_train_norm = self.normalize_df(df_train)
        print(f"df_train.head(): {df_train.head()}")
        print(f"corr: {self.compute_correlations(df_train_norm)}")
        self.plot_correlation_heatmap(df_train_norm)
    except Exception as e:
      print(f"Error: {e}")

def main():
  a = ConfusionMatrix()
  a.run()

if __name__ == "__main__":
  main()