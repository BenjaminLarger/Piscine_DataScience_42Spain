import pandas as pd
import os
import psycopg2
from sklearn.model_selection import train_test_split

class Normalization:
    def __init__(self):
        self.conn = self.connect_to_postgres()
        self.cur = self.conn.cursor()
        cur_dir = os.path.dirname(os.path.abspath(__file__))
        parent_dir = os.path.dirname(cur_dir)
        self.csv_dir = os.path.join(parent_dir, "sources/")
        self.table = "items"
        if not os.path.exists(self.csv_dir):
            os.makedirs(self.csv_dir)
        self.filename_test = "Test_knight.csv"
        self.filename_train = "Train_knight.csv"
        self.strong_features = ["Empowered", "Prescience"]
        self.weak_features = ["Midi-chlorien", "Push"]

    def connect_to_postgres(self):
        conn = psycopg2.connect(
            host=os.getenv("PGHOST", "localhost"),
            dbname=os.getenv("POSTGRES_DB", "piscineds"),
            user=os.getenv("POSTGRES_USER", "blarger"),
            password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
            port=5432,
        )
        return conn

    def close_connection(self):
        if self.cur:
            self.cur.close()
        if self.conn:
            self.conn.close()
        print("PostgreSQL connection closed.")

    def normalize_df(self, df):
        return (df - df.min()) / (df.max() - df.min())

    def split_training_data(self, df):
        # Stratify ensure the proportions of the classes are maintained in both sets    
        train_df, val_df = train_test_split(
            df, test_size=0.2, random_state=42, stratify=df["knight"]
        )
        return train_df, val_df

    def run(self):
        filepath_test = os.path.join(self.csv_dir, self.filename_train)
        df_train = pd.read_csv(filepath_test, sep=",")
        df_train["knight"] = df_train["knight"].map({"Sith": 0, "Jedi": 1})
        # df_train = self.normalize_df(df_train)
        print(f"df_train.head(): {df_train.head()}")
        train_df, val_df = self.split_training_data(df_train)
        # Save to csv files
        train_df.to_csv(os.path.join(self.csv_dir, "Training_knight.csv"), index=False)
        val_df.to_csv(os.path.join(self.csv_dir, "Validation_knight.csv"), index=False)

def main():
    a = Normalization()
    a.run()


if __name__ == "__main__":
    main()
