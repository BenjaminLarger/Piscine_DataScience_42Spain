import pandas as pd
import os
import psycopg2
import matplotlib.pyplot as plt


class Normalization:
    def __init__(self):
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

    def plot_clusters_feature(self, df, features):
        df_jedi = df[(df["knight"] == 1)]
        xpoints_jedi = df_jedi[features[0]].values
        ypoints_jedi = df_jedi[features[1]].values
        plt.plot(xpoints_jedi, ypoints_jedi, "o", label="Jedi", color="lightblue")

        df_sith = df[(df["knight"] == 0)]
        xpoints_sith = df_sith[features[0]].values
        ypoints_sith = df_sith[features[1]].values
        plt.plot(xpoints_sith, ypoints_sith, "o", label="Sith", color="lightcoral")

        plt.xlabel(features[0])
        plt.ylabel(features[1])
        plt.title("Clusters of Jedi and Sith")
        plt.legend()
        plt.show()

    def plot_undifferentiated_knight(self, df, features):
        xpoints = df[features[0]].values
        ypoints = df[features[1]].values
        plt.plot(xpoints, ypoints, "o", label="Knight", color="lightgreen")
        plt.xlabel(features[0])
        plt.ylabel(features[1])
        plt.title("Undifferentiated Clusters")
        plt.legend()
        plt.show()

    def normalize_df(self, df):
        return (df - df.min()) / (df.max() - df.min())

    def run(self):
        filepath_test = os.path.join(self.csv_dir, self.filename_test)
        filepath_train = os.path.join(self.csv_dir, self.filename_train)
        df_test = pd.read_csv(filepath_test, sep=",")
        df_train = pd.read_csv(filepath_train, sep=",")
        df_train["knight"] = df_train["knight"].map({"Sith": 0, "Jedi": 1})
        print(f"df_train.head(): {df_train.head()}")
        print(f"df_test.head(): {df_test.head()}")
        df_train = self.normalize_df(df_train)
        df_test = self.normalize_df(df_test)
        print(f"df_train.head(): {df_train.head()}")
        print(f"df_test.head(): {df_test.head()}")
        self.plot_clusters_feature(df_train, self.strong_features)


def main():
    a = Normalization()
    a.run()


if __name__ == "__main__":
    main()
