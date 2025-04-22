import psycopg2
import os
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from datetime import datetime
import numpy as np
from sklearn.decomposition import PCA
import pandas as pd
class Clustering:
    
    def __init__(self):
      self.connect_to_db()
      cur_dir = os.path.dirname(os.path.abspath(__file__))
      sql_spending_file_path = os.path.join(cur_dir, "Clustering.sql")
      with open(sql_spending_file_path, "r") as sql_spending_file:
        self.sql_script = sql_spending_file.read()

    def connect_to_db(self):
        print("Connecting to PostgreSQL...")
        self.conn = psycopg2.connect(
              host=os.getenv("PGHOST", "localhost"),
              dbname=os.getenv("POSTGRES_DB", "piscineds"),
              user=os.getenv("POSTGRES_USER", "blarger"),
              password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
          )
        self.cursor = self.conn.cursor()
        print("Connected to PostgreSQL!")

        
    def close_db(self):
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()

    def get_last_order(self):
        self.cursor.execute("SELECT MAX(event_time) FROM customers WHERE event_type = 'purchase';")
        last_order = self.cursor.fetchone()[0]
        last_order = last_order.strftime("%Y-%m-%d %H:%M:%S")
        last_order = datetime.strptime(last_order, "%Y-%m-%d %H:%M:%S")
        return last_order

    def plot_clusters(self, df_X):
        cluster_0 = df_X[df_X['cluster'] == 0]
        cluster_1 = df_X[df_X['cluster'] == 1]
        cluster_2 = df_X[df_X['cluster'] == 2]
        cluster_3 = df_X[df_X['cluster'] == 3]
        cluster_4 = df_X[df_X['cluster'] == 4]
        
        plt.scatter(cluster_0['X'], cluster_0['Y'], s=100, c='red', label='New Customer')
        plt.scatter(cluster_1['X'], cluster_1['Y'], s=100, c='blue', label='Gold')
        plt.scatter(cluster_2['X'], cluster_2['Y'], s=100, c='green', label='Loyal Customer')
        plt.scatter(cluster_3['X'], cluster_3['Y'], s=100, c='yellow', label='Platinum')
        plt.scatter(cluster_4['X'], cluster_4['Y'], s=100, c='purple', label='Inactive')
        plt.legend()
        plt.title('K-means result vizualization with 2 PCA components')
        plt.xlabel('PCA Component 1')
        plt.ylabel('PCA Component 2')
        plt.gca().set_facecolor('lightgrey')
        plt.show()
    
    def plot_groups(self, df_X):
        """
        Plot the number of customers in each cluster
        """
        cluster_0 = df_X[df_X['cluster'] == 0]
        cluster_1 = df_X[df_X['cluster'] == 1]
        cluster_2 = df_X[df_X['cluster'] == 2]
        cluster_3 = df_X[df_X['cluster'] == 3]
        cluster_4 = df_X[df_X['cluster'] == 4]

        plt.figure(figsize=(10, 6))
        plt.bar(['New Customer', 'Gold', 'Loyal Customer', 'Platinum', 'Inactive'], 
                [len(cluster_0), len(cluster_1), len(cluster_2), len(cluster_3), len(cluster_4)],
                color=['red', 'blue', 'green', 'yellow', 'purple'])
        plt.title('Number of customers in each cluster')
        plt.xlabel('Cluster')
        plt.ylabel('Number of customers')
        plt.gca().set_facecolor('lightgrey')
        plt.show()


    def eliminate_outliers(self, data):
        # Assuming data is a list of tuples
        data = np.array(data)
        for i in range(data.shape[1]):
            q1 = np.percentile(data[:, i], 25)
            q3 = np.percentile(data[:, i], 75)
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            data = data[(data[:, i] >= lower_bound) & (data[:, i] <= upper_bound)]
        return data

    def clean_data(self, data):
        print("Data fetched from the database!")
        print("Data: ", data[:5])
        data = self.eliminate_outliers(data)
        last_order = self.get_last_order()
        self.close_db()

        data_with_recency = [
            (
                user_id,
                num_orders,
                total_spent,
                (last_order - last_order_date).days,
                (last_order - first_order_date).days 
            )
            for user_id, num_orders, total_spent, last_order_date, first_order_date in data
        ]

        df_X = pd.DataFrame(
            [
                [recency, num_orders, total_spent]
                for _, num_orders, total_spent, recency, _ in data_with_recency
            ],
            columns=['recency', 'num_orders', 'total_spent']
        )
        return df_X
    
    def plot_3d_clusters(self, df):
      """
      3D plot of customer clusters based on recency, num_orders, and total_spent.

      Parameters:
          df (pd.DataFrame): Must contain columns 'recency', 'num_orders', 'total_spent', 'cluster'
      """
      fig = plt.figure(figsize=(12, 8))
      ax = fig.add_subplot(111, projection='3d')
      
      # Plot each cluster with a different color
      for cluster_id in df['cluster'].unique():
          cluster_data = df[df['cluster'] == cluster_id]
          ax.scatter(
              cluster_data['recency'],
              cluster_data['num_orders'],
              cluster_data['total_spent'],
              label=f'Cluster {cluster_id}',
              s=60,
              alpha=0.7
          )

      ax.set_xlabel('Recency')
      ax.set_ylabel('Number of Orders')
      ax.set_zlabel('Total Spent')
      ax.set_title('Customer Clusters: Recency vs Orders vs Spend')
      ax.legend(title="Cluster")
      plt.tight_layout()
      plt.grid(color='w')
      plt.gca().set_facecolor('lightgrey')
      plt.show()
      
    def run(self):
        print("Running clustering...")
        self.cursor.execute(self.sql_script)
        self.conn.commit()
        print("SQL code has been imported!")
        
        print("Data fetched from the database!")
        
        df_X = self.clean_data(self.cursor.fetchall())

        print("df_X: ", df_X[:5])
        # Recency column represents the number of days since the last order
        
        kmeans = KMeans(n_clusters=5, random_state=0, init='k-means++', max_iter=300)
        kmeans.fit(df_X)

        df_X['cluster'] = kmeans.labels_
        print(df_X.head())
        print("X: ", df_X[:5])
        
        pca = PCA(2)
        pca_res = pca.fit_transform(df_X)
        df_X['X'] = pca_res[:, 0]
        df_X['Y'] = pca_res[:, 1]
        print("PCA head: ", df_X[:5])

        self.plot_groups(df_X)
        self.plot_clusters(df_X)
        self.plot_3d_clusters(df_X)


if __name__ == "__main__":
    clustering = Clustering()
    clustering.run()