import psycopg2
import os
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
import numpy as np

def cluster_data_points(X):
    k_range = range(1, 5)

    for k in k_range:
        kmeans = KMeans(n_clusters=k, random_state=42)
        y_kmeans = kmeans.fit_predict(X)

        # Plot as a 1D scatter plot
        plt.scatter(X[:, 0], [0] * len(X), c=y_kmeans, cmap='viridis', marker='o', edgecolor='k')
        plt.scatter(
            kmeans.cluster_centers_[:, 0], [0] * len(kmeans.cluster_centers_),
            s=300, c='red', label='Centroids', marker='X', edgecolors='k'
        )
        plt.title(f'K-Means Clustering (k={k})')
        plt.xlabel("Purchases")
        plt.yticks([])  # Remove y-axis ticks for clarity
        plt.legend()
        plt.grid(color='w')
        plt.gca().set_facecolor('lightgrey')
        plt.show()
try:
    print("Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
    )
    print("Connected to PostgreSQL!")
    cursor = conn.cursor()
    
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    sql_spending_file_path = os.path.join(cur_dir, "elbow.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    conn.commit()
    print("SQL code has been imported!")
    data = cursor.fetchall()
    conn.close()
    cursor.close()


    # print the head of data
    print("data: ", data[:5])
    X = np.array([[purchases] for _, purchases in data])
    print("X: ", X[:5])
    cluster_data_points(X)
    
    inertia = []
    K = range(1, 11)

    for k in K:
        kmeans = KMeans(n_clusters=k, random_state=42)
        kmeans.fit(data)
        inertia.append(kmeans.inertia_)
        print(f"for k={k}, inertia = {kmeans.inertia_}")
    # Plot the elbow method
    
    plt.figure(figsize=(10, 6))
    plt.plot(K, inertia, 'bo-')
    plt.axvline(x=5, color='red', linestyle='--', label='Optimal k (5)')
    plt.xlabel('Number of clusters (k)')
    plt.ylabel('Inertia')
    plt.title('Elbow Method for Optimal k')
    plt.xticks(K)
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()

except Exception as e:
    print(f"Error: {str(e)}")