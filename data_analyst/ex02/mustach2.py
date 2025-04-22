import psycopg2
import os
import matplotlib.pyplot as plt
import numpy as np

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
    sql_spending_file_path = os.path.join(cur_dir, "mustach2.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    conn.commit()
    print("SQL code has been imported!")
    user_total = cursor.fetchall()
    conn.close()
    cursor.close()


    # print the head of user_total
    print("user_total: ", user_total[:5])
    
    
    user_total_values = [float(row[1]) for row in user_total]
    print("user_total_values: ", user_total_values[:5])

    # Eliminate outliers
    q1 = np.percentile(user_total_values, 25)
    q3 = np.percentile(user_total_values, 75)
    iqr = q3 - q1
    lower_bound = q1 - 2.5 * iqr
    upper_bound = q3 + 2.5 * iqr
    user_total_values = [x for x in user_total_values if lower_bound <= x <= upper_bound]

    # Print statistics
    
    # plt.figure(figsize=(10, 6))
    # plt.boxplot(avg_cart_prices, vert=False, widths=0.5, notch=True,
    #             boxprops=dict(facecolor='steelblue', edgecolor='black'),
    #             flierprops=dict(marker='D', markersize=8, markerfacecolor='lightgray', markeredgecolor='none'),
    #             patch_artist=True, whis=0.2)
    # plt.xticks(np.arange(int(min(avg_cart_prices)), int(max(avg_cart_prices)) + 1, step=2))
    # plt.tight_layout()
    # plt.xlim(min(avg_cart_prices) - 1, max(avg_cart_prices) + 1)
    # plt.yticks([])
    # plt.show()


    plt.figure(figsize=(10, 6))
    plt.boxplot(user_total_values, vert=False, widths=0.5, notch=True,
                boxprops=dict(facecolor='steelblue', edgecolor='black'),
                flierprops=dict(marker='D', markersize=8, markerfacecolor='blue', markeredgecolor='none'),
                patch_artist=True, whis=0.2)
    custom_labels = ['28', '30', '32', '34', '36', '38', '40', '42']
    custom_positions = np.linspace(min(user_total_values), max(user_total_values), len(custom_labels))
    plt.xticks(custom_positions, labels=custom_labels, rotation=45)
    plt.tight_layout()
    plt.xlim(min(user_total_values) - 1, max(user_total_values) + 1)
    plt.yticks([])
    plt.gca().set_axisbelow(True)
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()
    
except Exception as e:
    print(f"Error: {str(e)}")