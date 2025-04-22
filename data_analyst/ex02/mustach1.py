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
    sql_spending_file_path = os.path.join(cur_dir, "mustach1.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    print("SQL code has been imported!")
    data = cursor.fetchall()
    conn.commit()
    conn.close()
    cursor.close()
    
    prices = [price for (_, price) in data]

    count = len(prices)
    mean_price = np.mean(prices)
    std = np.std(prices)
    min_price = np.min(prices)
    q1 = np.percentile(prices, 25)
    q2 = np.percentile(prices, 50)
    q3 = np.percentile(prices, 75)
    median_price = np.median(prices)
    max_price = np.max(prices)
    print("--------------------")

    print(f"count:    {count}")
    print(f"mean:    {mean_price}")
    print(f"std:    {std}")
    print(f"min    {min_price}")
    print(f"25%    {q1}")
    print(f"50%    {q2}")
    print(f"75%    {q3}")
    print(f"median:    {median_price}")
    print(f"max:    {max_price}")
  
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6))
    boxprops = dict(facecolor='dimgray', edgecolor='none')
    flierprops=dict(marker='D', markersize=8, markerfacecolor='dimgray', markeredgecolor='none')
    ax1.boxplot(prices, vert=False, widths=0.5, notch=True,
                        boxprops=boxprops,
                        flierprops=flierprops,
                        patch_artist=True,
                        showfliers=True,)

    ax1.set_yticks([])
    ax1.set_xlabel("Price")
    ax1.set_title("Full Box Plot")
    ax1.set_axisbelow(True)


    boxprops = dict(facecolor='green', edgecolor='black')
    medianprops = dict(linestyle='-', linewidth=1, color='black')
    ax2.boxplot(prices, vert=False, widths=0.5, notch=True,
                boxprops=boxprops, medianprops=medianprops, showfliers=False, # showfliers => without outliers
                patch_artist=True)
    ax2.set_yticks([])
    ax2.set_xlabel("Price")
    ax2.set_title("Interquartile range (IQR)")
    ax2.set_axisbelow(True)
    plt.tight_layout()
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()
    
except Exception as e:
    print(f"Error: {str(e)}")