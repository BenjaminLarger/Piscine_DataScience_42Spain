import psycopg2
import os
import matplotlib.pyplot as plt

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
    sql_spending_file_path = os.path.join(cur_dir, "chart1.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    print("SQL code has been executed!")
    data = cursor.fetchall()
    conn.commit()
    conn.close()
    cursor.close()
    
    print(f"data: {data}")
    dates, num_customers = zip(*data)
    plt.figure(figsize=(10, 5))
    plt.plot(dates, num_customers, color='royalblue')
    plt.ylabel("Number of Customers Over Time")
    tick_indices = [0, len(dates)//3, 2*len(dates)//3, -1]
    tick_dates = [dates[i] for i in tick_indices]
    plt.xticks(tick_dates, labels=["Oct", "Nov", "Dec", "Jan"], rotation=45)

    plt.tight_layout()
    plt.gca().set_axisbelow(True)
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()

except Exception as e:
    print(f"Error: {str(e)}")
    cursor.close()
    conn.close()