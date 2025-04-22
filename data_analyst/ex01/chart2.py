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
    sql_spending_file_path = os.path.join(cur_dir, "chart2.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    print("SQL code has been imported!")
    data = cursor.fetchall()
    conn.commit()
    conn.close()
    cursor.close()
    
    print(f"data: {data}")
    dates, total_revenue = zip(*data)
    # Plot a bar chart
    plt.figure(figsize=(7, 4))
    plt.bar(dates, total_revenue, color='steelblue', width=27)
    plt.xlabel("month")
    plt.ylabel("Total Sales in million of A")
    plt.title("Month")
    plt.xticks(dates, labels=["Oct", "Nov", "Dec", "Jan"], rotation=45)
    plt.tight_layout()
    plt.gca().set_axisbelow(True)
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()
except Exception as e:
    print(f"Error: {str(e)}")
    cursor.close()
    conn.close()