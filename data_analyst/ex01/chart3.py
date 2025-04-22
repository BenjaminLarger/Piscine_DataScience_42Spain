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
    sql_spending_file_path = os.path.join(cur_dir, "chart3.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    cursor.execute(sql_script)
    print("SQL code has been imported!")
    data = cursor.fetchall()
    conn.commit()
    conn.close()
    cursor.close()
    
    print(f"data: {data}")
    dates, total_salesm, num_customers, avg_spend_per_customer= zip(*data)
    # Plot a bar chart
    plt.figure(figsize=(10, 5))
    plt.plot(dates, avg_spend_per_customer, color='w', label='Avg Spend per Customer')  # Change line color to light blue
    plt.fill_between(dates, avg_spend_per_customer, facecolor='steelblue', alpha=0.3, edgecolor="b")  # Fill the area under the line
    plt.xlabel("Date")
    plt.ylabel("Average Spend / Customer in A")
    plt.title("Average Spend / Customer Over Time")
    plt.grid(False)
    # Force x-axis to have specific labels
    custom_labels = ["Oct", "Nov", "Dec", "Jan"]
    custom_positions = [dates[0], dates[len(dates)//3], dates[2*len(dates)//3], dates[-1]]  # Select positions for labels
    plt.xticks(custom_positions, labels=custom_labels, rotation=45)
    plt.tight_layout()
    plt.gca().set_axisbelow(True)
    plt.grid(color='w')
    plt.gca().set_facecolor('lightgrey')
    plt.show()
except Exception as e:
    print(f"Error: {str(e)}")
    cursor.close()
    conn.close()