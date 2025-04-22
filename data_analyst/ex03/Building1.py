import psycopg2
import os
import matplotlib.pyplot as plt

def safe_int_conversion(value):
        try:
            return int(value)
        except ValueError:
            return None
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
    sql_spending_file_path = os.path.join(cur_dir, "Building1.sql")
    with open(sql_spending_file_path, "r") as sql_spending_file:
        sql_script = sql_spending_file.read()
    print("SQL code has been imported!")
    cursor.execute(sql_script)
    data = cursor.fetchall()
    print(f"data: {data}")
    conn.commit()
    conn.close()
    cursor.close()

    num_orders, num_users = zip(*data)
    # Aggregate the data
    # Total order between 0 and 10
    # Plot bar chart

    print(f"-----------")
    plt.bar(num_orders, num_users, color=['steelblue'])
    plt.gca().set_axisbelow(True)
    print(f"num_orders: {num_orders}")
    plt.xlabel('Monetary value in A')
    plt.ylabel('Customers')
    custom_labels = ['0', '10', '20', '30']
    plt.xticks(label=custom_labels, rotation=45)
    plt.tight_layout()
    plt.grid(color='w')
    plt.show()
    # Print variables type
    print("--------------------")

    
except Exception as e:
    print(f"Error: {str(e)}")