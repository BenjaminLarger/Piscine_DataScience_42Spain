import psycopg2
import os
import matplotlib.pyplot as plt

try:
    cur_dir = os.path.dirname(os.path.abspath(__file__))
    sql_file_path = os.path.join(cur_dir, "pie.sql")
    with open(sql_file_path, "r") as sql_file:
        sql_script = sql_file.read()
    print("SQL code has been imported!")
    conn = psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        dbname=os.getenv("POSTGRES_DB", "piscineds"),
        user=os.getenv("POSTGRES_USER", "blarger"),
        password=os.getenv("POSTGRES_PASSWORD", "mysecretpassword"),
    )
    print("Connected to postgres!")
    cursor = conn.cursor()
    cursor.execute(sql_script)
    data = cursor.fetchall()
    print("SQL script executed successfully!")
    print(f"Data fetched: {data}")
    plt.figure(figsize=(8, 8))
    # Data fetched: [('cart', 4619639), ('purchase', 1045014), ('remove_from_cart', 3167270), ('view', 7704235)]
    plt.pie([row[1] for row in data], labels=[row[0] for row in data], autopct='%1.1f%%', startangle=140)
    plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    plt.title('Distribution of Events')
    plt.show()
    conn.commit()
except Exception as e:
    print(f"Error: {str(e)}")

finally:
    cursor.close()
    conn.close()