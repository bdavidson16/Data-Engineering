import mysql.connector
import psycopg2

# Connect to MySQL
connection = mysql.connector.connect(user='root', password='yuD7tZFl5VwNkZwCDSqvCqa9', host='172.21.186.179', database='sales')
cursor = connection.cursor(dictionary=True)

# Connect to PostgreSQL
dsn_hostname = '172.21.83.205'
dsn_user = 'postgres'
dsn_pwd = 'OsmfFQS6uBOCB50lDnYnXitl'
dsn_port = "5432"
dsn_database = "postgres"

# Create PostgreSQL connection
conn = psycopg2.connect(
    database=dsn_database,
    user=dsn_user,
    password=dsn_pwd,
    host=dsn_hostname,
    port=dsn_port
)

# Create a cursor object for PostgreSQL
cur = conn.cursor()

# Function to get the last rowid from PostgreSQL
def get_last_rowid():
    cur.execute("SELECT COALESCE(MAX(rowid), 0) FROM sales_data")
    last_id = cur.fetchone()[0]
    return last_id

last_row_id = get_last_rowid()
print("Last row id on production data warehouse = ", last_row_id)

# Function to get the latest records from MySQL
def get_latest_records(rowid):
    cursor.execute("SELECT * FROM sales_data WHERE rowid > %s", (rowid,))
    records = cursor.fetchall()
    return records

new_records = get_latest_records(last_row_id)
print("New rows on staging data warehouse = ", len(new_records))

# Function to insert records into PostgreSQL
def insert_records(records):
    if not records:
        return
    insert_query = """
        INSERT INTO sales_data (rowid, product_id, customer_id, price, quantity, timestamp)
        VALUES (%s, %s, %s, %s, %s, %s)
    """
    data = [(r['rowid'], r['product_id'], r['customer_id'], r['price'], r['quantity'], r['timestamp']) for r in records]
    cur.executemany(insert_query, data)
    conn.commit()

insert_records(new_records)
print("New rows inserted into production data warehouse = ", len(new_records))

# Close all connections and cursors at the end
cursor.close()
connection.close()
cur.close()
conn.close()

# End of program
