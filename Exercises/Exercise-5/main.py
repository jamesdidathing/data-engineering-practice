import psycopg2

def create_tables(conn, cur):
    cur.execute("DROP TABLE IF EXISTS transactions")
    cur.execute("DROP TABLE IF EXISTS products")
    cur.execute("DROP TABLE IF EXISTS accounts")

    create_accounts = """
        CREATE TABLE IF NOT EXISTS accounts
        (
            customer_id INT PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            address_1 VARCHAR(100),
            address_2 VARCHAR(100),
            city VARCHAR(50),
            state VARCHAR(50),  
            zip_code VARCHAR(20),
            join_date DATE
        );
    """

    create_products = """
        CREATE TABLE IF NOT EXISTS products
        (
            product_id INT PRIMARY KEY,
            product_code INT,
            product_description VARCHAR(50)
        );
    """

    create_transactions = """
        CREATE TABLE IF NOT EXISTS transactions
        (
            transaction_id VARCHAR(60) PRIMARY KEY,
            transaction_date DATE,
            product_id INT,
            product_code INT,
            product_description VARCHAR(50),
            quantity INT,
            customer_id INT
        );
    """
    # Execute CREATE statements
    cur.execute(create_accounts)
    cur.execute(create_products)
    cur.execute(create_transactions)

    # commit the changes
    conn.commit()
    print("Table created successfully")

def load_csv_data(conn, cur):
    # Load accounts
    with open('data/accounts.csv', 'r') as f:
        next(f)
        for line in f.readlines():
            cur.execute(
                "INSERT INTO accounts (customer_id, first_name, last_name, address_1, address_2, city, state, zip_code, join_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                line.strip().split(',')
            )

    # Load accounts
    with open('data/products.csv', 'r') as f:
        next(f)
        for line in f.readlines():
            cur.execute(
                "INSERT INTO products (product_id, product_code, product_description) VALUES (%s, %s, %s)",
                line.strip().split(',')
            )

    # Load transactions
    with open('data/transactions.csv', 'r') as f:
        next(f)
        for line in f.readlines():
            cur.execute(
                "INSERT INTO transactions (transaction_id, transaction_date, product_id, product_code, product_description, quantity, customer_id) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                line.strip().split(',')
            )

    conn.commit()
    print("Data ingested successfully")


def main():
    try:
        host = "postgres"
        database = "postgres"
        user = "postgres"
        pas = "postgres"
        conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
        print("Database connected successfully")
    except:
        print("Database not connected successfully")

    cur = conn.cursor()
    try:
        create_tables(conn, cur)
        load_csv_data(conn, cur)
        conn.commit()
    except Exception as e:
        print(f"An error occurred: {e}")


if __name__ == "__main__":
    main()
