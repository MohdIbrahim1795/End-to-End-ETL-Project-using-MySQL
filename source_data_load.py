import mysql.connector
from mysql.connector import Error
import pandas as pd
import config

def create_connection():
    """Create a connection to MySQL."""
    try:
        connection = mysql.connector.connect(
            host=config.MYSQL_HOST,
            port=config.MYSQL_PORT,
            database=config.MYSQL_DATABASE,
            user=config.MYSQL_USER,
            password=config.MYSQL_PASSWORD
        )
        return connection
    except Error as e:
        print(f"Error connecting to MySQL: {e}")
        raise

def create_table():
    """Create the source table in MySQL."""
    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Create source table
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {config.SOURCE_TABLE} (
                timestamp TIMESTAMP,
                value INT,
                category VARCHAR(10),
                Usages DOUBLE DEFAULT 0,
                Outcome BOOLEAN DEFAULT FALSE
            )
        """)

        connection.commit()
        print(f"Table {config.SOURCE_TABLE} created successfully!")
    except Error as e:
        print(f"Error creating table: {e}")
        raise
    finally:
        if connection:
            cursor.close()
            connection.close()

def load_csv_to_mysql():
    """Load CSV data into the MySQL source table."""
    try:
        connection = create_connection()
        cursor = connection.cursor()

        # Read CSV files
        old_data = pd.read_csv("old_data.csv")
        new_data = pd.read_csv("new_data.csv")

        # Combine old and new data
        combined_data = pd.concat([old_data, new_data], ignore_index=True)
        combined_data.loc[:, 'Usages'] = combined_data['Usages'].fillna(0)
        combined_data.loc[:, 'Outcome'] = combined_data['Outcome'].fillna(False)


        # Insert data into MySQL table
        for _, row in combined_data.iterrows():
            cursor.execute(f"""
                INSERT INTO {config.SOURCE_TABLE} (timestamp, value, category, Usages, Outcome)
                VALUES (%s, %s, %s, %s, %s)
            """, (
                row['timestamp'],
                row['value'],
                row['category'],
                row.get('Usages', 0),  # Default to 0 if NaN
                row.get('Outcome', False)  # Default to False if NaN
            ))

        connection.commit()
        print(f"Data loaded into {config.SOURCE_TABLE} successfully!")
    except Error as e:
        print(f"Error loading data: {e}")
        raise
    finally:
        if connection:
            cursor.close()
            connection.close()

if __name__ == "__main__":
    create_table()
    load_csv_to_mysql()