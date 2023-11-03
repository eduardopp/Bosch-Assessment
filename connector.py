import pyodbc
import pandas as pd
import os
from dotenv import load_dotenv

load_dotenv()


# Note: I've uploadet .env for education purposes. Otherwise it would be in .gitignore
# SQL Azure Connection Settings
server = os.getenv('SERVER_NAME')
database = os.getenv('DB_NAME')
username = os.getenv('USERNAME')
password = os.getenv('PASSWORD')
port = os.getenv('PORT')
driver = '{ODBC Driver 17 for SQL Server}'


def create_coonection():
    """
    Create a connection to a Microsoft Azure SQL Database.

    Returns:
    pyodbc.Connection: A connection object to the Azure SQL Database.

    This function establishes a connection to a Microsoft Azure SQL Database using the provided configuration parameters such as the driver, server, database, username, and password.
    It then returns a pyodbc.Connection object, which can be used to interact with the database.

    Returns:
    pyodbc.Connection: A connection object for database operations.
    """
    conn_str = f"DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}"
    conn = pyodbc.connect(conn_str)
    return conn


def insert_complaints(df_complaints, conn):
    """
    Insert complaint data from a DataFrame into a database.
    Parameters:
    df_complaints (pandas.DataFrame): The DataFrame containing complaint data.
    conn (pyodbc.Connection): The connection to the database.
    
    Returns:
    bool: True if the insertion was successful, False otherwise.

    This function inserts complaint data from the provided DataFrame into a database table. It iterates through the DataFrame rows
        and executes SQL INSERT statements for each row in the 'complaints' table.
    If an error occurs during insertion, the function catches and handles it. If the insertion is successful, it returns True; otherwise, it returns False.

    Args:
    df_complaints (pandas.DataFrame): The DataFrame containing complaint data.
    conn (pyodbc.Connection): The connection to the database.

    Returns:
    bool: True if the insertion was successful, False otherwise.
    """
    try:
        cursor = conn.cursor()
        for index, row in df_complaints.iterrows():
            cursor.execute("INSERT INTO complaints (manufacturer, productMake, productModel, productYear, components, numComplaints, dateExtraction) VALUES (?, ?, ?, ?, ?, ?, ?)", row)
        conn.commit()
        return True

    except pyodbc.Error as ex:
        print(f"Error inserting to database: {str(ex)}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return False


def insert_alternative_fuel(df_alternative_fuel, conn):
    """
    Insert alternative fuel data from a DataFrame into a database.

    Parameters:
    df_alternative_fuel (pandas.DataFrame): The DataFrame containing alternative fuel data.
    conn (pyodbc.Connection): The connection to the database.

    Returns:
    bool: True if the insertion was successful, False otherwise.

    This function inserts alternative fuel data from the provided DataFrame into a database table. It iterates through the DataFrame rows 
        and executes SQL INSERT statements for each row in the 'alternativeFuel' table.
    If an error occurs during insertion, the function catches and handles it. If the insertion is successful, it returns True; otherwise, it returns False.

    Args:
    df_alternative_fuel (pandas.DataFrame): The DataFrame containing alternative fuel data.
    conn (pyodbc.Connection): The connection to the database.

    Returns:
    bool: True if the insertion was successful, False otherwise.
    """
    try:
        cursor = conn.cursor()
        for index, row in df1.iterrows():
            cursor.execute("INSERT INTO alternativeFuel (monthYearHistory, fuel_type_code, country, state, city, ev_connector_types, numDifConnectors) VALUES (?, ?, ?, ?, ?, ?, ?)", row)
        
        conn.commit()
        return True

    except pyodbc.Error as ex:
        print(f"Error inserting to database: {str(ex)}")
        return False

    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return False



# The following lines are commented because I didn't want the script to break because of the 'dummy connection'
def main(df_to_insert, df_name):

    # conn = create_connection()
    if "complaints" in df_name:
        print("Inserting complaints data into database...")
        # insert_complaints(df_to_insert, conn)
    else:
        print("Inserting alternative fuel data into database...")
        # insert_alternative_fuel(df_to_insert, conn)

    # conn.close()
 
