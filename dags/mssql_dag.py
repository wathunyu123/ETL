from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from io import StringIO
import matplotlib.pyplot as plt
import pandas as pd
import pymssql
from dotenv import load_dotenv
import os

# Load environment variables from the .env file
load_dotenv(dotenv_path='/opt/airflow/.env')

# MSSQL connection details
# server_production = '192.168.1.7' # '10.0.1.190'
# port_production = '1433'
# database_production = 'coop'
# username_production = 'sa'
# password_production = 'qwerty123$'

# server_development = '192.168.1.7' # '10.0.1.190'
# port_development = '1533'
# database_development = 'coop'
# username_development = 'sa'
# password_development = 'qwerty123$'

df_receipt = pd.DataFrame()
df_contract = pd.DataFrame()
df_member = pd.DataFrame()

table_map = {
    'receipt': 'df_receipt',
    'contract': 'df_contract',
    'member': 'df_member'
}

def connect_mssql(production=True, **kwargs):
    """Function to connect to MSSQL and return a connection."""
    try:
        if production is True:
            server_production = os.getenv('MSSQL_SERVER_PRODUCTION')
            port_production = os.getenv('MSSQL_PORT_PRODUCTION')
            username_production = os.getenv('MSSQL_USER_PRODUCTION')
            password_production = os.getenv('MSSQL_PASSWORD_PRODUCTION')
            database_production = os.getenv('MSSQL_DATABASE_PRODUCTION')
            connection = pymssql.connect(
                server=f"{server_production},{port_production}",
                user=username_production,
                password=password_production,
                database=database_production,
                charset='UTF-8'
            )
            # connection = pymssql.connect(
            #     server=f"{server_production},{port_production}",
            #     user=username_production,
            #     password=password_production,
            #     database=database_production,
            #     charset='UTF-8'
            # )
            return connection
        else:
            server_development = os.getenv('MSSQL_SERVER_DEVELOPMENT')
            port_development = os.getenv('MSSQL_PORT_DEVELOPMENT')
            username_development = os.getenv('MSSQL_USER_DEVELOPMENT')
            password_development = os.getenv('MSSQL_PASSWORD_DEVELOPMENT')
            database_development = os.getenv('MSSQL_DATABASE_DEVELOPMENT')
            connection = pymssql.connect(
                server=f"{server_development},{port_development}",
                user=username_development,
                password=password_development,
                database=database_development,
                charset='UTF-8'
            )
            # connection = pymssql.connect(
            #     server=f"{server_development},{port_development}",
            #     user=username_development,
            #     password=password_development,
            #     database=database_development,
            #     charset='UTF-8'
            # )
            return connection
    except Exception as e:
        raise RuntimeError(f"Error connecting to MSSQL: {e}")

def query_table(table_name, first=False):
    """Function to query a table from MSSQL and return it as a DataFrame."""
    global df_receipt
    global df_contract
    global df_member

    # Connect only if not already connected
    connection = connect_mssql()

    # Set the SQL query based on the table name
    query = ""
    if table_name == 'receipt':
        query = "SELECT * FROM receipt" if first else "SELECT * FROM tmp_receipt"
    elif table_name == 'contract':
        query = "SELECT * FROM contract" if first else "SELECT * FROM tmp_contract"
    elif table_name == 'member':
        query = "SELECT * FROM member" if first else "SELECT * FROM tmp_member"
    else:
        raise ValueError(f"Unknown table name: {table_name}")

    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            # Fetch all rows and create DataFrame
            df = pd.DataFrame(cursor.fetchall(), columns=[col[0] for col in cursor.description])
            
            if df.empty:
                print(f"The DataFrame for table '{table_name}' is empty.")
                return df

            # Handle special case for 'member' table if queried
            if table_name == 'member' and first:
                columns_to_remove = [
                    'mast_name', 'mast_fname', 'mast_addr', 'mast_addr2',
                    'mast_memo', 'mast_marry_name', 'mast_tel',
                    'mast_eng_name', 'mast_eng_fname', 'mast_eng_lname', 'mast_image',
                    'mast_email'
                ]
                columns_to_remove_upper = [col.upper() for col in columns_to_remove]

                # Drop the specified columns from df
                df = df.drop(columns=columns_to_remove_upper, errors='ignore')
                
                df.columns = df.columns.str.strip()

                columns_to_drop = ['mast_memb_no']
                columns_to_drop_upper = [col.upper() for col in columns_to_drop]

                df = df.dropna(subset=columns_to_drop_upper)
                
                print(df)

            if table_name == 'contract' and first:
                columns_to_drop = ['prom_memb_no']
                columns_to_drop_upper = [col.upper() for col in columns_to_drop]

                # Drop rows where 'prom_memb_no' is NULL
                df = df.dropna(subset=columns_to_drop_upper)
                print(df.columns)
            
            if table_name == 'receipt' and first:
                print(df.columns)

            # Assign the DataFrame to the correct global variable
            if table_name == 'receipt':
                df_receipt = df
            elif table_name == 'contract':
                df_contract = df
            elif table_name == 'member':
                df_member = df

            # If 'first' is True, drop the old temporary table, insert the new data, and return the DataFrame
            if first:
                delete_table(table=f"tmp_{table_name}")
                insert_dataframe(table=f"tmp_{table_name}", dataframe=df)
            else:
                return df

    except Exception as e:
        raise RuntimeError(f"Error executing query for table '{table_name}': {e}")

    finally:
        connection.close()

def insert_dataframe(table, dataframe):
    """
    Insert records from a DataFrame into the specified MSSQL table.
    
    Args:
        table (str): Name of the table to insert into.
        dataframe (pd.DataFrame): DataFrame containing the data to insert.
    """

    # Connect to the database (assuming the `connect_mssql` function exists)
    connection = connect_mssql(production=False)
    
    if dataframe.empty:
        print("DataFrame is empty. No data to insert.")
        return
    
    # Prepare the INSERT SQL statement
    columns = ', '.join(dataframe.columns)
    placeholders = ', '.join(['%s'] * len(dataframe.columns))
    sql = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
    
    try:
        cursor = connection.cursor()
        
        # Loop through the DataFrame and insert each row
        for index, row in dataframe.iterrows():
            cursor.execute(sql, tuple(row))
        
        # Commit the transaction
        connection.commit()
        print(f"Inserted {len(dataframe)} records into {table} successfully.")
    
    except Exception as e:
        # Handle any errors during the insertion process
        print(f"Error inserting DataFrame: {e}")
    
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

def delete_table(table):
    """
    Deletes all records from the specified MSSQL table.
    
    Args:
        table (str): Name of the table to delete all records from.
    """

    # Connect to the database (assuming the `connect_mssql` function exists)
    connection = connect_mssql()
    
    if connection is None:
        print("Failed to connect to the database.")
        return

    # Prepare the DELETE SQL statement
    sql = f"DELETE FROM {table}"
    
    try:
        cursor = connection.cursor()
        
        # Execute the DELETE query
        cursor.execute(sql)
        
        # Commit the transaction
        connection.commit()
        print(f"All records deleted from table '{table}' successfully.")
    
    except Exception as e:
        # Handle any errors that may occur
        print(f"Error deleting records from table '{table}': {e}")
    
    finally:
        # Close the cursor and connection
        cursor.close()
        connection.close()

def print_detail_df(table_name):
    """
    Function to print details of a DataFrame based on the table name.
    
    Args:
        table_name (str): Name of the table to query and print details for.
    """

    # Global variables for dataframes
    global df_receipt, df_contract, df_member

    # Dictionary to map table names to their corresponding global DataFrames
    global table_map

    # Check if the table name exists in the table_map
    if table_name in table_map:
        # Query the table and assign the result to the corresponding global DataFrame
        globals()[table_map[table_name]] = query_table(table_name=table_name)
        
        # Fetch the DataFrame from the global variable
        df = globals()[table_map[table_name]]
        
        # Check if the DataFrame is empty
        if df is not None and not df.empty:
            print(df.head(5))
            print(df.info())
            print(df.describe())
        else:
            print(f"The DataFrame for table '{table_name}' is empty.")
    else:
        print(f"Table '{table_name}' not found.")

def check_null_df(table_name):
    """Function to check for null values in a DataFrame."""

    # Global variables for dataframes
    global df_receipt, df_contract, df_member

    # Dictionary to map table names to their corresponding global DataFrames
    global table_map

    # Check if the table name exists in the table_map
    if table_name in table_map:
        # Query the table and assign the result to the corresponding global DataFrame
        globals()[table_map[table_name]] = query_table(table_name=table_name)
        
        # Fetch the DataFrame from the global variable
        df = globals()[table_map[table_name]]

        if df is not None and not df.empty:
            print(df.isnull().sum())
        else:
            print(f"No DataFrame found for {df}")

    else:
        print(f"The DataFrame for table '{table_name}' is empty.")

def clean1_data(table_name):
    """Function to clean the 'receipt' DataFrame."""

    # Global variables for dataframes
    global df_receipt, df_contract, df_member

    # Dictionary to map table names to their corresponding global DataFrames
    global table_map

    # Check if the table name exists in the table_map
    if table_name in table_map:
        # Query the table and assign the result to the corresponding global DataFrame
        globals()[table_map[table_name]] = query_table(table_name=table_name)
        
        # Fetch the DataFrame from the global variable
        df = globals()[table_map[table_name]]

        if table_name == 'receipt':
            # Perform cleaning
            df.drop_duplicates(inplace=True)
            df = df.replace("NULL", None)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)

        if table_name == 'contract':
            # Perform cleaning
            df.drop_duplicates(inplace=True)
            df = df.replace("NULL", None)
            df['PROM_PERD_AMT_ADD_NEXT'] = df['PROM_PERD_AMT_ADD_NEXT'].str.strip().replace('', None)
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)
        
        if table_name == 'member':
            # Perform cleaning
            df.drop_duplicates(inplace=True)
            df = df.replace("NULL", None)
            columns_to_clean = ['MAST_PAID_AMT', 'MAST_PAID_SHR', 'MAST_SAL_TO_COOP', 'MAST_SAL_TO_BANK', 'MAST_REP_PAY_AMT', 'MAST_RETIRE_SALARY', 'MAST_FINE_AMT']
            df[columns_to_clean] = df[columns_to_clean].apply(lambda x: x.str.strip().replace('', None))
            df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)

def clean2_data(table_name):
    """Function to clean the 'receipt' DataFrame."""

    # Global variables for dataframes
    global df_receipt, df_contract, df_member

    # Dictionary to map table names to their corresponding global DataFrames
    global table_map

    # Check if the table name exists in the table_map
    if table_name in table_map:
        # Query the table and assign the result to the corresponding global DataFrame
        globals()[table_map[table_name]] = query_table(table_name=table_name)
        
        # Fetch the DataFrame from the global variable
        df = globals()[table_map[table_name]]

        if table_name == 'tmp_receipt':
            # Clean columns
            columns_to_clean = {
                'PAY_SAVING_DOC_NO': 'int',
                'PAY_FINE_AMT': 'float',
                'PAY_OVER_AMT': 'float',
                'PAY_PRINT_DATE': 'int',
                'PAY_PRINT_OP_CODE': 'int',
                'POST_DATETIME': 'int',
                'PAY_PROM_INT': 'int',
                'PAY_RINT_AMT': 'int',
                'PAY_DESC': 'str',
                'PAY_REF_NO': 'int',
                'PAY_REF_ID': 'int',
                'PAY_PERD_AMT': 'int',
                'PAY_COND_TYPE': 'int',
            }
            # Perform cleaning    
            for column, dtype in columns_to_clean.items():
                df = clean_column(df, column, dtype)

            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)

        if table_name == 'tmp_contract':
            # Clean columns
            columns_to_clean = {
                'PROM_SPEC_TYPE': 'int',
                'PROM_DUE_DATE': 'int',
                'PROM_CHK_RIGHT': 'int',
                'PROM_IS_CALL_AVG_INT': 'int',
                'PROM_IS_CALL_ACCUED_INT': 'int',
                'PROM_STOP_AMT_STS': 'int',
                'PROM_NPL_CODE': 'int',
                'PROM_DEBT_STS': 'int',
                'PROM_DEBT_DUE_DATE': 'int',
                'PROM_START_PAY': 'int',
                'PROM_IS_CANCEL': 'int',
                'PROM_PAID_MONTH_AMT': 'float',
                'PROM_FUND_AMT': 'float',
                'PROM_FUND_PAID_AMT': 'float',
                'PROM_INT2_BAL': 'float',
                'PROM_PERD_AMT_ADD': 'float',
                'PROM_PERD_AMT_ADD_NEXT': 'float',
                'PROM_OP_CODE': 'int',
                'POST_DATETIME': 'int',
            }
            # Perform cleaning
            for column, dtype in columns_to_clean.items():
                df = clean_column(df, column, dtype)

            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)
        
        if table_name == 'tmp_member':
            # Clean columns
            columns_to_clean = {
                'MAST_MARRY_MEMBNO': 'int',
                'MAST_CARD_BANK_STS': 'int',
                'MAST_CLEVEL': 'int',
                'MAST_SMS_GRP': 'int',
                'MAST_ACC_NO': 'int',
                'MAST_SAL_TO_COOP': 'float',
                'MAST_SAL_TO_BANK': 'float',
                'MAST_REP_PAY_AMT': 'float',
                'MAST_IS_PRN_SLIP': 'int',
                'MAST_SEND_DOC_TYPE': 'int',
                'MAST_REF_MEMB_NO': 'int',
                'MAST_MEMB_REF_STS': 'int',
                'MAST_INC_AMT': 'int',
                'MAST_BLOOD_GROUP': 'str',
                'MAST_SALARY_DEP': 'int',
                'MAST_OP_CODE': 'int',
            }
            # Perform cleaning
            for column, dtype in columns_to_clean.items():
                df = clean_column(df, column, dtype)
            
            insert_dataframe(table=f"tmp_{table_name}", dataframe=df)

def clean_column(df, column_name, dtype, fill_value=0):
    """Helper function to clean a DataFrame column."""
    if dtype == 'int':
        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
        df[column_name] = df[column_name].fillna(fill_value).astype(int)
    elif dtype == 'float':
        df[column_name] = df[column_name].astype(float)
        df[column_name] = df[column_name].fillna(fill_value).astype(float)
    elif dtype == 'str':
        df[column_name] = df[column_name].astype(str).fillna('')
    return df

def boxplot_data(table_name):
    """
    This function generates a boxplot for three specific columns from a table (DataFrame) selected by name,
    and saves the plot as a PNG image.
    """
    try:
        # Check if the table_name exists in the table_map
        if table_name not in table_map:
            raise ValueError(f"Table '{table_name}' not found in table_map. Available tables: {list(table_map.keys())}")

        # Query the table and assign the result to the corresponding global DataFrame
        globals()[table_map[table_name]] = query_table(table_name=table_name)
        
        # Fetch the DataFrame from the global variable
        df = globals()[table_map[table_name]]

        print(df)
        
        # Define columns of interest for the boxplot
        if table_name == 'receipt':
            columns = ['PAY_PERD', 'PAY_AMT1', 'PAY_AMT2']
        elif table_name == 'contract':
            columns = ['PROM_LOAN_AMT', 'PROM_LOAN_PERD', 'PROM_LOAN_PERD_AMT']
        elif table_name == 'member':
            columns = ['MAST_PAID_AMT', 'MAST_PAID_SHR', 'MAST_SAL_TO_COOP']
        else:
            raise ValueError(f"No predefined columns for table '{table_name}'")
        
        # Check if columns exist in the DataFrame
        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            raise KeyError(f"The following columns are missing from the {table_name} DataFrame: {missing_cols}")
        
        # Convert the columns to numeric and force them to float, handling non-numeric values as NaN
        df[columns] = df[columns].apply(pd.to_numeric, errors='coerce').astype(float)
        
        # Create the boxplot
        plt.boxplot([df[columns[0]].dropna(), 
                     df[columns[1]].dropna(), 
                     df[columns[2]].dropna()], 
                     labels=columns)
        
        # Set plot title and labels
        plt.title(f"Boxplot From {table_name}")
        plt.ylabel("Y-Axis")
        
        # Save the boxplot as a PNG file
        image_path = f"/tmp/{table_name}_boxplot.png"
        plt.savefig(image_path, format='png')
        plt.close()
        
        print(f"Boxplot saved as {image_path}")

    except Exception as e:
        print(f"Error generating boxplot: {e}")

def insert_clean(table_name):
    """
    Insert cleaned records from a DataFrame into the specified MSSQL table.
    """

    # Connect to the database (assuming the `connect_mssql` function exists)
    connection = connect_mssql(production=False)

    # Dictionary to map table names to their corresponding global DataFrames
    global table_map
    
    try:
        # Check if the table name exists in the table_map
        if table_name not in table_map:
            raise ValueError(f"Table '{table_name}' not found in table_map. Available tables: {list(table_map.keys())}")
        
        # Fetch the DataFrame from the global variable
        df = query_table(table_name=table_name)
        
        # Delete the old records from the target table
        delete_table(table=table_name)

        # Insert the cleaned DataFrame into the new table with the "clean_" prefix
        insert_dataframe(table=f"clean_{table_name}", dataframe=df)
        
        # Print the number of rows inserted
        print(f"Successfully inserted {len(df)} rows into clean_{table_name}")
    
    except Exception as e:
        # Handle any errors during the insertion process
        print(f"Error inserting DataFrame: {e}")
    
    finally:
        # Close the connection properly
        connection.close()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 10),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id='mssql_dag',
    default_args=default_args,
    description='MSSQL Data Cleansing',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:

    # Task to query the 'receipt' table
    query_receipt = PythonOperator(
        task_id='query_receipt',
        python_callable=query_table,
        op_kwargs={
            'table_name': 'receipt',
            'first': True,
        }
    )

    # Task to query the 'contract' table
    query_contract = PythonOperator(
        task_id='query_contract',
        python_callable=query_table,
        op_kwargs={
            'table_name': 'contract',
            'first': True,
        }
    )

    # Task to query the 'member' table
    query_member = PythonOperator(
        task_id='query_member',
        python_callable=query_table,
        op_kwargs={
            'table_name': 'member',
            'first': True,
        }
    )

    # Task to display details of the 'receipt' dataframe
    print_receipt_detail = PythonOperator(
        task_id='print_receipt_detail',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'receipt'}
    )

    # Task to display details of the 'contract' dataframe
    print_contract_detail = PythonOperator(
        task_id='print_contract_detail',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'contract'}
    )

    # Task to display details of the 'member' dataframe
    print_member_detail = PythonOperator(
        task_id='print_member_detail',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'member'}
    )

    # Task to check null values in the 'contract' dataframe
    check_null_contract = PythonOperator(
        task_id='check_null_contract',
        python_callable=check_null_df,
        op_kwargs={'table_name': 'contract'}
    )

    # Task to check null values in the 'receipt' dataframe
    check_null_receipt = PythonOperator(
        task_id='check_null_receipt',
        python_callable=check_null_df,
        op_kwargs={'table_name': 'receipt'}
    )

    # Task to check null values in the 'member' dataframe
    check_null_member = PythonOperator(
        task_id='check_null_member',
        python_callable=check_null_df,
        op_kwargs={'table_name': 'member'}
    )

    # Task to clean1 of the 'receipt' dataframe
    clean1_receipt = PythonOperator(
        task_id='clean1_receipt',
        python_callable=clean1_data,
        op_kwargs={'table_name': 'receipt'}
    )

    # Task to clean1 of the 'contract' dataframe
    clean1_contract = PythonOperator(
        task_id='clean1_contract',
        python_callable=clean1_data,
        op_kwargs={'table_name': 'contract'}
    )

    # Task to clean1 of the 'member' dataframe
    clean1_member = PythonOperator(
        task_id='clean1_member',
        python_callable=clean1_data,
        op_kwargs={'table_name': 'member'}
    )

    # Task to clean2 of the 'receipt' dataframe
    clean2_receipt = PythonOperator(
        task_id='clean2_receipt',
        python_callable=clean2_data,
        op_kwargs={'table_name': 'receipt'}
    )

    # Task to clean2 of the 'contract' dataframe
    clean2_contract = PythonOperator(
        task_id='clean2_contract',
        python_callable=clean2_data,
        op_kwargs={'table_name': 'contract'}
    )

    # Task to clean2 of the 'member' dataframe
    clean2_member = PythonOperator(
        task_id='clean2_member',
        python_callable=clean2_data,
        op_kwargs={'table_name': 'member'}
    )

    # Define the second set of printing tasks
    print_receipt_detail_second = PythonOperator(
        task_id='print_receipt_detail_second',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'receipt'},
        dag=dag
    )

    # Task to display details of the 'contract' dataframe
    print_contract_detail_second = PythonOperator(
        task_id='print_contract_detail_second',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'contract'},
        dag=dag
    )

    # Task to display details of the 'member' dataframe
    print_member_detail_second = PythonOperator(
        task_id='print_member_detail_second',
        python_callable=print_detail_df,
        op_kwargs={'table_name': 'member'},
        dag=dag
    )

    # Task to display details of the 'receipt' dataframe
    boxplot_data_receipt = PythonOperator(
        task_id='boxplot_data_receipt',
        python_callable=boxplot_data,
        op_kwargs={'table_name': 'receipt'},
        dag=dag
    )

    # Task to draw boxplot of the 'contract'
    boxplot_data_contract = PythonOperator(
        task_id='boxplot_data_contract',
        python_callable=boxplot_data,
        op_kwargs={'table_name': 'contract'},
        dag=dag
    )

    # Task to draw boxplot of the 'member'
    boxplot_data_member = PythonOperator(
        task_id='boxplot_data_member',
        python_callable=boxplot_data,
        op_kwargs={'table_name': 'member'},
        dag=dag
    )

    # Task to insert clean_receipt table
    insert_clean_receipt = PythonOperator(
        task_id='insert_clean_receipt',
        python_callable=insert_clean,
        op_kwargs={'table_name': 'receipt'},
        dag=dag
    )

    # Task to insert clean_contract table
    insert_clean_contract = PythonOperator(
        task_id='nsert_clean_contract',
        python_callable=insert_clean,
        op_kwargs={'table_name': 'contract'},
        dag=dag
    )

    # Task to insert clean_member table
    insert_clean_member = PythonOperator(
        task_id='insert_clean_member',
        python_callable=insert_clean,
        op_kwargs={'table_name': 'member'},
        dag=dag
    )

    # Define task dependencies
    query_receipt >> print_receipt_detail >> check_null_receipt >> clean1_receipt >> clean2_receipt >> print_receipt_detail_second >> boxplot_data_receipt >> insert_clean_receipt
    query_contract >> print_contract_detail >> check_null_contract >> clean1_contract >> clean2_contract >> print_contract_detail_second >> boxplot_data_contract >> insert_clean_contract
    query_member >> print_member_detail >> check_null_member >> clean1_member >> clean2_member >> print_member_detail_second >> boxplot_data_member >> insert_clean_member