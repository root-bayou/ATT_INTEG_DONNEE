import mysql.connector
import configparser
import pandas as pd
from tqdm import tqdm
from colorama import Fore, Style
import argparse
import chardet
import pyfiglet
import os
from prettytable import PrettyTable
import requests
import time

def print_s(m) : 
    print(Fore.GREEN + m + Style.RESET_ALL)

def print_f(m) : 
    print(Fore.RED + m + Style.RESET_ALL)

def print_w(m) : 
    print(Fore.YELLOW + m + Style.RESET_ALL)
    
def detect_encoding(file_path):
    with open(file_path, 'rb') as f:
        result = chardet.detect(f.read())
    return result['encoding']

def get_mysql_data_type(pandas_data_type):
    
    data_type_mapping = {
        'object': 'VARCHAR(255)',
        'int64': 'INT',
        'float64': 'FLOAT',
    }
    return data_type_mapping.get(pandas_data_type, 'VARCHAR(255)')

def mysql_connect(arg) : 
    config = configparser.ConfigParser()
    config_path = arg.mysqlconfig
    user = arg.user_name
    password = arg.user_password
    config.read(config_path)
    host = config['mysql']['host']
    database = config['mysql']['database']
    port = config['mysql']['port']
    
    print("[*] Connect to the DB.")
    try : 
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database,
            port=port
        )
        cursor = conn.cursor() 
        print_s("[Success]\n")
    except Exception as e : 
        print_f(f"[Error] : {e}")
    
    return conn
    
def insert(arg) -> None: 
    
    table_name = arg.table_name
    csv_file_path = arg.inputfile
    conn = mysql_connect(arg)
    cursor = conn.cursor()
     
    encoding = detect_encoding(csv_file_path)
    print_w(f"[+] File encoding is {encoding}")
    
    try : 
        log_table_query = """
        CREATE TABLE IF NOT EXISTS log_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        who VARCHAR(255),
        event_type VARCHAR(255),
        event_description VARCHAR(255),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(log_table_query) 
    except Exception as e : 
        print(f"[Error] creating table 'log_table': {e}")
    
    df = pd.read_csv(csv_file_path, encoding=encoding, sep=';')
    df = df.head(15)
    p_table = PrettyTable(['Column', 'Type of data'])
    for col in df.columns:
        p_table.add_row([col, get_mysql_data_type(str(df[col].dtype))])

    print(f"[*] Data Type of {csv_file_path.split('/')[-1]}:")
    print(p_table)
    print(f"[*] Check the existance of the table named {table_name}")

    try:
        
        cursor.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        cursor.fetchone()
        print_s("[Success] : The table are already exist.") 
        
        column_query = f"SHOW COLUMNS FROM {table_name}"
        cursor.execute(column_query)
        columns = cursor.fetchall()
        column_names_from_database  = [column[0] for column in columns]
        column_names_from_dataframe = df.columns.str.lower().tolist()
        
        if column_names_from_database != column_names_from_dataframe:
            print_f("[*] The column names in the database do not match the column names in the given CSV file.")
            print_w(f"[+] Table name remplaced by {csv_file_path.split('/')[-1].split('.')[0]}")
            table_name = csv_file_path.split('/')[-1].split('.')[0]
            create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col.lower()} {get_mysql_data_type(str(df[col].dtype))}' for col in df.columns])})"
            cursor.execute(create_table_query)
    
     
    except mysql.connector.Error as e:
        print_w("[Warning] : The table does not exist. It will be created.")
        create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{col.lower()} {get_mysql_data_type(str(df[col].dtype))}' for col in df.columns])})"
        cursor.execute(create_table_query)
    
    print(f"[*] Insert data in {table_name} table\n")
    df = df.map(lambda x: str(x).lower() if pd.notna(x) else x)
    df = df.fillna('NA')
    
    try : 
        total_rows = len(df)
        rows_inserted = 0
        
        #Ajoute de la condition si le len(df) == len(table_mysql) sauté directement DEMAIN
        
        for index, row in tqdm(df.iterrows(), total=total_rows, desc="Check and Insert", unit="row"):
            where_conditions = " AND ".join(f"{col.lower()} = '{row[col]}'" for col in df.columns)
            select_query = f"SELECT * FROM {table_name} WHERE {where_conditions}"
            cursor.execute(select_query)
            row_exists = False if cursor.fetchone() is None else True
            if not row_exists:
                insert_query = f"INSERT INTO {table_name} VALUES ({', '.join(['%s' for _ in range(len(df.columns))])})"
                insert_params = tuple(row)
                cursor.execute(insert_query, insert_params)
                rows_inserted += 1
        
        try:
            trigger_query = f"""
            CREATE TRIGGER IF NOT EXISTS who_and_what
            AFTER INSERT ON {table_name}
            FOR EACH ROW
            BEGIN
                    INSERT INTO log_table (who, event_type, event_description, created_at)
                    VALUES (USER(), 'INSERT', CONCAT('New row inserted with ID: ', NEW.code_commune_insee), NOW());
            END
            """
            cursor.execute(trigger_query)
        except Exception as e:
            print(f"[Error] creating trigger: {e}")
            print("\n")
        
        conn.commit()        
        cursor.close()
        conn.close()
        
        if rows_inserted > 0:
            print(f"[+] {rows_inserted} rows have been successfully inserted.")
        else:
            print("[+] : No modification needed, all data already exists.")
        print_s("[Success]")
    except Exception as e : 
        print_f(f"[Faillure] : {e}")

                
if __name__ == "__main__" :
    
    terminal_width, _ = os.get_terminal_size()
    text = f"CSV Integrator"
    figlet = pyfiglet.Figlet(font='slant',width=terminal_width)
    print(figlet.renderText(text))
    
    parser = argparse.ArgumentParser(description="Script for data insertion from CSV file to the specified DB.")
    parser.add_argument('inputfile', help='Specify the CSV inputfile to load.')
    parser.add_argument('-c','--mysqlconfig', help='Specify the file config for mysql connector.',required=True)
    parser.add_argument('-t','--table-name', help='Specify the table name. If the table does not exist it will be created', required=True)
    parser.add_argument('--user-name',help='Specify the user name for MySql DB',required=True)
    parser.add_argument('--user-password',help='Specify the password user',required=True)
    parser.add_argument('-v', '--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()
    
    insert(args)
   
    
