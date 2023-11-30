import subprocess
print("[*] Install requirements :")
with open('./TP_2/requirements.txt', 'r') as file:
    for line in file:
        line = line.strip()
        if not line or line.startswith("#"):
            continue

        try:
            __import__(line)
            print(f"[*] library : {line} is already installed.")
        except ImportError:
            
            try:
                subprocess.check_call(['pip', 'install', line])
                print(f"[*] library : {line} installed successfully.")
            except Exception as e:
                print(f"[*] Failed to install : library {line}: {e}")
                exit()

import mysql.connector
import configparser
import getpass
import pandas as pd
import requests 
import time
import os 
import pyfiglet
from tqdm import tqdm

BASE_URL : str = 'https://geo.api.gouv.fr/communes'
HEADERS : str = {
        'content-type': "application/json",
        }

if __name__ == "__main__" :
    
    terminal_width, _ = os.get_terminal_size()
    text = f"CSV Integrator"
    figlet = pyfiglet.Figlet(font='slant',width=terminal_width)
    print(figlet.renderText(text))
    
    config = configparser.ConfigParser()
    config_path = './TP_2/config.ini'
    
    user = str(input("User name : "))
    password = getpass.getpass("password : ") 
    
    config.read(config_path)
    host = config['mysql']['host']
    database = config['mysql']['database']
    port = config['mysql']['port']
    table = config['mysql']['table']
    
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
        print("[Success]\n")
    except Exception as e : 
        print(f"[Error] : {e}")
        exit()
    
    print(f"[*] Try to Create column named 'population' in {table}.")
    try : 
        cursor.execute(f"ALTER TABLE {table} ADD COLUMN population INT;")
        conn.commit()
        print("[Success]\n")
    except Exception as e : 
        print("[*] Column population already exist ")
        
    
    cursor.execute(f"SELECT code_commune_insee FROM {table}")  
    resultats = cursor.fetchall()    
    
    count_rows = 0
    print(f"[*] Update {table}.")
    progress_bar = tqdm(total=len(resultats), desc="Get population", unit=" lines")
    for resultat in resultats:
        para = f'code={resultat[0]}'
        try : 
            response = requests.get(url=BASE_URL, params=para, headers=HEADERS)
            time.sleep(2)
            if response.status_code == 200 : 
                count_rows += 1
                data = response.json()
                cursor.execute(f"UPDATE table_test_api SET population = {int(data[0]['population'])} WHERE code_commune_insee = {resultat[0]};")
                            
        except requests.exceptions.HTTPError as errh:
                print(f"[Error]: {errh}")
        except requests.exceptions.ConnectionError as errc:
                print(f"[Error]: {errc}")
        except requests.exceptions.Timeout as errt:
                print(f"[Error]: {errt}")
        except requests.exceptions.RequestException as err:
                print(f"[Error]: {err}")
        
        progress_bar.update(1)
        
    conn.commit()
    conn.close()
    progress_bar.close()
    print(f"[Success] {count_rows}, rows updated\n")
    print("[Bye :)]\n")
    