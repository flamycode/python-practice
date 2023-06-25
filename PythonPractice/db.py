import psycopg2
import json
import re
import os
import shutil

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

log_directory = config_data['log_directory']
processed_directory = config_data['processed_directory']
log_file_mask = config_data['log_file_mask']

def connect_to_db(config_data):
    try:
        connection = psycopg2.connect(
            host=config_data['database']['host'],
            database=config_data['database']['name'],
            user=config_data['database']['username'],
            password=config_data['database']['password']
        )
        return connection
    except Exception as error:
        print("[INFO] - Error:", error)
    finally:
        if connection is not None:
            print("[INFO] - Connection successful")

def create_table(connection):
    try:
        with connection.cursor() as cursor:
            cursor.execute('SELECT version();')
            print(f'Server version: {cursor.fetchone()}')

            cursor.execute(
                """CREATE TABLE IF NOT EXISTS Logs(
                    Log_ID SERIAL PRIMARY KEY,
                    IP VARCHAR(20) NOT NULL,
                    Log_Date TIMESTAMP NOT NULL,
                    REQUEST TEXT NOT NULL,
                    STATUS VARCHAR(10) NOT NULL
                );"""
            )

        connection.commit()
    except Exception as error:
        print("[INFO] - Error:", error)
    finally:
        if connection is not None:
            print("[INFO] - Table created")

def insert_data(connection, config_data):
    try:
        with connection.cursor() as cursor:
            files = os.listdir(log_directory)
            for file in files:
                file_path = os.path.join(log_directory, file)
                with open(file_path) as logs_file:
                    for line in logs_file:
                        match = re.search(r'^(\d+.\d+.\d+.\d+).+(\d{2})/([a-zA-Z]+)/(\d{4}):(\d{2}):(\d{2}):(\d{2}).+(GET|POST).+(200|40\d)', line)
                        if match:
                            ip_address = match.group(1)
                            day = match.group(2)
                            month = match.group(3)
                            year = match.group(4)
                            hour = match.group(5)
                            minute = match.group(6)
                            second = match.group(7)
                            request = match.group(8)
                            status = match.group(9)

                            month = config_data['months'][month]

                            date_time = f'{year}-{month}-{day} {hour}:{minute}:{second}'
                            cursor.execute("INSERT INTO Logs (IP, LOG_DATE, REQUEST, STATUS) VALUES (%s, %s, %s, %s)",
                                            (ip_address, date_time, request, status))
                shutil.move(file_path, processed_directory)
            connection.commit()
    except Exception as error:
        print("[INFO] - Error:", error)
    finally:
        if connection is not None:
            print("[INFO] - Data inserted")

def select_by_ip(connection, get_ip):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""Select * from logs where ip = '{get_ip}';
                """)

            rows = cursor.fetchall()
            data = []
            for row in rows:
                log_id, ip, log_date, request, status = row
                data_input = {
                    'LOG_ID': log_id,
                    'IP': ip,
                    'LOG_DATE': log_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'REQUEST': request,
                    'STATUS': status
                }
                data.append(data_input)
            print(json.dumps(data, indent=4))
            return data
    except Exception as error:
        print("[INFO] - Error:", error)


def select_by_date(connection, get_date):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""Select * from logs where LOG_DATE = '{get_date}';
                """)

            rows = cursor.fetchall()
            data = []
            for row in rows:
                log_id, ip, log_date, request, status = row
                data_input = {
                    'LOG_ID': log_id,
                    'IP': ip,
                    'LOG_DATE': log_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'REQUEST': request,
                    'STATUS': status
                }
                data.append(data_input)
            print(json.dumps(data, indent=4))
            return data
    except Exception as error:
        print("[INFO] - Error:", error)

def select_by_range(connection, get_date_start, get_date_end):
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""Select * from logs where log_date BETWEEN '{get_date_start}' and '{get_date_end}';
                """)

            rows = cursor.fetchall()
            data = []
            for row in rows:
                log_id, ip, log_date, request, status = row
                data_input = {
                    'LOG_ID': log_id,
                    'IP': ip,
                    'LOG_DATE': log_date.strftime("%Y-%m-%d %H:%M:%S"),
                    'REQUEST': request,
                    'STATUS': status
                }
                data.append(data_input)
            print(json.dumps(data, indent=4))
            return data
    except Exception as error:
        print("[INFO] - Error:", error)
