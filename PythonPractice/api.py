from fastapi import FastAPI
import json
from db import connect_to_db, select_by_ip, select_by_date, select_by_range

app = FastAPI()

with open('config.json', 'r') as config_file:
    config_data = json.load(config_file)

connection = None
connection = connect_to_db(config_data)

@app.get("/get-by-ip/{ip}")
def get_by_ip(ip: str):
    data = select_by_ip(connection, ip)
    return {"data": data}

@app.get("/get-by-date/{date}")
def get_by_date(date: str):
    data = select_by_date(connection, date)
    return {"data": data}

@app.get("/get-by-date-range/{date_start}&{date_end}")
def get_by_date_range(date_start: str, date_end: str):
    data = select_by_range(connection, date_start, date_end)
    return {"data": data}