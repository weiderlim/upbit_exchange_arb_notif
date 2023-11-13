import requests 
import json 
import time 
import os 
import datetime 
from pymongo import MongoClient


def call_api_exchange_rate () : 

    url = "https://api.exchangeratesapi.io/v1/latest"

    parameters = {
        'access_key' : os.environ.get('EXCHANGE_RATE_KEY'),
        'symbols' : 'USD, KRW'
        }
    
    response = requests.get(url, params=parameters) 

    json_object = json.loads(response.text) 

    return json_object['rates']['KRW'] / json_object['rates']['USD']


def write_sql (exchange_rate) :
    collection_name = os.environ.get('COLLECTION_NAME', 'krw_usd_exchange_rate')
    mongo_conn_str = os.environ.get('MONGO_CONN_STR', 'local')
    db_name = os.environ.get('DB_NAME', 'upbit_tracker')
    
    client = MongoClient(mongo_conn_str)
    db = client[db_name]
    collection = db[collection_name]

    current_time = time.strftime("%y-%m-%d %H:%M:%S", time.localtime())
    timestamp = datetime.datetime.utcnow()

    try:
        data = {"dateCreated": timestamp, "exchange_rate": exchange_rate}
        collection.insert_one(data)

    except Exception as e:
        print(e)


def lambda_handler(event, context):
    # TODO implement
    curr_exchange_rate = call_api_exchange_rate() 
    write_sql(curr_exchange_rate)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
    
    