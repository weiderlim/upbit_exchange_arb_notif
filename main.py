import json 
import requests
import pymysql 
from threading import Thread
import time 

def api_call (ticker) : 

    # Reads url and converts it into a readable JSON format
    url = "https://api.upbit.com/v1/orderbook"

    headers = {
        "accept": "application/json"
    }

    parameters = {
        'markets' : ticker
    }

    response = requests.get(url, headers=headers, params=parameters)

    # convert (usually response is in non JSON form) to json object first 
    json_object = json.loads(response.text) 

    # check done here, if too many requests were made at the same time returns - {'name': 'too_many_requests'}
    # print(ticker, json_object)

    return json_object

def calc_lqtt (json_object) : 
    lqtt_total = 0
    orderbook_list = json_object[0]['orderbook_units']

    for i in orderbook_list : 
        lqtt_total += i['ask_price'] * i['ask_size']

    current_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())

    return current_time, lqtt_total

def write_sql (timestamp, lqtt_total) : 
    conn = pymysql.connect(
        host='localhost',
        user='root',
        password='password',
        db='mydatabase',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with conn.cursor() as cursor:
            # Create a new record
            sql = "INSERT INTO `users` (`dateCreated`, `lqtt`) VALUES (%s, %s)"
            cursor.execute(sql, (timestamp, lqtt_total))

        # Commit changes
        conn.commit()

        print("Record inserted successfully")
    finally:
        conn.close()

def get_tickers () : 

    url = 'https://api.upbit.com/v1/market/all'
    headers = {'accept': 'application/json'}
    params = {'isDetails': 'false'}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if the request was successful

    json_object = json.loads(response.text) 

    ticker_list = []

    for i in json_object : 
        ticker_list.append (i['market']) 

    return ticker_list

def timing_decorator(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} took {execution_time} seconds to execute")
        return result
    return wrapper

# # threading is good for anything that requires waiting and not computational limits. API's / connecting to databases are good for this as we need to wait for the site to respond. 
@timing_decorator
def execute_funcs() :     
    def task (ticker) :
        json_object = api_call(ticker)
        timestamp, lqtt_total = calc_lqtt(json_object)
        # write_sql(timestamp, lqtt_total)
        print(ticker, timestamp, lqtt_total)

    # need to query only 9 at a time, otherwise will return error. 
    curr_ticker_index = 0

    threads = []

    while curr_ticker_index < len(get_tickers()) : 
        for ticker in get_tickers()[curr_ticker_index : curr_ticker_index + 9]:
            t = Thread(target=task, args=(ticker,))
            threads.append(t)
            t.start()

        # wait for the threads to complete
        for t in threads:
            t.join()

        curr_ticker_index += 9 

@timing_decorator
def execute_funcs_inorder () : 
    for ticker in get_tickers() : 
        json_object = api_call(ticker)
        timestamp, lqtt_total = calc_lqtt(json_object)
        # write_sql(timestamp, lqtt_total)
        print(ticker, timestamp, lqtt_total)    

if __name__ =="__main__":

    execute_funcs() 
    # execute_funcs_inorder() 




