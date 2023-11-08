import requests 
import json 
import time 
import pandas as pd 
import os 
from pymongo import MongoClient


def timing_decorator(func):
    '''
    Script is ran every minute, so it is important to know how long the code takes to run 
    '''
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{func.__name__} took {execution_time} seconds to execute")
        return result
    return wrapper


def call_api (url, **kwargs) : 
    '''
    A general use api call function that is able to take in any number of parameters in json format (including no parameters)

    kwargs is in format of a dictionary with key value pairs or the URL parameters. 
    '''

    headers = {
        "accept": "application/json"
    }

    # if kwargs is not inputted, then kwargs = {}. {} is an acceptable input for params=
    response = requests.get(url, headers=headers, params=kwargs)
    
    # Handle the response and return data as needed.
    if response.status_code == 200:
        return response.json()
    else:
        return {"error": "API request failed"}
    
    # convert (usually response is in non JSON form) to json object first 
    json_object = json.loads(response.text) 

    return json_object  


def tg_notif (message) : 
    
    url = "https://api.telegram.org/bot{}/sendMessage".format(os.environ.get('telegram_key'))

    parameters = {
        'chat_id' : '-911570737',
        'text' : message
        }

    call_api(url, parameters)


def get_exchange_rate () : 
    '''
    Calling last row from the MongoDB collection. 
    '''

    collection_name = os.environ.get('COLLECTION_NAME', 'transactions')
    mongo_conn_str = os.environ.get('MONGO_CONN_STR', 'local')
    db_name = os.environ.get('DB_NAME', 'upbit_tracker')
    
    client = MongoClient(mongo_conn_str)
    db = client[db_name]
    collection = db[collection_name]

    for x in collection.find() : 
        curr_row = x

    return curr_row['exchange_rate']


def call_api_upbit (ticker) : 
    '''
    Accepts ticker, returns current price  
    '''

    url = "https://api.upbit.com/v1/orderbook"

    parameters = {
        'markets' : ticker
    }

    json_object = call_api(url, **parameters)

    orderbook = json_object[0]['orderbook_units']

    # check done here, if too many requests were made at the same time returns error - {'name': 'too_many_requests'}
    try : 
        # get bid price, ask price, and liquidty 
        bid_price = orderbook[0]['bid_price']
        ask_price = orderbook[0]['ask_price']
        lqtt = 0 

        # just report lqtt for first 5 
        for order in orderbook[0:5] : 
            lqtt += order['bid_price'] * order['bid_size']

        # emptying for RAM preservation
        orderbook = []

        return bid_price, ask_price, lqtt
        
    except : 
        if json_object['name'] == 'too_many_requests' : 
            print (ticker + '- too_many_request ERROR!')
            tg_notif(ticker + '- too_many_request ERROR!')
        else : 
            print ('SOME OTHER ERROR')
            tg_notif('SOME OTHER ERROR')


def get_tickers_upbit () : 

    url = 'https://api.upbit.com/v1/market/all'
    parameters = {'isDetails': 'false'}
    
    json_object = call_api(url, **parameters)

    ticker_list = []

    for i in json_object : 
        # take only prices for the ones which compares to KRW 
        if "KRW" in i['market'] : 
            ticker_list.append (i['market']) 

    return ticker_list


def get_prices_upbit() :     

    columns = ['ticker', 'bid_price_krw', 'ask_price_krw', 'lqtt', 'curr_time']
    df = pd.DataFrame(columns=columns)

    def task (ticker) :
        bid_price, ask_price, lqtt = call_api_upbit(ticker)
        curr_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())
        df.loc[len(df)] = [ticker, bid_price, ask_price, lqtt, curr_time]

        print(ticker, bid_price, lqtt)

    for ticker in get_tickers_upbit() : 
        task(ticker)

    curr_ex_rate = get_exchange_rate()

    df['price_usd_upbit'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd_upbit'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    # returns only the base pair for KRW pairs 
    df['base_ticker'] = df['ticker'].apply(lambda x : x.replace('KRW-', ''))

    return df 
    

def get_prices_binance() : 
    ''' 
    Returns price of all binance USDT pairs in a list. 
    '''

    url = "https://api.binance.com/api/v3/ticker/price"

    json_object = call_api(url)

    columns = ['base_ticker', 'price_usd_binance', 'curr_time']
    df = pd.DataFrame(columns=columns)

    # returns only the base pair for USDT pairs 
    for ticker in json_object : 
        if 'USDT' in  ticker['symbol'] : 
            curr_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())
            df.loc[len(df)] = [ticker['symbol'].replace('USDT', ''), float(ticker['price']), curr_time]

    delisted_binance = ['BTG']

    for ticker in delisted_binance : 
        df = df[~df['base_ticker'].str.contains(ticker)]
    
    return df 


def get_prices_bybit () : 
    return None 


def get_prices_bitget () : 
    return None 


def get_prices_mexc () : 
    return None 


def check_price_diff (df_upbit, df_binance) : 
    '''
    Accepts list of tickers for two exchanges, maps the tickers, and sends notification when triggered. 
    '''

    df_combined = pd.merge(df_upbit, df_binance, on='base_ticker', how='left')

    # if positive then upbit is higher, if negative then upbit is lower. 
    df_combined['usd_diff'] = df_combined['price_usd_upbit'] - df_combined['price_usd_binance']
    df_combined['pct_diff'] = abs(df_combined['usd_diff'] / df_combined['price_usd_binance']) 

    # Formula explanation : We are buying token from other exchanges, selling on upbit for KRW, sell KRW for ETH, and send back. So we need ask price of ETH on upbit instead bid price. 

    df_combined['ask_usd_diff'] = df_combined['ask_price_usd_upbit'] - df_combined['price_usd_binance']
    df_combined['ask_pct_diff'] = abs(df_combined['ask_usd_diff'] / df_combined['price_usd_binance']) 

    # get ask price pct difference of ETH on Upbit
    for index, row in df_combined.iterrows() : 
        # upbit_eth_ask_price_pct = df_combined[df_combined['base_ticker'].str.contains('ETH')]['ask_pct_diff'].loc[0]
        if df_combined.loc[index, 'base_ticker'] == 'ETH' : 
            upbit_eth_ask_price_pct = df_combined.loc[index, 'ask_pct_diff']
            break 

    # setting notification trigger so we know the script is actually running. 
    trigger = 0

    profit_pct_lim = 5

    for index, row in df_combined.iterrows() : 
        # case when upbit price > binance
        if df_combined.loc[index, 'usd_diff'] > 0 : 
            token_upbit_delta_pct = df_combined.loc[index, 'pct_diff'] 

            profit_pct =  100 * (token_upbit_delta_pct + 1) * (1 - upbit_eth_ask_price_pct) - 100

            print(df_combined.loc[index, 'base_ticker'], abs(df_combined.loc[index, 'pct_diff']) * 100, profit_pct, upbit_eth_ask_price_pct)
            
            # If profit_pct > x, then we want notification. 
            if profit_pct > profit_pct_lim : 
                trigger = 1
                message1 = '{} - Upbit is higher than Binance by {:.2f} %.'.format(df_combined.loc[index, 'base_ticker'], abs(df_combined.loc[index, 'pct_diff']) * 100)
                message2 = 'Absolute Diff - $ {:.6f}'.format(abs(df_combined.loc[index, 'usd_diff']))
                message3 = 'Profit Pct Estimate - {:.2f} %'.format(profit_pct)
                message4 = 'Upbit USD Liquidity Close to stated price - $ {:.2f}'.format(df_combined.loc[index, 'lqtt_usd'])
                tg_notif(message1 + '\n\n' + message2 + '\n\n' + message3 + '\n\n' + message4 + '\n\n')
    
    if trigger == 0 : 
        tg_notif("No tickers within profit pct range of > {:.0f} %".format(profit_pct_lim))


@timing_decorator
def execute() : 
    df_upbit = get_prices_upbit() 
    df_binance = get_prices_binance() 

    check_price_diff(df_upbit, df_binance)


def lambda_handler(event, context):
    # TODO implement
    execute()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }









    

