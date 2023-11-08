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

    call_api(url, **parameters)


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


def call_orderbook_upbit (ticker) : 
    '''
    Accepts ticker, returns current price and liquidity  
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


def get_prices_upbit() :     

    columns = ['ticker', 'bid_price_krw', 'ask_price_krw', 'lqtt', 'curr_time']
    df = pd.DataFrame(columns=columns)

    url = 'https://api.upbit.com/v1/market/all'
    parameters = {'isDetails': 'false'}
    
    json_object = call_api(url, **parameters)

    ticker_list = []

    for i in json_object : 
        # take only prices for the ones which compares to KRW 
        if "KRW" in i['market'] : 
            ticker_list.append (i['market']) 

    for ticker in ticker_list : 
        bid_price, ask_price, lqtt = call_orderbook_upbit(ticker)
        curr_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())
        df.loc[len(df)] = [ticker, bid_price, ask_price, lqtt, curr_time]

        print(ticker, bid_price, lqtt)

    curr_ex_rate = get_exchange_rate()

    df['price_usd'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    # returns only the base pair for KRW pairs 
    df['base_ticker'] = df['ticker'].apply(lambda x : x.replace('KRW-', ''))

    return df 
    

def get_prices_bithumb() : 

    # orderbook here contains all the tickers, don't have to call prices separately.
    url = "https://api.bithumb.com/public/orderbook/ALL_KRW"

    json_object = call_api(url)

    data = json_object['data'] 

    columns = ['ticker', 'bid_price_krw', 'ask_price_krw', 'lqtt', 'curr_time']
    df = pd.DataFrame(columns=columns)

    for key, value in data.items() : 
        lqtt_krw = 0 

        if key not in ['timestamp', 'payment_currency'] : 
            for bid in value['bids'] : 
                lqtt_krw += bid['price'] * bid['quantity']
        
        curr_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())
        df.loc[len(df)] = [key, value['bids'][0]['price'], value['asks'][0]['price'], lqtt_krw, curr_time]

    curr_ex_rate = get_exchange_rate()

    df['price_usd'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    df.rename(columns={'ticker' : 'base_ticker'})

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


def check_price_diff (df_base, df_against, base_name, against_name, notif_trig) : 
    '''
    Accepts list of tickers for two exchanges, maps the tickers, and sends notification when triggered. 
    '''

    df_base.rename(columns={'price_usd' : 'price_usd_base', 'ask_price_usd' : 'ask_price_usd_base'})

    df_against.rename(columns={'price_usd' : 'price_usd_against', 'ask_price_usd' : 'ask_price_usd_against'})

    df_combined = pd.merge(df_base, df_against, on='base_ticker', how='left')

    # if positive then base is higher, if negative then base is lower. 
    df_combined['usd_diff'] = df_combined['price_usd_base'] - df_combined['price_usd_against']
    df_combined['pct_diff'] = abs(df_combined['usd_diff'] / df_combined['price_usd_against']) 

    # Formula explanation : We are buying token from other exchanges, selling on upbit for KRW, sell KRW for ETH, and send back. So we need ask price of ETH on upbit instead bid price. 

    df_combined['ask_usd_diff'] = df_combined['ask_price_usd_base'] - df_combined['ask_price_usd_against']
    df_combined['ask_pct_diff'] = abs(df_combined['ask_usd_diff'] / df_combined['price_usd_against']) 

    # get ask price pct difference of ETH on Upbit
    for index, row in df_combined.iterrows() : 
        if df_combined.loc[index, 'base_ticker'] == 'ETH' : 
            base_eth_ask_price_pct = df_combined.loc[index, 'ask_pct_diff']
            break 

    profit_pct_lim = 5

    for index, row in df_combined.iterrows() : 
        # case when base price > against
        if df_combined.loc[index, 'usd_diff'] > 0 : 
            token_upbit_delta_pct = df_combined.loc[index, 'pct_diff'] 

            profit_pct =  100 * (token_upbit_delta_pct + 1) * (1 - base_eth_ask_price_pct) - 100

            print(df_combined.loc[index, 'base_ticker'], abs(df_combined.loc[index, 'pct_diff']) * 100, profit_pct, base_eth_ask_price_pct)
            
            # If profit_pct > x, then we want notification. 
            if profit_pct > profit_pct_lim : 
                notif_trig = 1
                message1 = '{} - {} is higher than {} by {:.2f} %.'.format(df_combined.loc[index, 'base_ticker'], base_name, against_name, abs(df_combined.loc[index, 'pct_diff']) * 100)
                message2 = 'Absolute Diff - $ {:.6f}'.format(abs(df_combined.loc[index, 'usd_diff']))
                message3 = 'Profit Pct Estimate - {:.2f} %'.format(profit_pct)
                message4 = '{} Rough USD Liquidity - $ {:.2f}'.format(base_name, df_combined.loc[index, 'lqtt_usd'])
                tg_notif(message1 + '\n\n' + message2 + '\n\n' + message3 + '\n\n' + message4 + '\n\n')
    
    return notif_trig
    

@timing_decorator
def execute() : 
    df_upbit = get_prices_upbit() 
    df_bithumb = get_prices_bithumb()
    df_binance = get_prices_binance() 

    # allows script to return default notification if condition is not triggered
    notif_trig = 0 

    notif_trig = check_price_diff(df_upbit, df_binance, 'Upbit', 'Binance', notif_trig)
    notif_trig = check_price_diff(df_bithumb, df_binance, 'Bithumb', 'Binance', notif_trig)

    if notif_trig == 0 : 
        tg_notif("No tickers within profit pct range of > {:.0f} %".format(profit_pct_lim))


def lambda_handler(event, context):
    # TODO implement
    execute()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


if __name__ == '__main__' : 
    print(get_tickers_bithumb()) 





    

