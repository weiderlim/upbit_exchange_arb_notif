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


def tg_notif (message, destination) : 
    
    url = "https://api.telegram.org/bot{}/sendMessage".format(os.environ.get('telegram_key'))

    if destination == 'real_time' : 
        chat_id = '-911570737'

    # sending notifications to alternate group for testing purposes
    else : 
        chat_id = '-4051618653'

    parameters = {
        'chat_id' : chat_id,
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
        curr_price = (bid_price + ask_price) / 2
        lqtt = 0 

        # 2% depth liquidity 
        for order in orderbook : 
            if order['bid_price'] > curr_price * 0.98 : 
                lqtt += order['bid_price'] * order['bid_size']

        return bid_price, ask_price, lqtt
        
    except : 
        if json_object['name'] == 'too_many_requests' : 
            print (ticker + '- too_many_request ERROR!')
            tg_notif(ticker + '- too_many_request ERROR!')
        else : 
            print ('SOME OTHER ERROR')
            tg_notif('SOME OTHER ERROR')


def get_prices_upbit() :     
    ''' 
    All df_base has output of Dataframe with : 
    columns = ['base_ticker', 'price_usd', 'ask_price_usd', 'lqtt_usd']
    datatype of 'price_usd', 'ask_price_usd' and 'lqtt' - float 
    '''

    columns = ['base_ticker', 'bid_price_krw', 'ask_price_krw', 'lqtt']
    df = pd.DataFrame(columns=columns)

    url = 'https://api.upbit.com/v1/market/all'
    parameters = {'isDetails': 'false'}
    
    json_object = call_api(url, **parameters)

    ticker_list = []

    # list of tokens that are diff between upbit and the rest of the market. 
    diff_ticker_list = ['TON']

    for i in json_object : 
        # take only prices for the ones which compares to KRW 
        if "KRW" in i['market'] : 
            ticker_list.append (i['market']) 

    for ticker in ticker_list : 
        if ticker not in diff_ticker_list : 
            bid_price, ask_price, lqtt = call_orderbook_upbit(ticker)
            df.loc[len(df)] = [ticker, bid_price, ask_price, lqtt]

    df['base_ticker'] = df['base_ticker'].apply(lambda x : x.replace('KRW-', '')) 

    curr_ex_rate = get_exchange_rate()

    df['price_usd'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    for ticker in diff_ticker_list : 
        df = df.drop(df[df['base_ticker'] == ticker].index)

    return df 
    

def call_orderbook_bithumb (ticker, curr_price) : 

    # orderbook here contains all the tickers, don't have to call prices separately.
    url = "https://api.bithumb.com/public/orderbook/" + ticker + "_KRW"

    json_object = call_api(url)

    data = json_object['data'] 

    bid_price = float(data['bids'][0]['price']) 
    ask_price = float(data['asks'][0]['price']) 

    lqtt = 0 

    for bid in data['bids'] : 
        # 2% depth liquidity 
        if float(bid['price']) > curr_price * 0.98 :
            lqtt += float(bid['price']) * float(bid['quantity']) 

    return bid_price, ask_price, lqtt
            

def get_prices_bithumb() : 

    # get all tickers and their prices
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"

    json_object = call_api(url) 

    data = json_object['data']

    columns = ['base_ticker', 'bid_price_krw', 'ask_price_krw', 'lqtt']
    df = pd.DataFrame(columns=columns)

    for ticker, info in data.items() : 
        if ticker != 'date' : 
            curr_price = float(info['closing_price'])
            bid_price, ask_price, lqtt = call_orderbook_bithumb(ticker, curr_price)
            df.loc[len(df)] = [ticker, bid_price, ask_price, lqtt]

    curr_ex_rate = get_exchange_rate()

    df['price_usd'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    return df 
    

def get_prices_binance() : 
    ''' 
    All df_base has output of Dataframe with : 
    columns = ['base_ticker', 'price_usd']
    datatype of 'price_usd' - float 
    '''

    url = "https://api.binance.com/api/v3/ticker/price"

    json_object = call_api(url)

    columns = ['base_ticker', 'price_usd']
    df = pd.DataFrame(columns=columns)

    # some of the tokens have been delisted but is still in the API showing wrong prices. 
    delisted_tickers = ['BTG']

    # returns only the base pair for USDT pairs 
    for ticker in json_object : 
        if 'USDT' in  ticker['symbol'] : 
            base_ticker = ticker['symbol'].replace('USDT', '')
            if base_ticker not in delisted_tickers : 
                df.loc[len(df)] = [base_ticker, float(ticker['price'])]

    return df 


def get_prices_bybit () : 
    url = "https://api.bybit.com/v5/market/tickers?category=spot"
    
    json_object = call_api(url) 

    data = json_object['result']['list']

    columns = ['base_ticker', 'price_usd']
    df = pd.DataFrame(columns=columns)

    # returns only the base pair for USDT pairs 
    for ticker in data : 
        if 'USDT' in  ticker['symbol'] : 
            df.loc[len(df)] = [ticker['symbol'].replace('USDT', ''), float(ticker['lastPrice'])]

    return df 


def get_prices_bitget () : 
    url = 'https://api.bitget.com/api/spot/v1/market/tickers'

    json_object = call_api(url) 

    data = json_object['data']

    columns = ['base_ticker', 'price_usd']
    df = pd.DataFrame(columns=columns)

    # returns only the base pair for USDT pairs 
    for ticker in data : 
        # some tickers does not have a price 
        if ticker['buyOne'] != '0':
            if 'USDT' in  ticker['symbol'] : 
                base_ticker = ticker['symbol'].replace('USDT', '')                
                df.loc[len(df)] = [base_ticker, float(ticker['buyOne'])]

    return df 


def get_prices_mexc () : 
    url = 'https://api.mexc.com/api/v3/ticker/price'

    json_object = call_api(url)

    columns = ['base_ticker', 'price_usd']
    df = pd.DataFrame(columns=columns)

    # some of the tokens give the wrong prices on MEXC
    dysfunc_tickers = ['GMT', 'GAS', 'META', 'TITAN', 'ALT']

    # returns only the base pair for USDT pairs 
    for ticker in json_object : 
        if 'USDT' in  ticker['symbol'] : 
            base_ticker = ticker['symbol'].replace('USDT', '')
            if base_ticker not in dysfunc_tickers : 
                df.loc[len(df)] = [base_ticker, float(ticker['price'])]

    return df 


def check_price_diff (df_base, df_against, base_name, against_name, notif_trig, profit_pct_lim, destination) : 
    '''
    Accepts list of tickers for two exchanges, maps the tickers, and sends notification when triggered. 
    '''

    df_base.rename(columns={'price_usd' : 'price_usd_base', 'ask_price_usd' : 'ask_price_usd_base'}, inplace=True)

    df_against.rename(columns={'price_usd' : 'price_usd_against'}, inplace=True)

    df_combined = pd.merge(df_base, df_against, on='base_ticker', how='left')

    # if positive then base is higher, if negative then base is lower. 
    df_combined['usd_diff'] = df_combined['price_usd_base'] - df_combined['price_usd_against']
    df_combined['pct_diff'] = abs(df_combined['usd_diff'] / df_combined['price_usd_against']) 

    # Formula explanation : We are buying token from other exchanges, selling on upbit for KRW, sell KRW for ETH, and send back. So we need ask price of ETH on upbit instead bid price. 

    df_combined['ask_usd_diff'] = df_combined['ask_price_usd_base'] - df_combined['price_usd_against']
    df_combined['ask_pct_diff'] = abs(df_combined['ask_usd_diff'] / df_combined['price_usd_against']) 

    # get ask price pct difference of ETH 
    for index, row in df_combined.iterrows() : 
        if df_combined.loc[index, 'base_ticker'] == 'ETH' : 
            base_eth_ask_price_pct = df_combined.loc[index, 'ask_pct_diff']
            break 

    for index, row in df_combined.iterrows() : 
        # case when base price > against price
        if df_combined.loc[index, 'usd_diff'] > 0 : 

            profit_pct =  100 * (df_combined.loc[index, 'pct_diff']  + 1) * (1 - base_eth_ask_price_pct) - 100

            print(df_combined.loc[index, 'base_ticker'], df_combined.loc[index, 'pct_diff'], base_eth_ask_price_pct, profit_pct)
            
            # conditions for notification trigger
            if profit_pct > profit_pct_lim and df_combined.loc[index, 'lqtt_usd'] > 10000: 
                notif_trig = 1
                message1 = '{} - {} is higher than {} by {:.2f} %.'.format(df_combined.loc[index, 'base_ticker'], base_name, against_name, abs(df_combined.loc[index, 'pct_diff']) * 100)
                message2 = 'Profit Pct Estimate - {:.2f} %'.format(profit_pct)
                message3 = '{} Rough USD Liquidity - $ {:.2f}'.format(base_name, df_combined.loc[index, 'lqtt_usd'])
                tg_notif(str(message1 + '\n\n' + message2 + '\n\n' + message3), destination) 
    
    return notif_trig
    

@timing_decorator
def execute(destination) : 

    # configurations 
    profit_pct_lim = 5

    # base exchanges
    df_upbit = get_prices_upbit() 
    df_bithumb = get_prices_bithumb()

    # exchanges compared to 
    df_binance = get_prices_binance() 
    df_bybit = get_prices_bybit() 
    df_bitget = get_prices_bitget()
    df_mexc = get_prices_mexc()

    # allows script to return default notification if condition is not triggered
    notif_trig = 0 

    # upbit comparisons
    notif_trig = check_price_diff(df_upbit, df_binance, 'Upbit', 'Binance', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_upbit, df_bybit, 'Upbit', 'Bybit', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_upbit, df_bitget, 'Upbit', 'Bitget', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_upbit, df_mexc, 'Upbit', 'MEXC', notif_trig, profit_pct_lim, destination)
    
    # bithumb comparisons
    notif_trig = check_price_diff(df_bithumb, df_binance, 'Bithumb', 'Binance', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_bithumb, df_bybit, 'Bithumb', 'Bybit', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_bithumb, df_bitget, 'Bithumb', 'Bitget', notif_trig, profit_pct_lim, destination)
    notif_trig = check_price_diff(df_bithumb, df_mexc, 'Bithumb', 'MEXC', notif_trig, profit_pct_lim, destination)

    if notif_trig == 0 : 
        tg_notif("No tickers within profit pct range of > {:.0f} %".format(profit_pct_lim), destination)


####################################### paste changes from the other main.py above #######################################

def lambda_handler(event, context):
    # TODO implement

    # tg notification destination for testing purposes 
    destination = 'real_time'

    execute(destination)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

