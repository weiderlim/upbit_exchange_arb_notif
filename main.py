import requests 
import json 
import time 
import pandas as pd 
import os 
from threading import Thread
from dotenv import load_dotenv

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


def tg_notif (message) : 
    load_dotenv()
    
    url = "https://api.telegram.org/bot{}/sendMessage".format(os.getenv('telegram_key'))

    parameters = {
        'chat_id' : '-911570737',
        'text' : message
        }

    response = requests.get(url, params=parameters)


def get_exchange_rate () : 
    '''
    Only want to call the exchange rate API if it has been an hour since the last call. This function creates a file with record of previous hour and exchange rate, and checks if it has ald been a hour. 
    '''

    # check if hour has not changed, if not then write new 
    if os.path.exists(os.getcwd() + '/files/exchange_rate_hour.json') : 
        
        with open ('files/exchange_rate_hour.json', 'r') as file :     
            output = json.load(file)
            
        if int(time.strftime("%H", time.localtime())) == output['previous_hour'] : 
            return output['previous_exchange_rate']
        
        else : 
            new_ex_rate = call_api_exchange_rate()

            new_data = {
                'previous_hour' : int(time.strftime("%H", time.localtime())),
                'previous_exchange_rate' : new_ex_rate
            }
            
            data = json.dumps(new_data, indent=4)
            
            with open ('files/exchange_rate_hour.json', 'w') as file :     
                file.write(data)     
            
            return new_ex_rate

    else : 
        new_ex_rate = call_api_exchange_rate()

        new_data = {
            'previous_hour' : int(time.strftime("%H", time.localtime())),
            'previous_exchange_rate' : new_ex_rate
        }
        
        data = json.dumps(new_data, indent=4)
        
        with open ('files/exchange_rate_hour.json', 'w') as file :     
            file.write(data)     
        
        return new_ex_rate 


def call_api_exchange_rate () : 

    url = "https://api.exchangeratesapi.io/v1/latest"

    load_dotenv()

    parameters = {
        'access_key' : os.getenv('exchange_rate_key'),
        'symbols' : 'USD, KRW'
        }
    
    response = requests.get(url, params=parameters) 

    json_object = json.loads(response.text) 

    return json_object['rates']['KRW'] / json_object['rates']['USD']


def call_api_upbit (ticker) : 
    '''
    Accepts ticker, returns current price  
    '''

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
    headers = {'accept': 'application/json'}
    params = {'isDetails': 'false'}

    response = requests.get(url, headers=headers, params=params)
    response.raise_for_status()  # Check if the request was successful

    json_object = json.loads(response.text) 

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

    curr_ticker_index = 0

    threads = []

    ticker_list = get_tickers_upbit()
    ticker_window_len = 1

    while curr_ticker_index < len(ticker_list) : 
        for ticker in ticker_list[curr_ticker_index : curr_ticker_index + ticker_window_len]:
            t = Thread(target=task, args=(ticker,))
            threads.append(t)
            t.start()

        # wait for the threads to complete
        for t in threads:
            t.join()

        curr_ticker_index += ticker_window_len

    curr_ex_rate = get_exchange_rate()

    df['price_usd_upbit'] = df['bid_price_krw'] / curr_ex_rate
    df['ask_price_usd_upbit'] = df['ask_price_krw'] / curr_ex_rate
    df['lqtt_usd'] = df['lqtt'] / curr_ex_rate

    # returns only the base pair for KRW pairs 
    df['base_ticker'] = df['ticker'].apply(lambda x : x.replace('KRW-', ''))

    # for troubleshoot purposes 
    df.to_csv('files/upbit_prices.csv', index=False) 

    return df 
    

def get_prices_binance() : 
    ''' 
    Returns price of all binance USDT pairs in a list. 
    '''

    # Reads url and converts it into a readable JSON format
    url = "https://api.binance.com/api/v3/ticker/price"

    headers = {
        "accept": "application/json"
    }

    response = requests.get(url, headers=headers)

    # convert (usually response is in non JSON form) to json object first 
    json_object = json.loads(response.text) 

    columns = ['base_ticker', 'price_usd_binance', 'curr_time']
    df = pd.DataFrame(columns=columns)

    # returns only the base pair for USDT pairs 
    for ticker in json_object : 
        if 'USDT' in  ticker['symbol'] : 
            curr_time = time.strftime("%d-%m-%y %H:%M:%S", time.localtime())
            df.loc[len(df)] = [ticker['symbol'].replace('USDT', ''), float(ticker['price']), curr_time]

    with open ('files/delisted_binance.json', 'r') as file : 
        output = json.load(file) 
        df = df[~df['base_ticker'].isin(output['ticker'])]

    # for troubleshoot purposes 
    df.to_csv('files/binance_prices.csv', index=False) 
    
    return df 


def check_price_diff (df_upbit, df_binance) : 
    '''
    Accepts list of tickers for two exchanges, maps the tickers, and sends notification when triggered. 
    '''
    tg_notif('cron job 2')

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

    profit_pct_lim = 1

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

    # for troubleshooting purposes 
    # df_combined.to_csv('files/df_combined.csv')


@timing_decorator
def execute() : 
    df_upbit = get_prices_upbit() 
    df_binance = get_prices_binance() 

    check_price_diff(df_upbit, df_binance)


if __name__ == '__main__' : 

    tg_notif('cron job 1')
    execute()
    tg_notif('cron job ended')









    

