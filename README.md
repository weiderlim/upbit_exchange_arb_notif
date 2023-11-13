## Telegram Notification Bot for Korean Exchange Arbitrage Opportunities

Historically, there has been times where tokens on Upbit trade for significantly higher than the rest of the market, partially due to difficulty in opening an account if individuals are not from Korea. 

Example of this happening is when MINA first launched, price comparison between Binance and Upbit : 

[MINA Historical Price on Upbit](https://www.binance.com/en/price/mina)

[MINA Historical Price on Binance](https://upbit.com/exchange?code=CRIX.UPBIT.KRW-MINA)

*The current version of the script does not include automated trading. This is because the script uses tickers to match tokens between exchanges. In testing, it was found that different exchanges might use the same ticker for different tokens. For now, the bot serves as a notification tool which allows for further checking to cross-check tokens between exchanges by comparing contract addresses and executing the trades manually.*

### Bot Ping Output : 

1. *Absolute Profit* - Profit % multipled by Available Liquidity on the Korean exchange
2. *Real-time profit percentages* - See explanation below
3. *Available Liquidity at 2% depth*

The idea behind the calculation of **Real-life profit percentages** is mimicking the profit that would be gained if an individual sent a particular token on the Korean Exchanges, converted that token to ETH and sent that ETH from the Korean Exchanges. Since ETH prices are also higher on the Korean Exchanges, profits out are discounted. 

ETH is chosen because it has high liquidity, and does not take as long as BTC for transactions to execute. 

### To use : 

Before using this script, the following needs to be set up, followed by associated ENVIRONMENT_VARIABLES : 

1. **API keys for Telegram** - TELEGRAM_KEY
[Instructions](https://core.telegram.org/bots/api) for setting up the bot.

2. **Exchange rate for KRW USD conversion** - EXCHANGE_RATE_KEY
[Getting API key](https://exchangeratesapi.io/).

3. **An Upbit / Bithumb exchange account** 
Needed to execute the trades. 

4. **DB to store the conversion values** - COLLECTION_NAME, MONGO_CONN_STR, DB_NAME
DB used here is MongoDB, updated by a script on Lambda (ex_rate_api.py) file that runs every hour. 

5. Under the *execute* function in the script, adjust the parameters for the bot to ping to suit your personal preference. 


### Lambda Deployment : 

Due to the API call monthly limit for the exchange rate, we only want to update the Korean Won prices every hour. A separate cron job is used for the main script (every minute) and the exchange rate API call (every hour). 

Pymongo is used to call the exchange rate from a MongoDB collection (table), which is updated every hour with the exchange rate API call above. 

The script is ran on AWS Lambda, hence the lambda_handler function which enables Lambda to execute the script. 




