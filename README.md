# Telegram Notification Bot for Korean Exchange Arbitrage Opportunities

Historically, there has been times where tokens on Upbit trade for significantly higher than the rest of the market, partially due to difficulty in opening an account if individuals are not from Korea. 

Example of this happening is when MINA first launched, price comparison between Binance and Upbit : 

[MINA Historical Price on Upbit](https://www.binance.com/en/price/mina)

[MINA Historical Price on Binance](https://upbit.com/exchange?code=CRIX.UPBIT.KRW-MINA)


## Output : 

The bot pings on **Absolute Profit** to be made from the trade - *Profit % times Available Liquidity on the Korean exchange*, available *liquidity at 2% depth*, *real-time profit percentages*. 

The idea behind the calculation of *real-life profit percentages* is mimicking the profit that would be gained if an individual sent a particular token to Upbit, converted that token to ETH and sent that ETH from Upbit. Since ETH prices are also higher on Upbit, profits out are discounted. 

ETH is chosen because it has high liquidity, and does not take as long as BTC for transactions to execute. 

## To use : 

**API keys for Telegram** - [Instructions](https://core.telegram.org/bots/api) for setting up the bot and [getting API key for exchange rate](https://exchangeratesapi.io/) are needed.

An **Upbit / Bithumb exchange account** is needed to execute the trades. 

Adjust the parameters for the bot to ping in the code to suit your personal preference. 


### Lambda Deployment code : 

Due to the API call monthly limit for the exchange rate, we only want to update the Korean Won prices every hour. A separate cron job is used for the main script (every minute) and the exchange rate API call (every hour). 

Pymongo is used to call the exchange rate from a MongoDB collection (table), which is update every hour with the exchange rate API call above. 

The code is ran on AWS Lambda, hence the lambda_handler function which enables Lambda to execute the script. 




