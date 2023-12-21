import os
import json
import psycopg2
import time
import logging

from psycopg2 import sql
from datetime import timedelta
from pandas import DataFrame
from tinkoff.invest import CandleInterval, Client, SecurityTradingStatus
from tinkoff.invest.retrying.sync.client import RetryingClient
from tinkoff.invest.retrying.settings import RetryClientSettings
from tinkoff.invest.utils import now, quotation_to_decimal
from tinkoff.invest.services import InstrumentsService



TOKEN = os.environ["TOKEN"]
LOGIN_DATA = {
    "USERNAME": 'gen_user',
    "DATABASE_NAME": 'default_db',
    "HOST": '5.23.49.175',
    "PORT": '5432',
    "PASSWORD": 'F}QNx&!TxRI%2('
}
retry_settings = RetryClientSettings(use_retry=True, max_retry_attempt=100)


logging.basicConfig(filename='tinkoff.log', filemode='a', level=logging.DEBUG, format="%(asctime)s - %(levelname)s: - %(message)s")
logger=logging.getLogger() 
logger.propagate = False


def div_quotation(q):
    return q.units + q.nano / 1000000000


def write_data(candles, tablename, flag):

    connection = psycopg2.connect(user=LOGIN_DATA["USERNAME"],
                                  password=LOGIN_DATA["PASSWORD"],
                                  host=LOGIN_DATA["HOST"],
                                  port=LOGIN_DATA["PORT"],
                                  database=LOGIN_DATA["DATABASE_NAME"])
    
    cursor = connection.cursor()
    cursor.execute(sql.SQL("SELECT * FROM {tablename} LIMIT 1").format(tablename=sql.Identifier(tablename)))
    record = cursor.fetchone()
    if not record:

        for candle in candles:
            insert_query = sql.SQL("INSERT INTO {tablename} \
                             (ticker, open, high, low, close, volume, time, is_complete) \
                             VALUES (%s, %s, %s, %s, %s, %s, %s, %s)").format(tablename=sql.Identifier(tablename))
            items = (candle[0], candle[1], candle[2], candle[3], candle[4], candle[5], candle[6], candle[7])
  
            cursor.execute(insert_query, items)          
    else:
        cursor.execute(sql.SQL("""SELECT time FROM {tablename} ORDER BY id DESC LIMIT 1""").format(tablename=sql.Identifier(tablename)))

        last_date = cursor.fetchone()[0]
        if not flag:
            cursor.execute(sql.SQL("DELETE FROM {tablename} WHERE time < %s").format(tablename=sql.Identifier(tablename)), (candles[0][6],))
        
        for candle in candles:
            insert_query = sql.SQL("INSERT INTO {tablename} \
                           (ticker, open, high, low, close, volume, time, is_complete) \
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)").format(tablename=sql.Identifier(tablename))
            if candle[6] > last_date:
                items = (candle[0], candle[1], candle[2], candle[3], candle[4], candle[5], candle[6], candle[7])
                cursor.execute(insert_query, items)
                
        
    connection.commit()
    cursor.close()
    connection.close()
    logger.info(f"Таблица успешно обновлена: {tablename} для тикера {candles[0][0]}")


def take_candles_by_interval(uid, interval, ticker, factor, flag):
    resultList = []
    begin = now()
    
    if flag:    
        resultLen = len(resultList)      
        while flag:
            with RetryingClient(TOKEN, retry_settings) as client:
                candles = client.get_all_candles(
                    from_=begin-timedelta(days=365),
                    to=begin,
                    interval=interval,
                    instrument_id=uid
                )

                for candle in candles:
                    resultList.append((ticker, div_quotation(candle.open), div_quotation(candle.high),
                                       div_quotation(candle.low), div_quotation(candle.close), candle.volume,
                                       candle.time, candle.is_complete))
            
            if resultLen != len(resultList):
                resultLen = len(resultList)
            else:
                flag = False
            begin = begin-timedelta(days=365)
    else:
        if factor:
            while factor:
                with RetryingClient(TOKEN, retry_settings) as client:
                    candles = client.get_all_candles(
                        from_=begin-timedelta(days=365),
                        to=begin,
                        interval=interval,
                        instrument_id=uid
                    )
                    if candles:
                        for candle in candles:
                            resultList.append((ticker, div_quotation(candle.open), div_quotation(candle.high),
                                           div_quotation(candle.low), div_quotation(candle.close), candle.volume,
                                           candle.time, candle.is_complete))
                
                begin = begin - timedelta(days=365) 
                factor -= 1
        else:
            with RetryingClient(TOKEN, retry_settings) as client:
                for candle in client.get_all_candles(
                        from_=begin-timedelta(days=61),
                        to=begin,
                        interval=interval,
                        instrument_id=uid
                    ):
                        resultList.append((ticker, div_quotation(candle.open), div_quotation(candle.high),
                                           div_quotation(candle.low), div_quotation(candle.close), candle.volume,
                                           candle.time, candle.is_complete))
            
    return resultList
        

def main(uids):
    tables = {
        CandleInterval.CANDLE_INTERVAL_15_MIN: 'historic_candles_15_min',
        CandleInterval.CANDLE_INTERVAL_HOUR: 'historic_candles_hour',
        CandleInterval.CANDLE_INTERVAL_4_HOUR: 'historic_candles_4_hour',
        CandleInterval.CANDLE_INTERVAL_DAY: 'historic_candles_day',
        CandleInterval.CANDLE_INTERVAL_WEEK: 'historic_candles_week'
    }
    start = time.time()

    for interval, factor, flag in ((CandleInterval.CANDLE_INTERVAL_15_MIN, 0, False), (CandleInterval.CANDLE_INTERVAL_HOUR, 1, False), 
                                   (CandleInterval.CANDLE_INTERVAL_4_HOUR, 2, False), (CandleInterval.CANDLE_INTERVAL_DAY, 10, False), 
                                   (CandleInterval.CANDLE_INTERVAL_WEEK, 0, True)):
        candles = []
        for uid, ticker in uids:
            print(interval, uid)
            candles.extend(take_candles_by_interval(uid=uid, interval=interval, ticker=ticker, factor=factor, flag=flag))

        print("Writing")
        write_data(candles, tables[interval], flag)
    
    end = time.time()
    logger.info(f"Затраченное время на работу программы (в минутах): {(end-start)//60}")


def get_uids():
    ticker_names = []

    with open('tickers.json', 'r', encoding='utf-8') as file:
        ticker_names = json.load(file)['data']

    with Client(TOKEN) as client:
        instruments: InstrumentsService = client.instruments
        tickers = []
        method = "shares"
        
        for item in getattr(instruments, method)().instruments:
            tickers.append(
                {
                    "name": item.name,
                    "ticker": item.ticker,
                    "class_code": item.class_code,
                    "figi": item.figi,
                    "uid": item.uid,
                    "type": method,
                    "min_price_increment": quotation_to_decimal(
                        item.min_price_increment
                    ),
                    "scale": 9 - len(str(item.min_price_increment.nano)) + 1,
                    "lot": item.lot,
                    "trading_status": str(
                        SecurityTradingStatus(item.trading_status).name
                    ),
                    "api_trade_available_flag": item.api_trade_available_flag,
                    "currency": item.currency,
                    "exchange": item.exchange,
                    "buy_available_flag": item.buy_available_flag,
                    "sell_available_flag": item.sell_available_flag,
                    "short_enabled_flag": item.short_enabled_flag,
                    "klong": quotation_to_decimal(item.klong),
                    "kshort": quotation_to_decimal(item.kshort),
                }
            )

        tickers_df = DataFrame(tickers)
        uids = []
        for val in ticker_names:
            ticker_df = tickers_df[tickers_df["ticker"] == val]
            if len(ticker_df["uid"].keys()):
                for key in ticker_df["uid"].keys():
                    uids.append((ticker_df["uid"][key], val))
            else:
                logger.error(f"Не удалось получить акции по тикеру: {val}")
        
        return uids



if __name__ == "__main__":
    uids = get_uids()
    main(uids)

