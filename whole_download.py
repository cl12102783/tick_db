from influxdb import InfluxDBClient
import datetime
import urllib3, shutil
import pandas as pd
from collections import defaultdict
import time
import numpy as np
import json
import requests
import warnings

warnings.filterwarnings("ignore")

def tick_trade_bitmex(results, pool):
    """
    This function is to transform a single point into the JSON format InfluxDB uses.

    :param results: One single point with dictionary data structure

    """
    body = {
        'measurement': 'trade',

        'tags': {'symbol': results['symbol'],
                 'side': results['side'],
                 'tickDirection': results['tickDirection']},
        # no matter replace D by T or space, influxDB will change it to T anyway.
        # Nanosec is stored by default in Bitmex.
        'time': results['timestamp'].replace('D', 'T')
    }

    # remove used columns
    del results['symbol'], results['timestamp'], results['side'], results['tickDirection']

    body['fields'] = results
    # Type coercion in case it is sometimes integer or float with confilcts
    body['fields']['price'] = float(body['fields']['price'])
    body['fields']['foreignNotional'] = float(body['fields']['foreignNotional'])
    body['fields']['homeNotional'] = float(body['fields']['homeNotional'])

    for member in list(body['fields'].keys()):  # list is to avoid iteration runtime error caused by removing keys
        if pd.isnull(body['fields'][member]):
            del body['fields'][member]  # remove field columns with nan

    pool.append(body)
    if len(pool) > 10000:
        influx_client.write_points(pool)
        pool.clear()
    return pool

def process_tick_trade_bitmex(start, end):
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    pool = []
    for date in date_generated:
        date = date.strftime("%Y%m%d")
        url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/' + str(date) + '.csv.gz'
        c = urllib3.PoolManager()
        filename = "day_trade.csv.gz"
        with c.request('Get', url, preload_content=False) as res, open(filename, 'wb') as out_file:
            shutil.copyfileobj(res, out_file)

        record = pd.read_csv('day_trade.csv.gz')[-10010:].to_dict(orient='record')
        for chunk in record:
            pool = tick_trade_bitmex(chunk, pool)
    influx_client.write_points(pool)


def tick_quote_bitmex(results, pool):
    body = {}
    body['measurement'] = 'quote'
    body['tags'] = {'symbol': results['symbol']}
    body['time'] = results['timestamp'].replace('D', 'T')

    del results['symbol'], results['timestamp']

    body['fields'] = results

    for member in list(body['fields'].keys()):  # list is to avoid iteration runtime error caused by removing keys
        if pd.isnull(body['fields'][member]):
            del body['fields'][member]
        else:
            body['fields'][member] = float(body['fields'][member])

    if len(body['fields']) != 0:
        pool.append(body)
        if len(pool) > 5000:
            influx_client.write_points(pool)
            pool.clear()
    return pool

def process_tick_quote_bitmex(start, end):
    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    pool = []

    for date in np.flip(date_generated):
        date = date.strftime("%Y%m%d")
        url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/quote/' + str(date) + '.csv.gz'
        c = urllib3.PoolManager()
        filename = "day_quote.csv.gz"
        with c.request('Get', url, preload_content=False) as res, open(filename, 'wb') as out_file:
            shutil.copyfileobj(res, out_file)
        # Do it in multiprocessing
        record = pd.read_csv('day_quote.csv.gz')[-10010:].to_dict(orient='record')
        for chunk in record:
            pool = tick_quote_bitmex(chunk, pool)
    influx_client.write_points(pool)

def trade_1min_bitmex(results):
    body = {}
    for record in results:

        body['measurement'] = 'bitmex_1min_trade'
        body['tags'] = {'symbol': record['symbol']}
        body['time'] = record['timestamp']

        del record['timestamp'], record['symbol']

        body['fields'] = record

        for member in list(body['fields'].keys()):  # list is to avoid iteration runtime error caused by removing keys
            if pd.isnull(body['fields'][member]):
                del body['fields'][member]
            else:
                body['fields'][member] = float(body['fields'][member])

        if len(body['fields']) != 0:
            influx_client.write_points([body])

    return pd.to_datetime(body['time'])

def process_trade_1min_bitmex(start, end, instrument_name):
    #pool = []

    for item in instrument_name:
        cont = True  # flag to show if more data to be loaded for a day since 1000 points is limited for one request
        while cont:
            baseURI = "https://www.bitmex.com/api/v1"
            endpoint = "/trade/bucketed"
            params = {'binSize': '1m', 'symbol': item, 'count': 1000, 'startTime': start, 'endTime': end}
            results = requests.get(baseURI + endpoint, params=params)
            results = results.json()
            time.sleep(2) # since there is time rate limit
            if len(results) > 0:
                newtime = trade_1min_bitmex(results)
                if end.date() == newtime.date():
                    cont = False
                else:
                    start = newtime  # set the tail as the new start
            else:
                start += datetime.timedelta(days=1)
                if start >= end:
                    cont=False

def trade_1min_deribit(results, symbol, pool):
    for i in range(results.shape[0]):
        record = dict(results.iloc[i, :])
        body = {}
        body['measurement'] = 'deribit_1min_quote'
        body['tags'] = {'symbol': symbol}
        body['time'] = record['ticks']

        del record['ticks']

        body['fields'] = record

        for member in list(body['fields'].keys()):  # list is to avoid iteration runtime error caused by removing keys
            if pd.isnull(body['fields'][member]):
                del body['fields'][member]
            else:
                body['fields'][member] = float(body['fields'][member])

        if len(body['fields']) != 0:
            pool.append(body)
            if len(pool) > 5000:
                influx_client.write_points(pool)
                pool.clear()
    return pool


def process_1min_trade_deribit(start, end, instrument_name):

    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end-start).days)]
    # Other variables
    resolution = 1 # min

    pool = []

    for item in instrument_name:
        for date in date_generated:
            t_start= int(date.timestamp()*1000)
            t_end = int((date+datetime.timedelta(days=1)).timestamp()*1000)

            baseURI = "https://www.deribit.com/api/v2"
            endpoint = "/public/get_tradingview_chart_data"
            params = {'resolution': 1, 'instrument_name': item, 'start_timestamp': t_start, 'end_timestamp': t_end}
            results = requests.get(baseURI + endpoint, params=params)
            data = results.json()

            data = pd.DataFrame(data['result'])
            data.drop(columns='status', inplace=True) # no use
            data.ticks = pd.to_datetime(data.ticks, unit='ms').dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            pool = trade_1min_deribit(data, item, pool)
    influx_client.write_points(pool)


def trade_tick_deribit(results, pool):
    body = defaultdict()
    for record in results:
        body = {'measurement': 'deribit_tick_trade',
                'tags': {'symbol': record['instrument_name'],
                         'tick_direction': record['tick_direction'],
                         'side': record['direction'],
                         'liquidation': record.get('liquidation', None)},
                'time': pd.to_datetime(record['timestamp'], unit='ms')}

        del record['instrument_name'], record['timestamp'], record['trade_seq'], record['trade_id']
        del record['tick_direction'], record['direction']
        if record.get('liquidation', 'none') != 'none': del record['liquidation']

        body['fields'] = record

        for member in ['price', 'amount',
                       'index_price']:  # list is to avoid iteration runtime error caused by removing keys
            if pd.isnull(body['fields'][member]):
                del body['fields'][member]
            else:
                body['fields'][member] = float(body['fields'][member])

        if len(body['fields']) != 0:
            pool.append(body)
            if len(pool) > 5000:
                influx_client.write_points(pool)
                pool.clear()
        # body.clear()
    new_start_time = 0
    if body.get('time', 'none') != 'none':
        new_start_time = int(pd.to_datetime(body['time']).timestamp() * 1000)

    return pool, new_start_time


def process_tick_trade_deribit(start, end, instrument_name):
    # Time: nanosecond epoch

    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    # Other variables

    pool_write = []

    for item in instrument_name:
        for date in date_generated:
            t_start = int(date.timestamp() * 1000)
            t_end = int((date + datetime.timedelta(days=1)).timestamp() * 1000)
            cont = True  # flag to continue load more unread data for the same day
            while cont:
                baseURI = "https://www.deribit.com/api/v2"
                endpoint = "/public/get_last_trades_by_instrument_and_time"
                params = {'include_old': 'true', 'count': 1000, 'instrument_name': item, 'start_timestamp': t_start, 'end_timestamp': t_end}
                results = requests.get(baseURI + endpoint, params=params)
                data = results.json()

                if len(data['result']['trades']) != 0:
                    cont = data['result']['has_more']
                    pool_write, t_start = trade_tick_deribit(data['result']['trades'], pool_write)
                else:
                    cont = False

    influx_client.write_points(pool_write)  # write the final data w/ size<5000 in the pool

if __name__ == '__main__':
    influx_client = InfluxDBClient('10.10.2.8', 8086, 'root', password='root''mydb')
    influx_client.switch_database('BitMex_Liquidation_test')

    start = datetime.datetime.strptime("20191111", "%Y%m%d")
    end = datetime.datetime.strptime("20191112", "%Y%m%d")  # the end date is not included, so +1
    instrument_name_bitmex = ['XBTUSD', 'XBTZ19', 'XBTH20']
    instrument_name_deribit = ['BTC-PERPETUAL', 'BTC-27DEC19', 'BTC-27MAR20']

    process_tick_trade_deribit(start, end, instrument_name_deribit)
    process_1min_trade_deribit(start, end, instrument_name_deribit)
    process_trade_1min_bitmex(start, end, instrument_name_bitmex)
    process_tick_quote_bitmex(start, end)
    process_tick_trade_bitmex(start, end)