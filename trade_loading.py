from influxdb import InfluxDBClient
import datetime
import urllib3, shutil
import pandas as pd
from multiprocessing import Pool
import warnings
warnings.filterwarnings("ignore")

influx_client = InfluxDBClient('10.10.2.8', 8086, 'root', password='root''mydb')
influx_client.switch_database('BitMex_Liquidation_test')


def load_trade(results, pool):
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


if __name__ == '__main__':
    start = datetime.datetime.strptime("20181109", "%Y%m%d")
    end = datetime.datetime.strptime("20191024", "%Y%m%d")  # the end date is not included, so +1

    date_generated = [start + datetime.timedelta(days=x) for x in range(0, (end - start).days)]
    pool = []
    for date in date_generated:
        date = date.strftime("%Y%m%d")
        url = 'https://s3-eu-west-1.amazonaws.com/public.bitmex.com/data/trade/' + str(date) + '.csv.gz'
        c = urllib3.PoolManager()
        filename = "day_trade.csv.gz"
        with c.request('Get', url, preload_content=False) as res, open(filename, 'wb') as out_file:
            shutil.copyfileobj(res, out_file)

        record = pd.read_csv('day_trade.csv.gz').to_dict(orient='record')
        for chunk in record:
            pool = load_trade(chunk, pool)
